package cn.bugstack.infrastructure.adapter.repository;

import cn.bugstack.domain.activity.adapter.repository.IRankRedisRepository;
import cn.bugstack.domain.activity.model.entity.RankItemEntity;
import cn.bugstack.infrastructure.dcc.DCCService;
import cn.bugstack.infrastructure.redis.IRedisService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.ScoredEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.bugstack.infrastructure.adapter.repository.lua.RankLuaExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.*;
import java.util.function.Supplier;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * @author Fuzhengwei bugstack.cn @小傅哥
 * @description 仓储抽象类
 */
@Slf4j
@Repository
public class RankRedisRepository extends AbstractRepository implements IRankRedisRepository {

    @Resource
    private StringRedisTemplate redis;
    @Resource
    private RankLuaExecutor rankLuaExecutor;

    @Resource
    private RedissonClient redissonClient;

    @Override
    public List<RankItemEntity> queryTopN(String key, int topN) {
        List<RankItemEntity> result = new ArrayList<>();
        try {
            // 1. 获取 ZSet (对应 Redis 命令: ZREVRANGE key 0 topN-1 WITHSCORES)
            RScoredSortedSet<String> scoredSortedSet = redissonClient.getScoredSortedSet(key);

            // 2. 获取前 N 名 (0 到 topN-1)
            // entryRangeReversed 是倒序取值，正好符合排行榜逻辑 (销量高的在前)
            Collection<ScoredEntry<String>> entries = scoredSortedSet.entryRangeReversed(0, topN - 1);

            // 3. 转换数据
            if (entries != null && !entries.isEmpty()) {
                for (ScoredEntry<String> entry : entries) {
                    result.add(RankItemEntity.builder()
                            .member(String.valueOf(entry.getValue())) // 商品ID
                            .score(entry.getScore() == null ? 0L : entry.getScore().longValue())  // 销量
                            .build());
                }
            }
        } catch (Exception e) {
            log.error("查询Redis排行榜异常 key:{}", key, e);
            // 异常时返回空列表，作为降级处理，防止整个接口崩掉
            return new ArrayList<>();
        }
        return result;
    }

    @Override
    public Date getUpdateTime(String key) {
        try {
            // 使用 RBucket 获取普通字符串/对象
            RBucket<String> bucket = redissonClient.getBucket(key, StringCodec.INSTANCE);
            if (bucket.isExists()) {
                String val = bucket.get();
                // 此时 val 是 "170345..." 字符串
                return new Date(Long.parseLong(val));
            }
        } catch (Exception e) {
            log.error("获取排行榜更新时间异常 key:{}", key, e);
        }
        // 如果没有或异常，返回当前时间兜底
        return new Date();
    }

//    @Override
//    public List<RankItemEntity> queryTopN(String zsetKey, int topN) {
//        Set<ZSetOperations.TypedTuple<String>> tuples =
//                redis.opsForZSet().reverseRangeWithScores(zsetKey, 0, topN - 1);
//        List<RankItemEntity> list = new ArrayList<>();
//        if (tuples == null) return list;
//
//        for (ZSetOperations.TypedTuple<String> t : tuples) {
//            list.add(new RankItemEntity(t.getValue(), t.getScore() == null ? 0L : t.getScore().longValue()));
//        }
//        return list;
//    }

//    @Override
//    public Date getUpdateTime(String s) {
//        String v = redis.opsForValue().get(s);
//        if (v == null) return null;
//        return new Date(Long.parseLong(v));
//
//    }

/*    @Override
    public boolean tryDedup(String dedupKey, Duration ttl) {
        Boolean first = redis.opsForValue().setIfAbsent(dedupKey, "1", ttl);
        return Boolean.TRUE.equals(first);
    }

    @Override
    public void incrWithMeta(String zsetKey, String metaUpdateKey, String goodsId, long delta, long ttlSeconds, long updateTimeMillis) {
        rankLuaExecutor.incrWithMeta(zsetKey, metaUpdateKey, goodsId, delta, ttlSeconds, updateTimeMillis);
    }*/
    /**
     * 修改 1: 去重/锁 (对应原 setIfAbsent)
     * 使用 Redisson 的 Bucket (等同于 Redis String) 的 trySet 方法
     */
    @Override
    public boolean tryDedup(String dedupKey, Duration ttl) {
        // 获取 Bucket 对象
        RBucket<String> bucket = redissonClient.getBucket(dedupKey);
        // trySet = SETNX (Set If Not Exist)
        // 注意：Redisson 接收的时间单位
        return bucket.trySet("1", ttl.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * 修改 2: 带元数据更新的累加 (对应原 Lua 脚本执行)
     * 彻底解决 "value is not a valid float" 报错
     */
    @Override
    public void incrWithMeta(String zsetKey, String metaUpdateKey, String goodsId, long delta, long ttlSeconds, long updateTimeMillis) {
        // Lua 脚本：
        // 1. ZINCRBY 增加销量
        // 2. EXPIRE 设置榜单过期时间
        // 3. SET 更新元数据时间
        // 4. EXPIRE 设置元数据过期时间
        String luaScript =
                "redis.call('ZINCRBY', KEYS[1], ARGV[1], ARGV[2]); " +
                        "redis.call('EXPIRE', KEYS[1], ARGV[3]); " +
                        "redis.call('SET', KEYS[2], ARGV[4]); " +
                        "redis.call('EXPIRE', KEYS[2], ARGV[3]); ";

        RScript script = redissonClient.getScript(StringCodec.INSTANCE); // 指定使用 String 编码，防止乱码

        // 执行脚本
        // keys: [zsetKey, metaUpdateKey]
        // values: [delta, goodsId, ttlSeconds, updateTimeMillis]
        script.eval(
                RScript.Mode.READ_WRITE,
                luaScript,
                RScript.ReturnType.VALUE,
                Arrays.asList(zsetKey, metaUpdateKey), // KEYS
                String.valueOf(delta),                 // ARGV[1]
                goodsId,                               // ARGV[2]
                String.valueOf(ttlSeconds),            // ARGV[3]
                String.valueOf(updateTimeMillis)       // ARGV[4]
        );
    }


}