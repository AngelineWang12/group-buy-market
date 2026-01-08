package cn.bugstack.infrastructure.adapter.repository;

import cn.bugstack.domain.activity.adapter.repository.IRankRedisRepository;
import cn.bugstack.domain.activity.model.entity.RankItemEntity;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.ScoredEntry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
    private RedissonClient redissonClient;

    @Override
    public List<RankItemEntity> queryTopN(String key, int topN) {
        if (topN <= 0) {
            log.warn("查询排行榜TopN参数无效 key:{} topN:{}", key, topN);
            return new ArrayList<>();
        }
        return queryByRange(key, 0, topN - 1);
    }

    @Override
    public List<RankItemEntity> queryByRange(String key, int start, int end) {
        List<RankItemEntity> result = new ArrayList<>();
        try {
            if (start < 0 || end < start) {
                log.warn("查询排行榜范围参数无效 key:{} start:{} end:{}", key, start, end);
                return result;
            }

            // 1. 获取 ZSet (对应 Redis 命令: ZREVRANGE key start end WITHSCORES)
            RScoredSortedSet<String> scoredSortedSet = redissonClient.getScoredSortedSet(key);

            // 2. 获取指定排名范围的数据（倒序，销量高的在前）
            Collection<ScoredEntry<String>> entries = scoredSortedSet.entryRangeReversed(start, end);

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
            log.error("查询Redis排行榜异常 key:{} start:{} end:{}", key, start, end, e);
            // 异常时返回空列表，作为降级处理，防止整个接口崩掉
            return new ArrayList<>();
        }
        return result;
    }

    @Override
    public RankItemEntity queryRankByGoodsId(String zsetKey, String goodsId) {
        try {
            RScoredSortedSet<String> scoredSortedSet = redissonClient.getScoredSortedSet(zsetKey);
            
            // 获取商品分数
            Double score = scoredSortedSet.getScore(goodsId);
            if (score == null) {
                log.debug("商品不在排行榜中 zsetKey:{} goodsId:{}", zsetKey, goodsId);
                return null;
            }

            // 获取正序排名（从0开始，分数低的排名靠前）
            Integer rank = scoredSortedSet.rank(goodsId);
            if (rank == null) {
                // 如果rank为null，说明商品不在ZSet中（虽然score不为null，但可能数据不一致）
                return null;
            }

            // 返回商品排名信息（包含商品ID和分数）
            // 注意：这里不返回排名序号，因为排名需要根据总数量和正序排名计算
            // 如果需要排名序号，可以在调用方根据返回的数据和总数量计算
            
            return RankItemEntity.builder()
                    .member(goodsId)
                    .score(score.longValue())
                    .build();
        } catch (Exception e) {
            log.error("查询商品排名异常 zsetKey:{} goodsId:{}", zsetKey, goodsId, e);
            return null;
        }
    }

    @Override
    public long[] getRankStatistics(String zsetKey) {
        try {
            RScoredSortedSet<String> scoredSortedSet = redissonClient.getScoredSortedSet(zsetKey);
            
            // 总数
            long totalCount = scoredSortedSet.size();
            
            // 总分数（所有商品的分数之和）
            long totalScore = 0L;
            Collection<ScoredEntry<String>> allEntries = scoredSortedSet.entryRange(0, -1);
            if (allEntries != null) {
                for (ScoredEntry<String> entry : allEntries) {
                    if (entry.getScore() != null) {
                        totalScore += entry.getScore().longValue();
                    }
                }
            }

            return new long[]{totalCount, totalScore};
        } catch (Exception e) {
            log.error("获取排行榜统计信息异常 zsetKey:{}", zsetKey, e);
            return new long[]{0L, 0L};
        }
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

    /**
     * 修改 1: 去重/锁 (对应原 setIfAbsent)
     * 使用 Redisson 的 Bucket (等同于 Redis String) 的 trySet 方法
     */
    @Override
    public boolean tryDedup(String dedupKey, Duration ttl) {
        // 获取 Bucket 对象
        RBucket<String> bucket = redissonClient.getBucket(dedupKey);
        // trySet = SETNX (Set If Not Exist)
        // 将Duration转换为毫秒，使用TimeUnit.MILLISECONDS
        return bucket.trySet("1", ttl.toMillis(), TimeUnit.MILLISECONDS);
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