package cn.bugstack.infrastructure.adapter.repository;

import cn.bugstack.domain.activity.adapter.repository.IRankRedisRepository;
import cn.bugstack.domain.activity.model.entity.RankItemEntity;
import cn.bugstack.infrastructure.dcc.DCCService;
import cn.bugstack.infrastructure.redis.IRedisService;
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
@Repository
public class RankRedisRepository extends AbstractRepository implements IRankRedisRepository {

    @Resource
    private StringRedisTemplate redis;
    @Resource
    private RankLuaExecutor rankLuaExecutor;

    @Override
    public List<RankItemEntity> queryTopN(String zsetKey, int topN) {
        Set<ZSetOperations.TypedTuple<String>> tuples =
                redis.opsForZSet().reverseRangeWithScores(zsetKey, 0, topN - 1);
        List<RankItemEntity> list = new ArrayList<>();
        if (tuples == null) return list;

        for (ZSetOperations.TypedTuple<String> t : tuples) {
            list.add(new RankItemEntity(t.getValue(), t.getScore() == null ? 0L : t.getScore().longValue()));
        }
        return list;
    }

    @Override
    public Date getUpdateTime(String s) {
        String v = redis.opsForValue().get(s);
        if (v == null) return null;
        return new Date(Long.parseLong(v));

    }

    @Override
    public boolean tryDedup(String dedupKey, Duration ttl) {
        Boolean first = redis.opsForValue().setIfAbsent(dedupKey, "1", ttl);
        return Boolean.TRUE.equals(first);
    }

    @Override
    public void incrWithMeta(String zsetKey, String metaUpdateKey, String goodsId, long delta, long ttlSeconds, long updateTimeMillis) {
        rankLuaExecutor.incrWithMeta(zsetKey, metaUpdateKey, goodsId, delta, ttlSeconds, updateTimeMillis);
    }
}