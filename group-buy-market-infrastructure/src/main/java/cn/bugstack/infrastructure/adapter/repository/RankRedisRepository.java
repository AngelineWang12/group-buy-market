package cn.bugstack.infrastructure.adapter.repository;

import cn.bugstack.domain.activity.adapter.repository.IRankRedisRepository;
import cn.bugstack.domain.activity.model.entity.RankItemEntity;
import cn.bugstack.infrastructure.dcc.DCCService;
import cn.bugstack.infrastructure.redis.IRedisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.*;
import java.util.function.Supplier;

/**
 * @author Fuzhengwei bugstack.cn @小傅哥
 * @description 仓储抽象类
 */
@Repository
public class RankRedisRepository extends AbstractRepository implements IRankRedisRepository {

    @Resource
    private StringRedisTemplate redis;
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
}
