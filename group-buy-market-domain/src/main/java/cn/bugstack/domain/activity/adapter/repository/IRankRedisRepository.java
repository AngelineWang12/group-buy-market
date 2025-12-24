package cn.bugstack.domain.activity.adapter.repository;

import cn.bugstack.domain.activity.model.entity.RankItemEntity;

import java.time.Duration;
import java.util.Date;
import java.util.List;

public interface IRankRedisRepository {

    /**
     * 查询销量榜 Top N 商品
     */
    List<RankItemEntity> queryTopN(String zsetKey, int topN);

    Date getUpdateTime(String s);

    /**
     * 幂等去重，首次返回 true
     */
    boolean tryDedup(String dedupKey, Duration ttl);

    /**
     * 榜单增量 + 更新时间 + TTL
     */
    void incrWithMeta(String zsetKey, String metaUpdateKey, String goodsId, long delta, long ttlSeconds, long updateTimeMillis);
}