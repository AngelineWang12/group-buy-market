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

    /**
     * 分页查询排行榜（按排名范围）
     * @param zsetKey Redis ZSet key
     * @param start 起始排名（从0开始）
     * @param end 结束排名（包含）
     * @return 排行榜列表
     */
    List<RankItemEntity> queryByRange(String zsetKey, int start, int end);

    /**
     * 查询指定商品的排名和分数
     * @param zsetKey Redis ZSet key
     * @param goodsId 商品ID
     * @return 排名信息，如果商品不存在返回null
     */
    RankItemEntity queryRankByGoodsId(String zsetKey, String goodsId);

    /**
     * 获取排行榜统计信息
     * @param zsetKey Redis ZSet key
     * @return 统计信息：[总数, 总分数]
     */
    long[] getRankStatistics(String zsetKey);

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