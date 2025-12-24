package cn.bugstack.domain.activity.adapter.repository;

import cn.bugstack.domain.activity.model.entity.MarketProductEntity;
import cn.bugstack.domain.activity.model.entity.RankItemEntity;

import java.util.Date;
import java.util.List;

public interface IRankRedisRepository {

    /**
     * 查询销量榜 Top N 商品
     */
    List<RankItemEntity> queryTopN(String zsetKey, int topN);

    Date getUpdateTime(String s);
}
