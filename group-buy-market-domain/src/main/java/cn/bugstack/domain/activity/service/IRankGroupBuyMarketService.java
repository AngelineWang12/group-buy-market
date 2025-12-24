package cn.bugstack.domain.activity.service;

import cn.bugstack.api.dto.GoodsMarketRankListResponseDTO;
import cn.bugstack.domain.activity.model.entity.MarketProductEntity;
import cn.bugstack.domain.activity.model.entity.TrialBalanceEntity;
import cn.bugstack.domain.activity.model.entity.UserGroupBuyOrderDetailEntity;
import cn.bugstack.domain.activity.model.valobj.TeamStatisticVO;

import java.util.List;

/**
 * @author Fuzhengwei bugstack.cn @小傅哥
 * @description 首页营销服务接口
 * @create 2024-12-14 13:39
 */
public interface IRankGroupBuyMarketService {

    GoodsMarketRankListResponseDTO queryTopNByActivityId(Long activityId, String timeWindow, String windowKey) throws Exception;

}
