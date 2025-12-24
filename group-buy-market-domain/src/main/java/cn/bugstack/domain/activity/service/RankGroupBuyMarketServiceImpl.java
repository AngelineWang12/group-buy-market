package cn.bugstack.domain.activity.service;

import cn.bugstack.api.dto.GoodsMarketRankListResponseDTO;
import cn.bugstack.api.dto.GoodsMarketRankResponseDTO;
import cn.bugstack.domain.activity.adapter.repository.IActivityRepository;
import cn.bugstack.domain.activity.adapter.repository.IRankRedisRepository;
import cn.bugstack.domain.activity.model.entity.MarketProductEntity;
import cn.bugstack.domain.activity.model.entity.RankItemEntity;
import cn.bugstack.domain.activity.model.entity.TrialBalanceEntity;
import cn.bugstack.domain.activity.service.trial.factory.RankKeyFactory;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author Fuzhengwei bugstack.cn @小傅哥
 * @description 首页营销服务
 * @create 2024-12-14 14:33
 */
@Service
public class RankGroupBuyMarketServiceImpl implements IRankGroupBuyMarketService {

    @Resource
    private RankKeyFactory rankKeyFactory;
    @Resource
    private IRankRedisRepository rankRedisRepository;
    @Resource
    private IActivityRepository activityRepository;
    @Resource
    private IIndexGroupBuyMarketService indexGroupBuyMarketService;

    @Override
    public GoodsMarketRankListResponseDTO queryTopNByActivityId(Long activityId, String timeWindow, String windowKey) throws Exception {
        String zsetKey = rankKeyFactory.saleKey(activityId, timeWindow, windowKey);
        int topN = 10;

        List<RankItemEntity> items = rankRedisRepository.queryTopN(zsetKey, topN); // goodsId + score(saleCount)

        // 2) 取更新时间
        Date updateTime = rankRedisRepository.getUpdateTime(zsetKey + ":meta:updateTime");

        // 3) 组装排行榜数据
        List<GoodsMarketRankResponseDTO> rankList = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            RankItemEntity it = items.get(i);
            String goodsId = it.getMember();

            // 通过试算获取商品价格信息
            TrialBalanceEntity trialBalanceEntity = indexGroupBuyMarketService.indexMarketTrial(MarketProductEntity.builder()
                    .activityId(activityId)
                    .userId("system") // 使用默认用户ID，因为排行榜查询没有具体用户
                    .goodsId(goodsId)
                    .source("system") // 使用默认来源
                    .channel("system") // 使用默认渠道
                    .build());

            // 构建商品信息
            GoodsMarketRankResponseDTO.Goods goods = GoodsMarketRankResponseDTO.Goods.builder()
                    .goodsId(goodsId)
                    .originalPrice(trialBalanceEntity.getOriginalPrice())
                    .deductionPrice(trialBalanceEntity.getDeductionPrice())
                    .payPrice(trialBalanceEntity.getPayPrice())
                    .build();

            // 组装排行榜响应对象
            rankList.add(GoodsMarketRankResponseDTO.builder()
                    .rankNo(i + 1)
                    .goodsId(goodsId)
                    .saleCount(it.getScore())
                    .goods(goods)
                    .build());
        }

        GoodsMarketRankListResponseDTO resp = GoodsMarketRankListResponseDTO.builder()
                .activityId(activityId)
                .rankType("SALE")
                .timeWindow(timeWindow)
                .windowKey(windowKey)
                .updateTime(updateTime)
                .rankList(rankList)
                .build();
        return resp;
    }
}