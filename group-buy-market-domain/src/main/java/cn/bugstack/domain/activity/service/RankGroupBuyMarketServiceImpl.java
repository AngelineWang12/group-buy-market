package cn.bugstack.domain.activity.service;

import cn.bugstack.api.dto.GoodsMarketRankListResponseDTO;
import cn.bugstack.api.dto.GoodsMarketRankResponseDTO;
import cn.bugstack.domain.activity.adapter.repository.IActivityRepository;
import cn.bugstack.domain.activity.adapter.repository.IRankRedisRepository;
import cn.bugstack.domain.activity.model.entity.RankItemEntity;
import cn.bugstack.domain.activity.service.trial.factory.RankKeyFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Fuzhengwei bugstack.cn @小傅哥
 * @description 首页营销服务
 * @create 2024-12-14 14:33
 */
@Slf4j
@Service
public class RankGroupBuyMarketServiceImpl implements IRankGroupBuyMarketService {

    @Resource
    private RankKeyFactory rankKeyFactory;
    @Resource
    private IRankRedisRepository rankRedisRepository;
    @Resource
    private IActivityRepository activityRepository;

    @Override
    public GoodsMarketRankListResponseDTO queryTopNByActivityId(Long activityId, String timeWindow, String windowKey) throws Exception {
        String zsetKey = rankKeyFactory.saleKey(activityId, timeWindow, windowKey);
        int topN = 10;

        List<RankItemEntity> items = rankRedisRepository.queryTopN(zsetKey, topN); // goodsId + score(saleCount)

        // 2) 取更新时间
        Date updateTime = rankRedisRepository.getUpdateTime(zsetKey + ":meta:updateTime");

        // 3) 组装排行榜数据，只包含基本信息，价格信息将在Controller中通过试算获取
        List<GoodsMarketRankResponseDTO> rankList = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            RankItemEntity item = items.get(i);
            String goodsId = item.getMember(); // RankItemEntity使用member字段存储goodsId

            // 构建排行榜响应对象，注意GoodsMarketRankResponseDTO的结构
            GoodsMarketRankResponseDTO rankItem = GoodsMarketRankResponseDTO.builder()
                    .rankNo(i + 1)
                    .score(item.getScore()) // GoodsMarketRankResponseDTO使用score字段而不是saleCount
                    .build();

            // 构建并设置商品信息，包括goodsId
            GoodsMarketRankResponseDTO.Goods goods = GoodsMarketRankResponseDTO.Goods.builder()
                    .goodsId(goodsId)
                    .build();
            rankItem.setGoods(goods);

            rankList.add(rankItem);
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