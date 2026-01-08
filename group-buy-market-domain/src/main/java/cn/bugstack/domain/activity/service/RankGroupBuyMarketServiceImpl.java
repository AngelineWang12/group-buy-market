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

/**
 * @author Fuzhengwei bugstack.cn @小傅哥
 * @description 排行榜服务实现
 * @create 2024-12-14 14:33
 */
@Slf4j
@Service
public class RankGroupBuyMarketServiceImpl implements IRankGroupBuyMarketService {

    private static final int DEFAULT_TOP_N = 10;
    private static final int MAX_TOP_N = 100; // 最大查询数量限制

    @Resource
    private RankKeyFactory rankKeyFactory;
    @Resource
    private IRankRedisRepository rankRedisRepository;
    @Resource
    private IActivityRepository activityRepository;

    @Override
    public GoodsMarketRankListResponseDTO queryTopNByActivityId(Long activityId, String timeWindow, String windowKey) throws Exception {
        return queryTopNByActivityId(activityId, timeWindow, windowKey, DEFAULT_TOP_N);
    }

    /**
     * 查询排行榜TopN（支持自定义数量）
     * 
     * @param activityId 活动ID
     * @param timeWindow 时间窗口
     * @param windowKey 窗口key
     * @param topN 查询数量，如果<=0则使用默认值，最大不超过MAX_TOP_N
     * @return 排行榜数据
     */
    public GoodsMarketRankListResponseDTO queryTopNByActivityId(Long activityId, String timeWindow, String windowKey, Integer topN) throws Exception {
        // 参数校验和默认值处理
        if (topN == null || topN <= 0) {
            topN = DEFAULT_TOP_N;
        }
        if (topN > MAX_TOP_N) {
            log.warn("查询排行榜TopN超过最大限制，使用最大值 activityId:{} topN:{} max:{}", activityId, topN, MAX_TOP_N);
            topN = MAX_TOP_N;
        }

        String zsetKey = rankKeyFactory.saleKey(activityId, timeWindow, windowKey);
        log.debug("查询排行榜 activityId:{} timeWindow:{} windowKey:{} topN:{} zsetKey:{}", 
                activityId, timeWindow, windowKey, topN, zsetKey);

        // 1) 查询TopN数据
        List<RankItemEntity> items = rankRedisRepository.queryTopN(zsetKey, topN);
        if (items == null || items.isEmpty()) {
            log.info("排行榜数据为空 activityId:{} zsetKey:{}", activityId, zsetKey);
            return buildEmptyResponse(activityId, timeWindow, windowKey);
        }

        // 2) 获取更新时间
        String metaKey = rankKeyFactory.saleUpdateTimeKey(activityId, timeWindow, windowKey);
        Date updateTime = rankRedisRepository.getUpdateTime(metaKey);

        // 3) 获取统计信息（用于后续扩展，如显示总商品数等）
        long[] statistics = rankRedisRepository.getRankStatistics(zsetKey);
        // long totalCount = statistics[0];
        // long totalScore = statistics[1];

        // 4) 组装排行榜数据，只包含基本信息，价格信息将在Controller中通过试算获取
        List<GoodsMarketRankResponseDTO> rankList = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            RankItemEntity item = items.get(i);
            String goodsId = item.getMember(); // RankItemEntity使用member字段存储goodsId

            // 构建排行榜响应对象
            GoodsMarketRankResponseDTO rankItem = GoodsMarketRankResponseDTO.builder()
                    .rankNo(i + 1) // 排名从1开始
                    .score(item.getScore()) // 销量分数
                    .build();

            // 构建并设置商品信息，包括goodsId
            GoodsMarketRankResponseDTO.Goods goods = GoodsMarketRankResponseDTO.Goods.builder()
                    .goodsId(goodsId)
                    .build();
            rankItem.setGoods(goods);

            rankList.add(rankItem);
        }

        // 获取统计信息
        long[] statistics = rankRedisRepository.getRankStatistics(zsetKey);
        long totalCount = statistics[0];
        long totalScore = statistics[1];
        
        // 构建统计信息
        GoodsMarketRankListResponseDTO.RankStatistics rankStatistics = 
                GoodsMarketRankListResponseDTO.RankStatistics.builder()
                        .totalCount(totalCount)
                        .totalScore(totalScore)
                        .build();
        
        GoodsMarketRankListResponseDTO resp = GoodsMarketRankListResponseDTO.builder()
                .activityId(activityId)
                .rankType("SALE")
                .timeWindow(timeWindow)
                .windowKey(windowKey)
                .updateTime(updateTime)
                .rankList(rankList)
                .statistics(rankStatistics)
                .build();
        
        log.info("查询排行榜完成 activityId:{} topN:{} 实际返回:{} 总数:{}", 
                activityId, topN, rankList.size(), totalCount);
        
        return resp;
    }

    /**
     * 分页查询排行榜
     * 
     * @param activityId 活动ID
     * @param timeWindow 时间窗口
     * @param windowKey 窗口key
     * @param pageNum 页码（从1开始）
     * @param pageSize 每页大小
     * @return 排行榜数据
     */
    public GoodsMarketRankListResponseDTO queryByPage(Long activityId, String timeWindow, String windowKey, 
                                                       Integer pageNum, Integer pageSize) throws Exception {
        // 参数校验
        if (pageNum == null || pageNum < 1) {
            pageNum = 1;
        }
        if (pageSize == null || pageSize <= 0) {
            pageSize = DEFAULT_TOP_N;
        }
        if (pageSize > MAX_TOP_N) {
            pageSize = MAX_TOP_N;
        }

        String zsetKey = rankKeyFactory.saleKey(activityId, timeWindow, windowKey);
        
        // 计算排名范围
        int start = (pageNum - 1) * pageSize;
        int end = start + pageSize - 1;

        log.debug("分页查询排行榜 activityId:{} pageNum:{} pageSize:{} start:{} end:{}", 
                activityId, pageNum, pageSize, start, end);

        // 查询指定范围的数据
        List<RankItemEntity> items = rankRedisRepository.queryByRange(zsetKey, start, end);
        if (items == null || items.isEmpty()) {
            log.info("排行榜分页数据为空 activityId:{} pageNum:{} pageSize:{}", activityId, pageNum, pageSize);
            return buildEmptyResponse(activityId, timeWindow, windowKey);
        }

        // 获取更新时间和统计信息
        String metaKey = rankKeyFactory.saleUpdateTimeKey(activityId, timeWindow, windowKey);
        Date updateTime = rankRedisRepository.getUpdateTime(metaKey);
        // 获取统计信息（用于后续扩展）
        // long[] statistics = rankRedisRepository.getRankStatistics(zsetKey);
        // long totalCount = statistics[0];

        // 组装数据
        List<GoodsMarketRankResponseDTO> rankList = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            RankItemEntity item = items.get(i);
            GoodsMarketRankResponseDTO rankItem = GoodsMarketRankResponseDTO.builder()
                    .rankNo(start + i + 1) // 排名从1开始
                    .score(item.getScore())
                    .build();

            GoodsMarketRankResponseDTO.Goods goods = GoodsMarketRankResponseDTO.Goods.builder()
                    .goodsId(item.getMember())
                    .build();
            rankItem.setGoods(goods);

            rankList.add(rankItem);
        }

        return GoodsMarketRankListResponseDTO.builder()
                .activityId(activityId)
                .rankType("SALE")
                .timeWindow(timeWindow)
                .windowKey(windowKey)
                .updateTime(updateTime)
                .rankList(rankList)
                .build();
    }

    /**
     * 查询指定商品的排名
     * 
     * @param activityId 活动ID
     * @param timeWindow 时间窗口
     * @param windowKey 窗口key
     * @param goodsId 商品ID
     * @return 商品排名信息，如果商品不在排行榜中返回null
     */
    public RankItemEntity queryGoodsRank(Long activityId, String timeWindow, String windowKey, String goodsId) {
        String zsetKey = rankKeyFactory.saleKey(activityId, timeWindow, windowKey);
        return rankRedisRepository.queryRankByGoodsId(zsetKey, goodsId);
    }

    /**
     * 构建空响应
     */
    private GoodsMarketRankListResponseDTO buildEmptyResponse(Long activityId, String timeWindow, String windowKey) {
        String metaKey = rankKeyFactory.saleUpdateTimeKey(activityId, timeWindow, windowKey);
        Date updateTime = rankRedisRepository.getUpdateTime(metaKey);
        
        return GoodsMarketRankListResponseDTO.builder()
                .activityId(activityId)
                .rankType("SALE")
                .timeWindow(timeWindow)
                .windowKey(windowKey)
                .updateTime(updateTime)
                .rankList(new ArrayList<>())
                .build();
    }
}