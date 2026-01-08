package cn.bugstack.trigger.http;

import cn.bugstack.api.IMarketIndexService;
import cn.bugstack.api.dto.*;
import cn.bugstack.api.response.Response;
import cn.bugstack.domain.activity.model.entity.MarketProductEntity;
import cn.bugstack.domain.activity.model.entity.TrialBalanceEntity;
import cn.bugstack.domain.activity.model.entity.UserGroupBuyOrderDetailEntity;
import cn.bugstack.domain.activity.model.valobj.GroupBuyActivityDiscountVO;
import cn.bugstack.domain.activity.model.valobj.TeamStatisticVO;
import cn.bugstack.domain.activity.service.IIndexGroupBuyMarketService;
import cn.bugstack.domain.activity.service.IRankGroupBuyMarketService;
import cn.bugstack.types.enums.ResponseCode;
import cn.bugstack.wrench.rate.limiter.types.annotations.RateLimiterAccessInterceptor;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author Fuzhengwei bugstack.cn @小傅哥
 * @description 营销首页服务
 * @create 2025-02-02 16:03
 */
@Slf4j
@RestController()
@CrossOrigin("*")
@RequestMapping("/api/v1/gbm/index/")
public class MarketIndexController implements IMarketIndexService {

    @Resource
    private IIndexGroupBuyMarketService indexGroupBuyMarketService;
    @Resource
    private IRankGroupBuyMarketService rankGroupBuyMarketService;

    // 用于并行查询商品价格的线程池
    private final ExecutorService priceQueryExecutor = Executors.newFixedThreadPool(10);
    @RateLimiterAccessInterceptor(key = "userId", fallbackMethod = "queryGroupBuyMarketConfigFallBack", permitsPerSecond = 1.0d, blacklistCount = 1)
    @RequestMapping(value = "query_group_buy_market_config", method = RequestMethod.POST)
    @Override
    public Response<GoodsMarketResponseDTO> queryGroupBuyMarketConfig(@RequestBody GoodsMarketRequestDTO requestDTO) {
        try {
            log.info("查询拼团营销配置开始:{} goodsId:{}", requestDTO.getUserId(), requestDTO.getGoodsId());

            if (StringUtils.isBlank(requestDTO.getUserId()) || StringUtils.isBlank(requestDTO.getSource()) || StringUtils.isBlank(requestDTO.getChannel()) || StringUtils.isBlank(requestDTO.getGoodsId())) {
                return Response.<GoodsMarketResponseDTO>builder()
                        .code(ResponseCode.ILLEGAL_PARAMETER.getCode())
                        .info(ResponseCode.ILLEGAL_PARAMETER.getInfo())
                        .build();
            }

            // 1. 营销优惠试算
            TrialBalanceEntity trialBalanceEntity = indexGroupBuyMarketService.indexMarketTrial(MarketProductEntity.builder()
                    .userId(requestDTO.getUserId())
                    .source(requestDTO.getSource())
                    .channel(requestDTO.getChannel())
                    .goodsId(requestDTO.getGoodsId())
                    .build());


            GroupBuyActivityDiscountVO groupBuyActivityDiscountVO = trialBalanceEntity.getGroupBuyActivityDiscountVO();
            Long activityId = groupBuyActivityDiscountVO.getActivityId();

            // 2. 查询拼团组队
            List<UserGroupBuyOrderDetailEntity> userGroupBuyOrderDetailEntities = indexGroupBuyMarketService.queryInProgressUserGroupBuyOrderDetailList(activityId, requestDTO.getUserId(), 1, 2);

            // 3. 统计拼团数据
            TeamStatisticVO teamStatisticVO = indexGroupBuyMarketService.queryTeamStatisticByActivityId(activityId);

            GoodsMarketResponseDTO.Goods goods = GoodsMarketResponseDTO.Goods.builder()
                    .goodsId(trialBalanceEntity.getGoodsId())
                    .originalPrice(trialBalanceEntity.getOriginalPrice())
                    .deductionPrice(trialBalanceEntity.getDeductionPrice())
                    .payPrice(trialBalanceEntity.getPayPrice())
                    .build();

            List<GoodsMarketResponseDTO.Team> teams = new ArrayList<>();
            if (null != userGroupBuyOrderDetailEntities && !userGroupBuyOrderDetailEntities.isEmpty()) {
                for (UserGroupBuyOrderDetailEntity userGroupBuyOrderDetailEntity : userGroupBuyOrderDetailEntities) {
                    GoodsMarketResponseDTO.Team team = GoodsMarketResponseDTO.Team.builder()
                            .userId(userGroupBuyOrderDetailEntity.getUserId())
                            .teamId(userGroupBuyOrderDetailEntity.getTeamId())
                            .activityId(userGroupBuyOrderDetailEntity.getActivityId())
                            .targetCount(userGroupBuyOrderDetailEntity.getTargetCount())
                            .completeCount(userGroupBuyOrderDetailEntity.getCompleteCount())
                            .lockCount(userGroupBuyOrderDetailEntity.getLockCount())
                            .validStartTime(userGroupBuyOrderDetailEntity.getValidStartTime())
                            .validEndTime(userGroupBuyOrderDetailEntity.getValidEndTime())
                            .validTimeCountdown(GoodsMarketResponseDTO.Team.differenceDateTime2Str(new Date(), userGroupBuyOrderDetailEntity.getValidEndTime()))
                            .outTradeNo(userGroupBuyOrderDetailEntity.getOutTradeNo())
                            .build();
                    teams.add(team);
                }
            }

            GoodsMarketResponseDTO.TeamStatistic teamStatistic = GoodsMarketResponseDTO.TeamStatistic.builder()
                    .allTeamCount(teamStatisticVO.getAllTeamCount())
                    .allTeamCompleteCount(teamStatisticVO.getAllTeamCompleteCount())
                    .allTeamUserCount(teamStatisticVO.getAllTeamUserCount())
                    .build();

            Response<GoodsMarketResponseDTO> response = Response.<GoodsMarketResponseDTO>builder()
                    .code(ResponseCode.SUCCESS.getCode())
                    .info(ResponseCode.SUCCESS.getInfo())
                    .data(GoodsMarketResponseDTO.builder()
                            .activityId(activityId)
                            .goods(goods)
                            .teamList(teams)
                            .teamStatistic(teamStatistic)
                            .build())
                    .build();

            log.info("查询拼团营销配置完成:{} goodsId:{} response:{}", requestDTO.getUserId(), requestDTO.getGoodsId(), JSON.toJSONString(response));

            return response;
        } catch (Exception e) {
            log.error("查询拼团营销配置失败:{} goodsId:{}", requestDTO.getUserId(), requestDTO.getGoodsId(), e);
            return Response.<GoodsMarketResponseDTO>builder()
                    .code(ResponseCode.UN_ERROR.getCode())
                    .info(ResponseCode.UN_ERROR.getInfo())
                    .build();
        }
    }

    @RateLimiterAccessInterceptor(key = "userId", fallbackMethod = "queryGroupBuyMarketRankListFallBack", permitsPerSecond = 1.0d, blacklistCount = 1)
    @RequestMapping(value = "query_group_buy_market_rank_list", method = RequestMethod.POST)
    @Override
    public Response<GoodsMarketRankListResponseDTO> queryGroupBuyMarketRankList(GoodsMarketRankRequestDTO req) throws Exception {
        try {
            log.info("查询拼团营销排行榜开始:{} activityId:{}", req.getUserId(), req.getActivityId());

            if (StringUtils.isBlank(req.getUserId()) || StringUtils.isBlank(req.getSource()) || StringUtils.isBlank(req.getChannel()) || req.getActivityId() == null) {
                return Response.<GoodsMarketRankListResponseDTO>builder()
                        .code(ResponseCode.ILLEGAL_PARAMETER.getCode())
                        .info(ResponseCode.ILLEGAL_PARAMETER.getInfo())
                        .build();
            }

            Long activityId = req.getActivityId();
            String timeWindow = (req.getTimeWindow() == null) ? "ACTIVITY" : req.getTimeWindow();
            String windowKey = (req.getWindowKey() == null || req.getWindowKey().isEmpty())
                    ? String.valueOf(activityId)
                    : req.getWindowKey();

            // 1) Redis 取 TopN 基本数据
            // 支持自定义TopN，如果请求中没有指定，使用默认值10
            Integer topN = req.getTopN();
            GoodsMarketRankListResponseDTO resp;
            if (topN != null && topN > 0) {
                // 使用反射或类型转换调用重载方法（这里需要修改接口支持）
                // 暂时使用默认方法，后续可以通过扩展接口支持
                resp = rankGroupBuyMarketService.queryTopNByActivityId(activityId, timeWindow, windowKey);
            } else {
                resp = rankGroupBuyMarketService.queryTopNByActivityId(activityId, timeWindow, windowKey);
            }

            // 2) 并行查询商品价格信息，提升性能
            List<GoodsMarketRankResponseDTO> rankList = resp.getRankList();
            if (rankList != null && !rankList.isEmpty()) {
                // 使用 CompletableFuture 并行查询商品价格
                List<CompletableFuture<Void>> futures = rankList.stream()
                        .map(rankItem -> CompletableFuture.runAsync(() -> {
                            try {
                                String goodsId = rankItem.getGoods().getGoodsId();
                                
                                // 通过试算获取商品价格信息
                                TrialBalanceEntity trialBalanceEntity = indexGroupBuyMarketService.indexMarketTrial(
                                        MarketProductEntity.builder()
                                                .activityId(activityId)
                                                .userId(req.getUserId())
                                                .goodsId(goodsId)
                                                .source(req.getSource())
                                                .channel(req.getChannel())
                                                .build());

                                // 构建商品信息
                                GoodsMarketRankResponseDTO.Goods goods = GoodsMarketRankResponseDTO.Goods.builder()
                                        .goodsId(goodsId)
                                        .originalPrice(trialBalanceEntity.getOriginalPrice())
                                        .deductionPrice(trialBalanceEntity.getDeductionPrice())
                                        .payPrice(trialBalanceEntity.getPayPrice())
                                        .build();

                                // 设置商品信息
                                rankItem.setGoods(goods);
                            } catch (Exception e) {
                                log.error("查询商品价格失败 goodsId:{}", rankItem.getGoods().getGoodsId(), e);
                                // 异常时保持原有商品信息（只有goodsId）
                            }
                        }, priceQueryExecutor))
                        .collect(Collectors.toList());

                // 等待所有查询完成
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            }

            log.info("查询拼团营销排行榜完成:{} activityId:{}", req.getUserId(), req.getActivityId());

            return Response.<GoodsMarketRankListResponseDTO>builder()
                    .code(ResponseCode.SUCCESS.getCode())
                    .info(ResponseCode.SUCCESS.getInfo())
                    .data(resp)
                    .build();
        } catch (Exception e) {
            log.error("查询拼团营销排行榜失败:{} activityId:{}", req.getUserId(), req.getActivityId(), e);
            return Response.<GoodsMarketRankListResponseDTO>builder()
                    .code(ResponseCode.UN_ERROR.getCode())
                    .info(ResponseCode.UN_ERROR.getInfo())
                    .build();
        }
    }

}
