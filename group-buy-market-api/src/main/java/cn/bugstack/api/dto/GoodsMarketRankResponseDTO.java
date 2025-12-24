package cn.bugstack.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Fuzhengwei bugstack.cn @小傅哥
 * @description 商品营销应答对象
 * @create 2025-02-02 12:20
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class GoodsMarketRankResponseDTO {

    // 商品信息
    private Goods goods;
/*    // 组队统计
    private ScoreStatistic scoreStatistic;*/
    // 商品得分
    private Long score;
    // 商品排名
    private Integer rankNo;

    /**
     * 商品信息
     */
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Goods {
        // 商品ID
        private String goodsId;
        // 原始价格
        private BigDecimal originalPrice;
        // 折扣金额
        private BigDecimal deductionPrice;
        // 支付价格
        private BigDecimal payPrice;
    }

/*    *//**
     * 组队统计
     *//*
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ScoreStatistic {
        *//** 原始指标（用于展示/校验） *//*
        private Long orderCount;       // 下单数（建议明确是“锁单成功数” or “创建订单数”）
        private Long payCount;         // 支付数
        private Long payAmountFen;     // GMV（分），可选但很有用
    }*/

}
