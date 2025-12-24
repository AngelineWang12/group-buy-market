package cn.bugstack.domain.activity.service.trial.factory;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class PricingStep {
    private String plan;        // 策略标识
    private BigDecimal before;  // 前价
    private BigDecimal discount;// 本次优惠额（>=0）
    private BigDecimal after;   // 后价
    private String reason;      // 说明（档位命中/封顶/未达门槛等）
}
