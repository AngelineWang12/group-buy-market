package cn.bugstack.domain.activity.service.trial.factory;

import cn.bugstack.domain.activity.model.entity.MarketProductEntity;
import cn.bugstack.domain.activity.model.entity.TrialBalanceEntity;
import cn.bugstack.domain.activity.model.valobj.GroupBuyActivityDiscountVO;
import cn.bugstack.domain.activity.model.valobj.SkuVO;
import cn.bugstack.domain.activity.service.trial.node.RootNode;
import cn.bugstack.wrench.design.framework.tree.StrategyHandler;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;

/**
 * @author Fuzhengwei bugstack.cn @小傅哥
 * @description 活动策略工厂
 * @create 2024-12-14 13:41
 */
@Service
public class DefaultActivityStrategyFactory {

    private final RootNode rootNode;

    public DefaultActivityStrategyFactory(RootNode rootNode) {
        this.rootNode = rootNode;
    }

    public StrategyHandler<MarketProductEntity, DynamicContext, TrialBalanceEntity> strategyHandler() {
        return rootNode;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DynamicContext {

        // 拼团活动营销配置值对象
        private GroupBuyActivityDiscountVO groupBuyActivityDiscountVO;
        // 商品信息
        private SkuVO skuVO;
        // 折扣金额
        private BigDecimal deductionPrice;
        // 支付金额
        private BigDecimal payPrice;
        // 活动可见性限制
        private boolean visible;
        // 活动
        private boolean enable;

        private BigDecimal originalPrice;                    // 原价
        private BigDecimal currentPrice;                     // 滚动价格
        private BigDecimal minPayFloor = BigDecimal.ZERO;    // 最低价保护（如 0 元）
        private List<String> itinerary;                      // 执行清单：["DIRECT_REDUCE","DISCOUNT","FULL_REDUCE"...]
        private int cursor;                                  // 当前执行到第几个节点
        private Map<String, GroupBuyActivityDiscountVO.DiscountItem> itemByPlan; // plan -> 配置
        private Set<String> appliedDiscounts = new HashSet<>();                  // 已使用的优惠（去重）
        private List<PricingStep> steps = new ArrayList<>(); // 审计轨迹
        private Object request;                               // 可选：保留 request 引用，路由器里用得到
    }

}
