package cn.bugstack.domain.activity.service.trial.node;

import cn.bugstack.domain.activity.model.entity.MarketProductEntity;
import cn.bugstack.domain.activity.model.entity.TrialBalanceEntity;
import cn.bugstack.domain.activity.model.valobj.GroupBuyActivityDiscountVO;
import cn.bugstack.domain.activity.service.trial.AbstractGroupBuyMarketSupport;
import cn.bugstack.domain.activity.service.trial.factory.DefaultActivityStrategyFactory;
import cn.bugstack.domain.activity.service.trial.factory.PricingStep;
import cn.bugstack.wrench.design.framework.tree.StrategyHandler;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public abstract class AbstractDiscountNode
        extends AbstractGroupBuyMarketSupport<MarketProductEntity, DefaultActivityStrategyFactory.DynamicContext, cn.bugstack.domain.activity.model.entity.TrialBalanceEntity> {

    @Resource
    private Map<String, AbstractDiscountNode> discountNodesByBeanName; // Spring自动注入：beanName -> bean

    private final Map<String, AbstractDiscountNode> nodesByPlan = new HashMap<>(); // plan -> bean

    @Resource
    private EndNode endNode;

    /** 将以 plan 命名的 @Service("PLAN") Bean 注册为 plan -> node */
    @PostConstruct
    public void init() {
        for (Map.Entry<String, AbstractDiscountNode> e : discountNodesByBeanName.entrySet()) {
            AbstractDiscountNode node = e.getValue();
            // bean 标注为 @Service("DIRECT_REDUCE") 时，这里能拿到 key=“DIRECT_REDUCE”
            nodesByPlan.putIfAbsent(e.getKey(), node);
            // 兜底：若开发者没用别名，则用节点自报的 plan
            nodesByPlan.putIfAbsent(node.getDiscountType(), node);
        }
    }

    /** 该节点对应的策略标识（与 DiscountItem.plan 一致） */
    protected abstract String getDiscountType();

    /** 判断当前请求/上下文是否适用该优惠（除“是否配置了该 plan”外的业务前置校验） */
    protected abstract boolean isApplicable(MarketProductEntity req, DefaultActivityStrategyFactory.DynamicContext ctx);

    /** 执行一次优惠，返回“本次优惠额”与说明 */
    protected abstract DiscountResult doCalculate(MarketProductEntity req,
                                                  DefaultActivityStrategyFactory.DynamicContext ctx,
                                                  GroupBuyActivityDiscountVO.DiscountItem cfg);

    @Data
    protected static class DiscountResult {
        private final BigDecimal discount; // >= 0
        private final String reason;
    }

    @Override
    public cn.bugstack.domain.activity.model.entity.TrialBalanceEntity doApply(MarketProductEntity req,
                                                                               DefaultActivityStrategyFactory.DynamicContext ctx) throws Exception {
        String plan = getDiscountType();
        GroupBuyActivityDiscountVO.DiscountItem cfg =
                ctx.getItemByPlan() == null ? null : ctx.getItemByPlan().get(plan);

        if (cfg != null && isApplicable(req, ctx)) {
            BigDecimal before = ctx.getCurrentPrice();
            DiscountResult dr;
            try {
                dr = doCalculate(req, ctx, cfg);
            } catch (Exception e) {
                log.warn("优惠节点执行异常，plan={}，跳过。原因={}", plan, e.getMessage());
                // 异常时记录轨迹但不改变价格
                ctx.getSteps().add(new PricingStep(plan, before, BigDecimal.ZERO, before, "error:" + e.getMessage()));
                ctx.setCursor(ctx.getCursor() + 1);
                return router(req, ctx);
            }

            BigDecimal minus = dr.getDiscount() == null ? BigDecimal.ZERO : dr.getDiscount();
            if (minus.signum() < 0) minus = BigDecimal.ZERO;

            // 封底与精度处理
            BigDecimal after = before.subtract(minus)
                    .max(ctx.getMinPayFloor())
                    .setScale(2, RoundingMode.HALF_UP);

            ctx.setCurrentPrice(after);
            ctx.getAppliedDiscounts().add(plan);
            ctx.getSteps().add(new PricingStep(plan, before, minus, after, dr.getReason()));

            log.info("优惠[{}]：{} -> {} (-{}) reason={}", plan, before, after, minus, dr.getReason());
        }

        // 推进游标，进入下一个节点
        ctx.setCursor(ctx.getCursor() + 1);
        return router(req, ctx);
    }

    @Override
    public StrategyHandler<MarketProductEntity, DefaultActivityStrategyFactory.DynamicContext, cn.bugstack.domain.activity.model.entity.TrialBalanceEntity>
    get(MarketProductEntity req, DefaultActivityStrategyFactory.DynamicContext ctx) {
        if (ctx.getItinerary()==null || ctx.getCursor() >= ctx.getItinerary().size()) {
            return endNode;
        }
        String plan = ctx.getItinerary().get(ctx.getCursor());
        AbstractDiscountNode node = nodesByPlan.get(plan);
        return node != null ? node : endNode;
    }
}
