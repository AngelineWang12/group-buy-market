package cn.bugstack.domain.activity.service.trial.node;

import cn.bugstack.domain.activity.model.entity.MarketProductEntity;
import cn.bugstack.domain.activity.service.trial.factory.DefaultActivityStrategyFactory;
import cn.bugstack.wrench.design.framework.tree.StrategyHandler;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

@Component
public class NextNodeRouter {

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

    public StrategyHandler<MarketProductEntity, DefaultActivityStrategyFactory.DynamicContext, cn.bugstack.domain.activity.model.entity.TrialBalanceEntity>
    next(MarketProductEntity req, DefaultActivityStrategyFactory.DynamicContext ctx) {
        if (ctx.getItinerary()==null || ctx.getCursor() >= ctx.getItinerary().size()) {
            return endNode;
        }
        String plan = ctx.getItinerary().get(ctx.getCursor());
        AbstractDiscountNode node = nodesByPlan.get(plan);
        return node != null ? node : endNode;
    }
}
