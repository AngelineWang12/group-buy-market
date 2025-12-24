package cn.bugstack.domain.activity.service.trial.node;

import cn.bugstack.domain.activity.model.entity.MarketProductEntity;
import cn.bugstack.domain.activity.model.valobj.GroupBuyActivityDiscountVO;
import cn.bugstack.domain.activity.service.trial.factory.DefaultActivityStrategyFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service("ZJ")
public class ZJDiscountNode extends AbstractDiscountNode {

    @Override
    protected String getDiscountType() { return "ZJ"; }

    @Override
    protected boolean isApplicable(MarketProductEntity req, DefaultActivityStrategyFactory.DynamicContext ctx) {
        if (ctx.getAppliedDiscounts().contains(getDiscountType())) return false;
        return ctx.getItemByPlan().get(getDiscountType()) != null;
    }

    @Override
    protected DiscountResult doCalculate(MarketProductEntity req,
                                         DefaultActivityStrategyFactory.DynamicContext ctx,
                                         GroupBuyActivityDiscountVO.DiscountItem cfg) {
        BigDecimal before = ctx.getCurrentPrice();
        BigDecimal reduce = readReduce(cfg);
        if (reduce == null) reduce = BigDecimal.ZERO;

        BigDecimal allowedMax = before.subtract(ctx.getMinPayFloor());
        BigDecimal applied = reduce.max(BigDecimal.ZERO).min(allowedMax.max(BigDecimal.ZERO));
        return new DiscountResult(applied, "reduce=" + reduce);
    }

    private BigDecimal readReduce(GroupBuyActivityDiscountVO.DiscountItem cfg) {
        Map<String,Object> a = cfg.getArgs();
        if (a != null) {
            Object v = a.get("reduceAmount");
            if (v == null) v = a.get("reduce");
            if (v == null) v = a.get("amount");
            if (v != null) return toBd(v);
            Object expr = a.get("expr"); // "20"
            if (expr != null) return toBd(expr);
        }
        return null;
    }
    private BigDecimal toBd(Object v){ return v instanceof BigDecimal ? (BigDecimal)v : new BigDecimal(v.toString()); }
}
