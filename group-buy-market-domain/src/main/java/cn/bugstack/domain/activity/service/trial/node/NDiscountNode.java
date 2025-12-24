package cn.bugstack.domain.activity.service.trial.node;

import cn.bugstack.domain.activity.model.entity.MarketProductEntity;
import cn.bugstack.domain.activity.model.valobj.GroupBuyActivityDiscountVO;
import cn.bugstack.domain.activity.service.trial.factory.DefaultActivityStrategyFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service("N")
public class NDiscountNode extends AbstractDiscountNode {

    @Override
    protected String getDiscountType() { return "N"; }

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
        BigDecimal n = readN(cfg);
        if (n == null) n = before; // 未配置则不变

        // N 元购等价于：折扣额 = max(0, before - N)
        BigDecimal minus = before.subtract(n);
        if (minus.signum() < 0) minus = BigDecimal.ZERO;

        // 不让减穿底价
        BigDecimal allowedMax = before.subtract(ctx.getMinPayFloor());
        BigDecimal applied = minus.min(allowedMax.max(BigDecimal.ZERO));
        return new DiscountResult(applied, "n=" + n);
    }

    private BigDecimal readN(GroupBuyActivityDiscountVO.DiscountItem cfg) {
        Map<String,Object> a = cfg.getArgs();
        if (a != null) {
            Object v = a.get("n");
            if (v == null) v = a.get("price");
            if (v != null) return toBd(v);
            Object expr = a.get("expr"); // "99"
            if (expr != null) return toBd(expr);
        }
        return null;
    }
    private BigDecimal toBd(Object v){ return v instanceof BigDecimal ? (BigDecimal)v : new BigDecimal(v.toString()); }
}
