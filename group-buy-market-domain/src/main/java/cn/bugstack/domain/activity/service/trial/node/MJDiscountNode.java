package cn.bugstack.domain.activity.service.trial.node;

import cn.bugstack.domain.activity.model.entity.MarketProductEntity;
import cn.bugstack.domain.activity.model.valobj.GroupBuyActivityDiscountVO;
import cn.bugstack.domain.activity.service.trial.factory.DefaultActivityStrategyFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service("MJ")
public class MJDiscountNode extends AbstractDiscountNode {

    @Override
    protected String getDiscountType() { return "MJ"; }

    @Override
    protected boolean isApplicable(MarketProductEntity req, DefaultActivityStrategyFactory.DynamicContext ctx) {
        if (ctx.getAppliedDiscounts().contains(getDiscountType())) return false;
        GroupBuyActivityDiscountVO.DiscountItem cfg = ctx.getItemByPlan().get(getDiscountType());
        if (cfg == null) return false;

        BigDecimal threshold = readThreshold(cfg);
        // 达门槛才适用；没配门槛则认为可用
        return threshold == null || ctx.getCurrentPrice().compareTo(threshold) >= 0;
    }

    @Override
    protected DiscountResult doCalculate(MarketProductEntity req,
                                         DefaultActivityStrategyFactory.DynamicContext ctx,
                                         GroupBuyActivityDiscountVO.DiscountItem cfg) {
        BigDecimal before = ctx.getCurrentPrice();
        BigDecimal threshold = readThreshold(cfg);
        BigDecimal minus = readMinus(cfg);

        if (threshold != null && before.compareTo(threshold) < 0) {
            return new DiscountResult(BigDecimal.ZERO, "below-threshold");
        }
        if (minus == null) minus = BigDecimal.ZERO;

        // 不让减穿底价
        BigDecimal allowedMax = before.subtract(ctx.getMinPayFloor());
        BigDecimal applied = minus.max(BigDecimal.ZERO).min(allowedMax.max(BigDecimal.ZERO));
        return new DiscountResult(applied, "full=" + (threshold==null?"-":threshold) + ", minus=" + minus);
    }

    // ------- helpers -------
    private BigDecimal readThreshold(GroupBuyActivityDiscountVO.DiscountItem cfg) {
        Map<String,Object> a = cfg.getArgs();
        if (a != null) {
            Object v = firstNonNull(a.get("full"), a.get("reach"), a.get("x"), a.get("threshold"));
            if (v != null) return toBd(v);
            Object expr = a.get("expr"); // "100,10"
            if (expr != null && expr.toString().contains(",")) {
                String[] sp = expr.toString().split(",");
                return toBd(sp[0].trim());
            }
        }
        return null;
    }
    private BigDecimal readMinus(GroupBuyActivityDiscountVO.DiscountItem cfg) {
        Map<String,Object> a = cfg.getArgs();
        if (a != null) {
            Object v = firstNonNull(a.get("minus"), a.get("y"), a.get("reduce"));
            if (v != null) return toBd(v);
            Object expr = a.get("expr");
            if (expr != null && expr.toString().contains(",")) {
                String[] sp = expr.toString().split(",");
                return toBd(sp[1].trim());
            }
        }
        return null;
    }
    private Object firstNonNull(Object... vs){ for(Object v:vs){ if(v!=null) return v;} return null; }
    private BigDecimal toBd(Object v){ return v instanceof BigDecimal ? (BigDecimal)v : new BigDecimal(v.toString()); }
}
