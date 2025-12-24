package cn.bugstack.domain.activity.service.trial.node;

import cn.bugstack.domain.activity.model.entity.MarketProductEntity;
import cn.bugstack.domain.activity.model.valobj.GroupBuyActivityDiscountVO;
import cn.bugstack.domain.activity.service.trial.factory.DefaultActivityStrategyFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

@Service("ZK")
public class ZKDiscountNode extends AbstractDiscountNode {

    @Override
    protected String getDiscountType() { return "ZK"; }

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
        BigDecimal rate = readRate(cfg);                 // 如 0.8 表示 8 折
        if (rate == null) rate = BigDecimal.ONE;

        // 与旧实现对齐：before * rate 后按业务取整（旧是 DOWN 到整数；此处返回折扣额，基类会做两位小数）
        BigDecimal afterByRate = before.multiply(rate).setScale(0, RoundingMode.DOWN);
        BigDecimal minus = before.subtract(afterByRate);

        // 不让减穿底价
        BigDecimal allowedMax = before.subtract(ctx.getMinPayFloor());
        BigDecimal applied = minus.max(BigDecimal.ZERO).min(allowedMax.max(BigDecimal.ZERO));
        return new DiscountResult(applied, "rate=" + rate);
    }

    private BigDecimal readRate(GroupBuyActivityDiscountVO.DiscountItem cfg) {
        Map<String,Object> a = cfg.getArgs();
        if (a != null) {
            Object v = a.get("rate");
            if (v != null) return toBd(v);
            Object expr = a.get("expr"); // "0.9"
            if (expr != null) return toBd(expr);
        }
        return null;
    }
    private BigDecimal toBd(Object v){ return v instanceof BigDecimal ? (BigDecimal)v : new BigDecimal(v.toString()); }
}
