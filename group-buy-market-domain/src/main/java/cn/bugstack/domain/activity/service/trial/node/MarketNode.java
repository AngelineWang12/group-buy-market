package cn.bugstack.domain.activity.service.trial.node;

import cn.bugstack.domain.activity.model.entity.MarketProductEntity;
import cn.bugstack.domain.activity.model.entity.TrialBalanceEntity;
import cn.bugstack.domain.activity.model.valobj.GroupBuyActivityDiscountVO;
import cn.bugstack.domain.activity.model.valobj.SkuVO;
import cn.bugstack.domain.activity.service.trial.AbstractGroupBuyMarketSupport;
import cn.bugstack.domain.activity.service.trial.factory.DefaultActivityStrategyFactory;
import cn.bugstack.wrench.design.framework.tree.StrategyHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class MarketNode extends AbstractGroupBuyMarketSupport<MarketProductEntity, DefaultActivityStrategyFactory.DynamicContext, TrialBalanceEntity> {

    @Resource
    private NextNodeRouter nextNodeRouter;

    @Override
    public TrialBalanceEntity doApply(MarketProductEntity req, DefaultActivityStrategyFactory.DynamicContext ctx) throws Exception {
        SkuVO sku = ctx.getSkuVO();
        if (sku == null) return router(req, ctx);

        // 初始化定价上下文
        ctx.setOriginalPrice(sku.getOriginalPrice());
        ctx.setCurrentPrice(sku.getOriginalPrice());
        ctx.setCursor(0);
        ctx.setRequest(req);

        // 生成执行清单
        GroupBuyActivityDiscountVO vo = ctx.getGroupBuyActivityDiscountVO();
        if (vo == null || vo.getItems()==null || vo.getItems().isEmpty()) {
            return router(req, ctx);
        }

        List<GroupBuyActivityDiscountVO.DiscountItem> candidates = filterByBusinessGate(vo.getItems(), req, ctx);
        List<GroupBuyActivityDiscountVO.DiscountItem> chosen = resolveMutexAndSort(candidates);
        List<String> itinerary = chosen.stream().map(GroupBuyActivityDiscountVO.DiscountItem::getPlan).collect(Collectors.toList());
        Map<String, GroupBuyActivityDiscountVO.DiscountItem> map =
                chosen.stream().collect(Collectors.toMap(GroupBuyActivityDiscountVO.DiscountItem::getPlan, it -> it, (a,b)->a, LinkedHashMap::new));

        ctx.setItinerary(itinerary);
        ctx.setItemByPlan(map);

        log.info("定价管线 itinerary={} originalPrice={}", itinerary, ctx.getOriginalPrice());
        return router(req, ctx);
    }

    @Override
    public StrategyHandler<MarketProductEntity, DefaultActivityStrategyFactory.DynamicContext, TrialBalanceEntity> get(MarketProductEntity req, DefaultActivityStrategyFactory.DynamicContext ctx) {
        // 从 itinerary[0] 开始
        return nextNodeRouter.next(req, ctx);
    }

    // —— 以下为编排所需的工具方法 ——

    /** 业务前置过滤（示例：渠道/端/人群等，此处简单返回） */
    private List<GroupBuyActivityDiscountVO.DiscountItem> filterByBusinessGate(List<GroupBuyActivityDiscountVO.DiscountItem> items,
                                                                               MarketProductEntity req,
                                                                               DefaultActivityStrategyFactory.DynamicContext ctx) {
        // 可在这里依据 req/context 做渠道、人群、端过滤；示例直接返回
        return items.stream()
                .filter(Objects::nonNull)
                .filter(it -> it.getPlan()!=null && !it.getPlan().isEmpty())
                .collect(Collectors.toList());
    }

    /** 互斥消解 + 排序（priority ASC） */
    private List<GroupBuyActivityDiscountVO.DiscountItem> resolveMutexAndSort(List<GroupBuyActivityDiscountVO.DiscountItem> items) {
        Map<String, GroupBuyActivityDiscountVO.DiscountItem> keepByGroup = new HashMap<>();
        List<GroupBuyActivityDiscountVO.DiscountItem> kept = new ArrayList<>();

        for (GroupBuyActivityDiscountVO.DiscountItem it : items) {
            if (it.getMutexGroup()==null || it.getMutexGroup().isEmpty()) {
                kept.add(it);
                continue;
            }
            GroupBuyActivityDiscountVO.DiscountItem old = keepByGroup.get(it.getMutexGroup());
            if (old == null || comparePriority(it, old) < 0) {
                keepByGroup.put(it.getMutexGroup(), it);
            }
        }
        kept.addAll(keepByGroup.values());
        kept.sort(this::comparePriority);
        return kept;
    }

    private int comparePriority(GroupBuyActivityDiscountVO.DiscountItem a, GroupBuyActivityDiscountVO.DiscountItem b) {
        int pa = a.getPriority() == null ? Integer.MAX_VALUE : a.getPriority();
        int pb = b.getPriority() == null ? Integer.MAX_VALUE : b.getPriority();
        return Integer.compare(pa, pb);
    }
}
