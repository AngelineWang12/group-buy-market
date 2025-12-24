package cn.bugstack.domain.trade.adapter.port;

import cn.bugstack.domain.trade.model.entity.NotifyTaskEntity;
import cn.bugstack.types.event.MarketRankEvent;

/**
 * @author Fuzhengwei bugstack.cn @小傅哥
 * @description 交易接口服务接口
 * @create 2025-01-31 10:38
 */
public interface ITradePort {

    String groupBuyNotify(NotifyTaskEntity notifyTask) throws Exception;

    void sendRankEvent(MarketRankEvent rankEvent);
}
