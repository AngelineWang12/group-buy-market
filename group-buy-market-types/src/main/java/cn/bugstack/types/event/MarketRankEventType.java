package cn.bugstack.types.event;

/**
 * 排行榜事件类型
 */
public enum MarketRankEventType {
    UNPAID,
    REFUND_SUCCESS,
    ORDER_CANCEL,
    GROUP_BUY_PROGRESS,  // 拼单中状态变化
    GROUP_BUY_COMPLETE   // 拼单完成
}