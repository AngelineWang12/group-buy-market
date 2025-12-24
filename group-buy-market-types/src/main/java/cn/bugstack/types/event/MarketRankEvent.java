package cn.bugstack.types.event;

import cn.bugstack.types.enums.GroupBuyOrderEnumVO;
import lombok.Data;

import java.util.Date;

/**
 * 营销排行榜事件
 */
@Data
public class MarketRankEvent {
    private String eventId;
    private String orderId;
    private Long activityId;
    private String goodsId;
    private Integer quantity;
    private Date occurTime;
    private MarketRankEventType eventType;
    /**
     * 订单状态：拼单中/完成/失败
     */
    private GroupBuyOrderEnumVO orderStatus;
}