package cn.bugstack.types.event;

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
}