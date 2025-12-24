package cn.bugstack.api.dto;

import lombok.Data;

/**
 * @author Fuzhengwei bugstack.cn @小傅哥
 * @description 商品营销请求对象
 * @create 2025-02-02 12:19
 */
@Data
public class GoodsMarketRankRequestDTO {

    // 用户ID
    private String userId;
    // 渠道
    private String source;
    // 来源
    private String channel;
    // 活动ID
    private Long activityId;
    // DAY / WEEK / MONTH / ACTIVITY / HOUR_ROLLING
    private String timeWindow;

    private String windowKey;
}
