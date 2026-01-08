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
    
    // 查询TopN数量（可选，默认10）
    private Integer topN;
    
    // 分页查询：页码（从1开始）
    private Integer pageNum;
    
    // 分页查询：每页大小
    private Integer pageSize;
    
    // 查询指定商品的排名（可选）
    private String goodsId;
}
