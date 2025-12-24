package cn.bugstack.domain.activity.service.trial.factory;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Locale;

/**
 * 统一生成排行榜 Redis Key
 */
@Component
public class RankKeyFactory {

    private static final String PREFIX = "rank";
    private static final String SCOPE_ACTIVITY = "act";

    /**
     * 生成销量榜 Key（ZSET）
     * rank:act:{activityId}:{timeWindow}:{windowKey}:sale
     */
    public String saleKey(Long activityId, String timeWindow, String windowKey) {
        String tw = normalizeTimeWindow(timeWindow);
        String wk = normalizeWindowKey(activityId, windowKey, tw);
        return String.join(":",
                PREFIX, SCOPE_ACTIVITY, String.valueOf(activityId),
                tw, wk,
                "sale"
        );
    }

    /**
     * 榜单更新时间 Key（STRING）
     * rank:act:{activityId}:{timeWindow}:{windowKey}:sale:meta:updateTime
     */
    public String saleUpdateTimeKey(Long activityId, String timeWindow, String windowKey) {
        return saleKey(activityId, timeWindow, windowKey) + ":meta:updateTime";
    }

    /**
     * 统一 timeWindow：空 -> ACTIVITY；大小写统一为大写
     */
    private String normalizeTimeWindow(String timeWindow) {
        if (!StringUtils.hasText(timeWindow)) return "ACTIVITY";
        return timeWindow.trim().toUpperCase(Locale.ROOT);
    }

    /**
     * windowKey 兜底规则：
     * - ACTIVITY：默认用 activityId（保证每个活动有唯一 key）
     * - 其它窗口：如果没传，给一个保守默认（也可以直接抛异常，看你接口约束）
     */
    private String normalizeWindowKey(Long activityId, String windowKey, String timeWindow) {
        if (StringUtils.hasText(windowKey)) return windowKey.trim();

        if ("ACTIVITY".equals(timeWindow)) {
            return String.valueOf(activityId);
        }

        // DAY/WEEK/MONTH/HOUR_ROLLING 等：你可以选择默认当天，也可以强制要求传入
        // 这里给保守默认，避免 NPE；但我更建议：非 ACTIVITY 时强制传 windowKey
        return "UNKNOWN";
    }
}
