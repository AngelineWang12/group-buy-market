package cn.bugstack.trigger.listener;

import cn.bugstack.domain.activity.adapter.repository.IRankRedisRepository;
import cn.bugstack.domain.activity.service.trial.factory.RankKeyFactory;
import cn.bugstack.types.enums.GroupBuyOrderEnumVO;
import cn.bugstack.types.event.MarketRankEvent;
import cn.bugstack.types.event.MarketRankEventType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.Duration;

/**
 * 排行榜事件消费者，实时维护 Redis 排行榜
 */
@Slf4j
@Component
public class MarketRankEventConsumer {

    private static final String TIME_WINDOW_ACTIVITY = "ACTIVITY";
    private static final long DEFAULT_TTL_SECONDS = 30L * 24 * 3600;
    private static final long COMPLETE_WEIGHT = 10L;
    private static final long PROGRESS_WEIGHT = 3L;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Resource
    private RankKeyFactory rankKeyFactory;
    @Resource
    private IRankRedisRepository rankRedisRepository;

    @RabbitListener(queues = "market.rank.queue")
    public void onMessage(String message) {
        try {
            MarketRankEvent event = objectMapper.readValue(message, MarketRankEvent.class);
            if (event == null || event.getEventId() == null) {
                log.warn("排行榜事件解析失败，消息: {}", message);
                return;
            }

            String dedupKey = "dedup:rank:" + event.getEventId();
            if (!rankRedisRepository.tryDedup(dedupKey, Duration.ofDays(3))) {
                return;
            }

            long delta = calculateWeightedDelta(event);
            if (delta == 0L) {
                return;
            }

            String zsetKey = rankKeyFactory.saleKey(event.getActivityId(), TIME_WINDOW_ACTIVITY, String.valueOf(event.getActivityId()));
            String metaKey = rankKeyFactory.saleUpdateTimeKey(event.getActivityId(), TIME_WINDOW_ACTIVITY, String.valueOf(event.getActivityId()));

            long now = System.currentTimeMillis();
            rankRedisRepository.incrWithMeta(zsetKey, metaKey, event.getGoodsId(), delta, DEFAULT_TTL_SECONDS, now);

            log.info("排行榜事件处理完成 eventId:{} goodsId:{} delta:{} key:{}", event.getEventId(), event.getGoodsId(), delta, zsetKey);
        } catch (Exception e) {
            log.error("排行榜事件消费异常, message: {}", message, e);
        }
    }

    private long calculateWeightedDelta(MarketRankEvent event) {
        long qty = event.getQuantity() == null ? 1L : event.getQuantity();
        GroupBuyOrderEnumVO status = event.getOrderStatus();
        long weight = PROGRESS_WEIGHT;
        if (status == GroupBuyOrderEnumVO.COMPLETE) {
            weight = COMPLETE_WEIGHT;
        } else if (status == GroupBuyOrderEnumVO.PROGRESS) {
            weight = PROGRESS_WEIGHT;
        } else if (status == GroupBuyOrderEnumVO.FAIL) {
            weight = 0L;
        }

        long delta = qty * weight;
        if (event.getEventType() == MarketRankEventType.REFUND_SUCCESS
                || event.getEventType() == MarketRankEventType.ORDER_CANCEL) {
            delta = -delta;
        }
        return delta;
    }
}