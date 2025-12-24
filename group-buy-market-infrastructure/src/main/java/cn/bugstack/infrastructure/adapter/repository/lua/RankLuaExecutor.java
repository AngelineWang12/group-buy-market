package cn.bugstack.infrastructure.adapter.repository.lua;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;

/**
 * 排行榜 Lua 脚本执行器
 */
@Component
public class RankLuaExecutor {

    private static final String INCR_WITH_META_SCRIPT = ""
            + "redis.call('ZINCRBY', KEYS[1], ARGV[2], ARGV[1])\n"
            + "redis.call('SET', KEYS[2], ARGV[4])\n"
            + "redis.call('EXPIRE', KEYS[1], ARGV[3])\n"
            + "redis.call('EXPIRE', KEYS[2], ARGV[3])\n"
            + "return 1";

    @Resource
    private RedisTemplate<String, String> redisTemplate;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private DefaultRedisScript<Long> incrWithMetaScript;

    @PostConstruct
    public void init() {
        incrWithMetaScript = new DefaultRedisScript<>();
        incrWithMetaScript.setResultType(Long.class);
        incrWithMetaScript.setScriptText(INCR_WITH_META_SCRIPT);
    }

    public void incrWithMeta(String zsetKey, String metaUpdateKey, String goodsId, long delta, long ttlSeconds, long updateTimeMillis) {
        List<String> keys = Arrays.asList(zsetKey, metaUpdateKey);
        stringRedisTemplate.execute(incrWithMetaScript, keys, goodsId, String.valueOf(delta), String.valueOf(ttlSeconds), String.valueOf(updateTimeMillis));
    }
}