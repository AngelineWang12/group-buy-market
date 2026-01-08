package cn.bugstack.infrastructure.adapter.repository.lua;

import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;

/**
 * 库存占用 Lua 脚本执行器
 * <p>
 * 解决高并发下库存扣减的安全性问题：
 * 1. 使用 Lua 脚本保证"检查库存+扣减库存+加锁"的原子性
 * 2. 如果 SETNX 失败，自动回滚库存，避免库存泄漏
 * 
 * @author Fuzhengwei bugstack.cn @小傅哥
 */
@Component
public class StockOccupyLuaExecutor {

    /**
     * Lua 脚本：原子化执行库存占用逻辑
     * 
     * 参数说明：
     * KEYS[1]: teamStockKey - 组队库存 key
     * ARGV[1]: target - 目标数量
     * ARGV[2]: recoveryCount - 恢复数量
     * ARGV[3]: validTimeMinutes - 锁过期时间（分钟）
     * ARGV[4]: lockKeyPrefix - 锁 key 前缀（teamStockKey + "_"）
     * 
     * 返回值：
     * 1 - 成功
     * 0 - 库存不足或 SETNX 失败（已自动回滚）
     * -1 - 库存 key 不存在
     */
    private static final String OCCUPY_STOCK_SCRIPT = ""
            + "local teamStockKey = KEYS[1]\n"
            + "local target = tonumber(ARGV[1])\n"
            + "local recoveryCount = tonumber(ARGV[2])\n"
            + "local validTimeMinutes = tonumber(ARGV[3])\n"
            + "local lockKeyPrefix = ARGV[4]\n"
            + "\n"
            + "-- 1. INCR 增加库存计数，得到占用后的值\n"
            + "-- 注意：从有组队量开始，相当于已经有了一个占用量，所以要 +1\n"
            + "local occupy = redis.call('INCR', teamStockKey) + 1\n"
            + "\n"
            + "-- 2. 检查是否超过目标数量（包含恢复量）\n"
            + "if occupy > target + recoveryCount then\n"
            + "    -- 超过限制，回滚库存\n"
            + "    redis.call('DECR', teamStockKey)\n"
            + "    return 0\n"
            + "end\n"
            + "\n"
            + "-- 3. 构建锁 key：lockKeyPrefix + occupy\n"
            + "local lockKey = lockKeyPrefix .. occupy\n"
            + "\n"
            + "-- 4. 尝试加锁（SETNX），设置过期时间\n"
            + "-- SET key value NX EX seconds 等价于 SETNX + EXPIRE 的原子操作\n"
            + "local lockResult = redis.call('SET', lockKey, '1', 'NX', 'EX', validTimeMinutes * 60)\n"
            + "\n"
            + "-- 5. 如果加锁失败，回滚库存并返回失败\n"
            + "if lockResult == false then\n"
            + "    redis.call('DECR', teamStockKey)\n"
            + "    return 0\n"
            + "end\n"
            + "\n"
            + "-- 6. 加锁成功，返回成功\n"
            + "return 1";

    @Resource
    private RedissonClient redissonClient;

    private RScript script;

    @PostConstruct
    public void init() {
        // 初始化 RScript，使用 StringCodec 确保编码正确
        script = redissonClient.getScript(StringCodec.INSTANCE);
    }

    /**
     * 原子化占用库存
     * 
     * @param teamStockKey 组队库存 key
     * @param lockKeyPrefix 锁 key 前缀（通常是 teamStockKey + "_"）
     * @param target 目标数量
     * @param recoveryCount 恢复数量
     * @param validTimeMinutes 锁过期时间（分钟）
     * @return true-成功，false-失败（库存不足或 SETNX 失败，已自动回滚）
     */
    public boolean occupyStock(String teamStockKey, String lockKeyPrefix, Integer target, Long recoveryCount, Integer validTimeMinutes) {
        // 准备参数
        // KEYS[1]: teamStockKey
        List<Object> keys = Arrays.asList(teamStockKey);
        
        // ARGV[1]: target
        // ARGV[2]: recoveryCount
        // ARGV[3]: validTimeMinutes
        // ARGV[4]: lockKeyPrefix
        List<Object> values = Arrays.asList(
                String.valueOf(target),
                String.valueOf(recoveryCount == null ? 0 : recoveryCount),
                String.valueOf(validTimeMinutes),
                lockKeyPrefix
        );

        // 执行 Lua 脚本
        // RScript.Mode.READ_WRITE: 脚本会读取和写入数据
        // RScript.ReturnType.INTEGER: 返回整数类型
        Long result = script.eval(
                RScript.Mode.READ_WRITE,
                OCCUPY_STOCK_SCRIPT,
                RScript.ReturnType.INTEGER,
                keys,
                values.toArray()
        );

        // 返回值：1 表示成功，0 或 -1 表示失败
        return result != null && result == 1;
    }
}
