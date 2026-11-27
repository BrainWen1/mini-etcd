#!/bin/bash

LEADER_PORT=10001
LOCK_KEY="my_distributed_lock"
URL="127.0.0.1:$LEADER_PORT"
CLIENTS=5
WATCH_TIMEOUT=10

# 依赖检查：grpcurl 访问 gRPC 接口，jq 构造请求 / 解析响应
# （int64 字段在 protojson 里是字符串，例如 leaseId，不能直接当数字塞回请求）
for cmd in grpcurl jq; do
    if ! command -v "$cmd" > /dev/null 2>&1; then
        echo "❌ 缺少依赖: $cmd"
        exit 1
    fi
done

# 预检：用反射列一下服务，确认节点在监听（Get 不存在的 key 会返回 NotFound，
# 不能只看 grpcurl 的退出码判断连通性）。
if ! grpcurl -plaintext -max-time 3 \
    "$URL" list minietcd.v1.KV > /dev/null 2>&1; then
    echo "❌ 无法连接 $URL，请先启动 mini-etcd 集群"
    exit 1
fi

# 等待锁释放事件。
#
# grpcurl 的 Watch 是长连接流：即使 grep -m 1 已经匹配到事件并退出，grpcurl
# 仍会阻塞在下一次 Recv 上，不会因为管道关闭而立刻退出（这点和 curl 不同）。
# 所以这里把 grpcurl 放到后台写临时文件，轮询到 DELETE 事件后显式 kill。
wait_for_release() {
    local out="$WATCH_DIR/watch_$1.out"
    grpcurl -plaintext -d "{\"key\":\"$LOCK_KEY\"}" \
        "$URL" minietcd.v1.Watch/Watch > "$out" 2>&1 &
    local pid=$!
    local deadline=$((SECONDS + WATCH_TIMEOUT))
    while [ $SECONDS -lt $deadline ]; do
        if grep -q 'EVENT_TYPE_DELETE' "$out" 2>/dev/null; then
            kill "$pid" 2>/dev/null
            wait "$pid" 2>/dev/null
            return 0
        fi
        sleep 0.2
    done
    kill "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    return 1
}

acquire_lock() {
    CLIENT_ID=$1

    while true; do
        # 1. 申请临时租约
        LEASE_ID=$(grpcurl -plaintext \
            -d '{"ttl": 5}' \
            "$URL" minietcd.v1.Lease/LeaseGrant 2>/dev/null | jq -r '.leaseId // empty')
        if [ -z "$LEASE_ID" ]; then
            echo "⚠️  [客户端 $CLIENT_ID] 申请租约失败（可能暂无 Leader），重试..."
            sleep 1
            continue
        fi

        # 2. 尝试抢锁：比较 key 是否为空，是则写入并绑定租约。
        # leaseId 是 int64，protojson 用字符串表示，这里保持字符串形式，
        # 否则 19 位雪花 ID 经过 jq 的数字解析会丢精度。
        TXN_BODY=$(jq -n \
            --arg key "$LOCK_KEY" \
            --arg value "client_$CLIENT_ID" \
            --arg lease "$LEASE_ID" \
            '{
                compares: [{key: $key, op: "COMPARE_OP_EQUAL", value: ""}],
                success: [{op: "OPERATION_TYPE_PUT", key: $key, value: $value, leaseId: $lease}]
            }')
        RESULT=$(grpcurl -plaintext -d "$TXN_BODY" "$URL" minietcd.v1.KV/Txn 2>/dev/null)

        # 3. 判断结果：RPC 成功不代表抢到锁，必须检查 succeeded 字段
        if echo "$RESULT" | jq -e '.succeeded == true' > /dev/null 2>&1; then
            echo "✅ [客户端 $CLIENT_ID] 抢到了锁！"
            sleep 2 # 模拟业务处理

            echo "🔓 [客户端 $CLIENT_ID] 释放锁..."
            grpcurl -plaintext -d "{\"key\":\"$LOCK_KEY\"}" \
                "$URL" minietcd.v1.KV/Delete > /dev/null
            # 加上撤销租约，防止孤立的租约过期
            grpcurl -plaintext -d "{\"leaseId\":\"$LEASE_ID\"}" \
                "$URL" minietcd.v1.Lease/LeaseRevoke > /dev/null
            break # 业务做完了，退出循环
        else
            echo "❌ [客户端 $CLIENT_ID] 没抢到锁，开启 Watch 监听等待..."

            # 监听锁释放（DELETE 事件），超时后重新抢锁。
            # Watch 读的是本地状态机，任意节点都能收到该事件。
            if wait_for_release "$CLIENT_ID"; then
                echo "🔔 [客户端 $CLIENT_ID] 收到锁释放事件，重新尝试抢锁..."
            else
                echo "⏱️  [客户端 $CLIENT_ID] 等待超时，重新尝试抢锁..."
            fi
            # 继续循环重试
        fi
    done
}

WATCH_DIR=$(mktemp -d)
trap 'rm -rf "$WATCH_DIR"' EXIT

echo "🚀 开始分布式锁压测，并发客户端数量: $CLIENTS"

for i in $(seq 1 $CLIENTS); do
    acquire_lock $i &
done

wait
echo "🏁 所有客户端测试结束"
