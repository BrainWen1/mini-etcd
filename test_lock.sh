#!/bin/bash

LEADER_PORT=10001
LOCK_KEY="my_distributed_lock"
URL="http://127.0.0.1:$LEADER_PORT"
CLIENTS=5

echo "🚀 开始分布式锁压测，并发客户端数量: $CLIENTS"

acquire_lock() {
    CLIENT_ID=$1
    
    while true; do
        # 1. 申请临时租约
        LEASE_RESP=$(curl -s -X POST "$URL/v1/lease/grant" -H "Content-Type: application/json" -d '{"ttl": 5}')
        LEASE_ID=$(echo $LEASE_RESP | grep -o '"lease_id":[0-9]*' | cut -d':' -f2)
        
        # 2. 尝试抢锁
        TXN_BODY="{\"compares\":[{\"key\":\"$LOCK_KEY\",\"op\":\"==\",\"value\":\"\"}],\"success\":[{\"op\":\"PUT\",\"key\":\"$LOCK_KEY\",\"value\":\"client_$CLIENT_ID\",\"lease_id\":$LEASE_ID}]}"
        RESULT=$(curl -s -X POST "$URL/v1/kv/txn" -H "Content-Type: application/json" -d "$TXN_BODY")
        
        # 3. 判断结果
        if echo "$RESULT" | grep -q '"succeeded":true'; then
            echo "✅ [客户端 $CLIENT_ID] 抢到了锁！"
            sleep 2 # 模拟业务处理
            
            echo "🔓 [客户端 $CLIENT_ID] 释放锁..."
            curl -s -X DELETE "$URL/v1/kv/$LOCK_KEY" > /dev/null
            # 加上撤销租约，防止孤立的租约过期
            curl -s -X POST "$URL/v1/lease/revoke" -d "{\"lease_id\":$LEASE_ID}" > /dev/null
            break # 业务做完了，退出循环
        else
            echo "❌ [客户端 $CLIENT_ID] 没抢到锁，开启 Watch 监听等待..."
            
            # 使用 grep -m 1 等待具体的 DELETE 事件。
            # 当匹配到 DELETE 后，grep 退出 -> curl 收到 SIGPIPE 退出。不会留下僵尸进程。
            # --line-buffered 保证输出及时，不会被缓冲。
            timeout 10s curl -s -N "$URL/v1/kv/watch?key=$LOCK_KEY" | grep --line-buffered -m 1 '"Type":"DELETE"'
            
            echo "🔔 [客户端 $CLIENT_ID] 收到锁释放事件，重新尝试抢锁..."
            # 继续循环重试
        fi
    done
}

for i in $(seq 1 $CLIENTS); do
    acquire_lock $i &
done

wait
echo "🏁 所有客户端测试结束"
