#!/bin/bash

# 确保任何错误都能被捕获
set -e

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

LOG_DIR="test_logs"
DATA_DIR="data"
BIN_NAME="mini-etcd-test"

echo -e "${YELLOW}=== 1. 环境清理 & 编译 ===${NC}"
# 杀掉可能残留的进程
pkill -f "$BIN_NAME" || true
rm -rf $DATA_DIR
rm -rf $LOG_DIR
mkdir -p $LOG_DIR

# 编译二进制文件，避免 go run 产生的僵尸进程
go build -o $BIN_NAME ./cmd/mini-etcd
echo -e "${GREEN}编译完成: ./$BIN_NAME${NC}"

# 获取 Leader 的 HTTP 端口函数
get_leader_port() {
    for port in 10001 10002 10003 10004 10005; do
        # 向各个节点发起请求，如果返回 200 说明它是 Leader
        res=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://127.0.0.1:$port/v1/kv -H "Content-Type: application/json" -d '{"key":"probe","value":"1"}' || true)
        if [ "$res" = "200" ]; then
            echo $port
            return
        fi
    done
    echo ""
}

echo -e "${YELLOW}=== 2. 启动初始集群 (1, 2, 3) ===${NC}"
./$BIN_NAME -id 1 > $LOG_DIR/node1.log 2>&1 &
./$BIN_NAME -id 2 > $LOG_DIR/node2.log 2>&1 &
./$BIN_NAME -id 3 > $LOG_DIR/node3.log 2>&1 &

echo "等待 5 秒让集群选出 Leader..."
sleep 5

LEADER_PORT=$(get_leader_port)
if [ -z "$LEADER_PORT" ]; then
    echo -e "${RED}❌ 集群未能选出 Leader，测试终止！请检查 $LOG_DIR/node1.log${NC}"
    pkill -f "$BIN_NAME"
    exit 1
fi
echo -e "${GREEN}✅ 集群启动成功，当前 Leader HTTP 端口: $LEADER_PORT${NC}"

echo -e "${YELLOW}=== 3. 验证基础读写 ===${NC}"
curl -s -X POST http://127.0.0.1:$LEADER_PORT/v1/kv -H "Content-Type: application/json" -d '{"key":"init_key", "value":"init_val"}' > /dev/null
res=$(curl -s http://127.0.0.1:$LEADER_PORT/v1/kv/init_key | grep -o '"value":"init_val"')
if [ -n "$res" ]; then
    echo -e "${GREEN}✅ 基础读写正常${NC}"
else
    echo -e "${RED}❌ 基础读写失败${NC}"
    exit 1
fi

echo -e "${YELLOW}=== 4. 场景一：节点掉线期间新增节点，然后恢复 ===${NC}"
# 4.1 启动节点 4
echo "启动节点 4..."
./$BIN_NAME -id 4 -join -raft-addr "127.0.0.1:8004" -http-addr "127.0.0.1:10004" \
-peers "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003,4=127.0.0.1:8004" \
-http-peers "1=127.0.0.1:10001,2=127.0.0.1:10002,3=127.0.0.1:10003,4=127.0.0.1:10004" \
> $LOG_DIR/node4.log 2>&1 &
NODE4_PID=$!
sleep 1

# 4.2 将节点 4 加入集群
echo "向 Leader 发送添加节点 4 的请求..."
curl -s -X POST http://127.0.0.1:$LEADER_PORT/v1/cluster/members/add -H "Content-Type: application/json" \
-d '{"id":4, "raft_addr":"127.0.0.1:8004", "http_addr":"127.0.0.1:10004"}' > /dev/null
sleep 2

# 4.3 写 200 条数据逼迫日志压缩（假设阈值为 10）
echo "向集群写入 200 条数据触发压缩..."
for i in {1..200}; do
    curl -s -X POST http://127.0.0.1:$LEADER_PORT/v1/kv -H "Content-Type: application/json" -d "{\"key\":\"batch_key\", \"value\":\"$i\"}" > /dev/null
done
sleep 2

# 4.4 杀掉节点 4（模拟掉线）
echo -e "${YELLOW}模拟节点 4 掉线 (kill $NODE4_PID)${NC}"
kill $NODE4_PID || true
sleep 2

# 4.5 启动并加入节点 5
echo "启动并添加节点 5..."
./$BIN_NAME -id 5 -join -raft-addr "127.0.0.1:8005" -http-addr "127.0.0.1:10005" \
-peers "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003,5=127.0.0.1:8005" \
-http-peers "1=127.0.0.1:10001,2=127.0.0.1:10002,3=127.0.0.1:10003,5=127.0.0.1:10005" \
> $LOG_DIR/node5.log 2>&1 &
NODE5_PID=$!
sleep 1
curl -s -X POST http://127.0.0.1:$LEADER_PORT/v1/cluster/members/add -H "Content-Type: application/json" \
-d '{"id":5, "raft_addr":"127.0.0.1:8005", "http_addr":"127.0.0.1:10005"}' > /dev/null
sleep 2

# 写一条只有节点 5 参与共识的数据
echo "写入 post_node4_crash_key..."
curl -s -X POST http://127.0.0.1:$LEADER_PORT/v1/kv -H "Content-Type: application/json" -d '{"key":"post_node4_crash", "value":"success"}' > /dev/null
sleep 1

# 4.6 重启节点 4
echo "重启节点 4 (不传任何 peers 参数)..."
./$BIN_NAME -id 4 -join > $LOG_DIR/node4_restart.log 2>&1 &
NODE4_RESTART_PID=$!
sleep 4

# 4.7 验证节点 4 是否追平了数据
echo "验证节点 4 是否追平数据..."
res=$(curl -s http://127.0.0.1:10004/v1/kv/post_node4_crash | grep -o '"value":"success"')
if [ -n "$res" ]; then
    echo -e "${GREEN}✅ 节点 4 成功恢复并追平了节点 5 参与写入的数据！${NC}"
else
    echo -e "${RED}❌ 节点 4 数据追赶失败，可能 Member 信息不全或快照被拒${NC}"
    echo "请检查节点 4 日志: $LOG_DIR/node4_restart.log"
    pkill -f "$BIN_NAME"
    exit 1
fi

echo -e "${YELLOW}=== 5. 动态缩容：移除节点 5 ===${NC}"
echo "移除节点 5..."
curl -s -X POST http://127.0.0.1:$LEADER_PORT/v1/cluster/members/remove -H "Content-Type: application/json" -d '{"id":5}' > /dev/null
sleep 2

if ps -p $NODE5_PID > /dev/null; then
    echo -e "${RED}❌ 节点 5 未按预期退出${NC}"
else
    echo -e "${GREEN}✅ 节点 5 成功收到移除日志并优雅退出${NC}"
fi

echo -e "${YELLOW}=== 6. 清理环境 ===${NC}"
pkill -f "$BIN_NAME" || true
rm -f $BIN_NAME
echo -e "${GREEN}🎉 所有端到端测试通过，动态扩缩容能力测试通过${NC}"
