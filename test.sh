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

# 依赖检查：grpcurl 访问 gRPC 接口，jq 解析 protojson 返回（int64 会编码成字符串）
for cmd in go grpcurl jq; do
    if ! command -v "$cmd" > /dev/null 2>&1; then
        echo -e "${RED}❌ 缺少依赖: $cmd${NC}"
        exit 1
    fi
done

# 获取 Leader 的 gRPC 端口。
#
# 所有写请求都会由 Follower 自动转发给 Leader，因此"能写成功"并不能区分谁是
# Leader。这里利用服务端的防转发环路标记：带 x-mini-etcd-forwarded 的写请求
# 只会在 Leader 上执行，非 Leader 节点识别出"已经转发过"会直接返回
# Unavailable，于是只有真正的 Leader 返回成功。
get_leader_port() {
    for port in 10001 10002 10003 10004 10005; do
        if grpcurl -plaintext -max-time 2 \
            -H 'x-mini-etcd-forwarded: 1' \
            -d '{"key":"probe","value":"1"}' \
            "127.0.0.1:$port" minietcd.v1.KV/Put > /dev/null 2>&1; then
            echo "$port"
            return
        fi
    done
    echo ""
}

echo -e "${YELLOW}=== 1. 环境清理 & 编译 ===${NC}"
# 杀掉可能残留的进程
pkill -f "$BIN_NAME" || true
rm -rf $DATA_DIR
rm -rf $LOG_DIR
mkdir -p $LOG_DIR

# 编译二进制文件，避免 go run 产生的僵尸进程
go build -o $BIN_NAME ./cmd/mini-etcd
echo -e "${GREEN}编译完成: ./$BIN_NAME${NC}"

echo -e "${YELLOW}=== 2. 启动初始集群 (1, 2, 3) ===${NC}"
./$BIN_NAME -id 1 > $LOG_DIR/node1.log 2>&1 &
./$BIN_NAME -id 2 > $LOG_DIR/node2.log 2>&1 &
./$BIN_NAME -id 3 > $LOG_DIR/node3.log 2>&1 &

echo "等待 5 秒让集群选出 Leader..."
sleep 5

LEADER_PORT=$(get_leader_port)
if [ -z "$LEADER_PORT" ]; then
    echo -e "${RED}❌ 集群未能选出 Leader，测试终止！请检查 $LOG_DIR/node1.log${NC}"
    pkill -f "$BIN_NAME" || true
    exit 1
fi
echo -e "${GREEN}✅ 集群启动成功，当前 Leader gRPC 端口: $LEADER_PORT${NC}"

echo -e "${YELLOW}=== 3. 验证基础读写 ===${NC}"
grpcurl -plaintext \
    -d '{"key":"init_key","value":"init_val"}' \
    "127.0.0.1:$LEADER_PORT" minietcd.v1.KV/Put > /dev/null
res=$(grpcurl -plaintext \
    -d '{"key":"init_key"}' \
    "127.0.0.1:$LEADER_PORT" minietcd.v1.KV/Get | jq -r '.kv.value')
if [ "$res" = "init_val" ]; then
    echo -e "${GREEN}✅ 基础读写正常${NC}"
else
    echo -e "${RED}❌ 基础读写失败${NC}"
    exit 1
fi

echo -e "${YELLOW}=== 4. 场景一：节点掉线期间新增节点，然后恢复 ===${NC}"
# 4.1 启动节点 4
echo "启动节点 4..."
./$BIN_NAME -id 4 -join -raft-addr "127.0.0.1:8004" -grpc-addr "127.0.0.1:10004" \
-peers "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003,4=127.0.0.1:8004" \
-grpc-peers "1=127.0.0.1:10001,2=127.0.0.1:10002,3=127.0.0.1:10003,4=127.0.0.1:10004" \
> $LOG_DIR/node4.log 2>&1 &
NODE4_PID=$!
sleep 1

# 4.2 将节点 4 加入集群
echo "向 Leader 发送添加节点 4 的请求..."
grpcurl -plaintext \
    -d '{"id":4, "raft_addr":"127.0.0.1:8004", "grpc_addr":"127.0.0.1:10004"}' \
    "127.0.0.1:$LEADER_PORT" minietcd.v1.Cluster/MemberAdd > /dev/null
sleep 2

# 4.3 写 200 条数据逼迫日志压缩（假设阈值为 10）
echo "向集群写入 200 条数据触发压缩..."
for i in {1..200}; do
    grpcurl -plaintext \
        -d "{\"key\":\"batch_key\", \"value\":\"$i\"}" \
        "127.0.0.1:$LEADER_PORT" minietcd.v1.KV/Put > /dev/null
done
sleep 2

# 4.4 杀掉节点 4（模拟掉线）
echo -e "${YELLOW}模拟节点 4 掉线 (kill $NODE4_PID)${NC}"
kill $NODE4_PID || true
sleep 2

# 4.5 启动并加入节点 5
echo "启动并添加节点 5..."
./$BIN_NAME -id 5 -join -raft-addr "127.0.0.1:8005" -grpc-addr "127.0.0.1:10005" \
-peers "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003,5=127.0.0.1:8005" \
-grpc-peers "1=127.0.0.1:10001,2=127.0.0.1:10002,3=127.0.0.1:10003,5=127.0.0.1:10005" \
> $LOG_DIR/node5.log 2>&1 &
NODE5_PID=$!
sleep 1
grpcurl -plaintext \
    -d '{"id":5, "raft_addr":"127.0.0.1:8005", "grpc_addr":"127.0.0.1:10005"}' \
    "127.0.0.1:$LEADER_PORT" minietcd.v1.Cluster/MemberAdd > /dev/null
sleep 2

# 写一条只有节点 5 参与共识的数据
echo "写入 post_node4_crash_key..."
grpcurl -plaintext \
    -d '{"key":"post_node4_crash", "value":"success"}' \
    "127.0.0.1:$LEADER_PORT" minietcd.v1.KV/Put > /dev/null
sleep 1

# 4.6 重启节点 4
echo "重启节点 4 (不传任何 peers 参数)..."
./$BIN_NAME -id 4 -join > $LOG_DIR/node4_restart.log 2>&1 &
NODE4_RESTART_PID=$!
sleep 4

# 4.7 验证节点 4 是否追平了数据
# Get 走 ReadIndex + 本地状态机，Follower 也会等待追平到 Leader 的已提交位置。
echo "验证节点 4 是否追平数据..."
res=$(grpcurl -plaintext \
    -d '{"key":"post_node4_crash"}' \
    127.0.0.1:10004 minietcd.v1.KV/Get | jq -r '.kv.value')
if [ "$res" = "success" ]; then
    echo -e "${GREEN}✅ 节点 4 成功恢复并追平了节点 5 参与写入的数据！${NC}"
else
    echo -e "${RED}❌ 节点 4 数据追赶失败，可能 Member 信息不全或快照被拒${NC}"
    echo "请检查节点 4 日志: $LOG_DIR/node4_restart.log"
    pkill -f "$BIN_NAME" || true
    exit 1
fi

echo -e "${YELLOW}=== 5. 动态缩容：移除节点 5 ===${NC}"
echo "移除节点 5..."
grpcurl -plaintext \
    -d '{"id":5}' \
    "127.0.0.1:$LEADER_PORT" minietcd.v1.Cluster/MemberRemove > /dev/null
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
