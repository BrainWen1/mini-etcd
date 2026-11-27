# 阶段一：编译
# 注意：官方 golang 镜像默认是基于 Debian 的，没有 apk。
# 这里显式用 alpine 变体，下面的 apk 命令才成立；同时产物是静态链接，
# 可以直接放进只有几 MB 的运行镜像。
FROM golang:1.26.3-alpine AS builder
WORKDIR /src

# 先只拷贝依赖清单，让 go mod download 这一层能命中构建缓存
COPY go.mod go.sum ./
RUN go mod download

# 生成好的 *.pb.go 已随仓库提交（见 Makefile 说明），
# 所以镜像构建里不需要再装 protoc/make，直接编译即可
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mini-etcd ./cmd/mini-etcd

# 阶段二：运行镜像
FROM alpine:3 AS runtime
WORKDIR /app

# 安装时区和证书，并创建非 root 运行用户
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 10001 mini-etcd \
    && adduser -S -D -H -u 10001 -G mini-etcd mini-etcd \
    && mkdir -p /app/data \
    && chown -R mini-etcd:mini-etcd /app

# 从编译阶段拷贝二进制文件
COPY --from=builder /out/mini-etcd /app/mini-etcd

USER mini-etcd

# 暴露 Raft 和 gRPC 端口
EXPOSE 8001 10001

# WAL / 快照 / bbolt 数据默认写在 /app/data 下，挂载出来才能持久化
VOLUME ["/app/data"]

# 启动命令（节点 ID 等参数由运行时传入，例如 -id 1）
ENTRYPOINT ["/app/mini-etcd"]
