# mini-etcd 常用开发命令。
#
# 生成 gRPC 代码依赖以下工具（版本需与 go.mod 中的 protobuf/grpc 大致匹配）：
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2

PROTOC      ?= protoc
PROTO_DIR   := api/proto
PROTO_FILES := $(PROTO_DIR)/etcd.proto $(PROTO_DIR)/kv.proto
MODULE      := mini-etcd

.PHONY: proto build test clean

# 编译 .proto 文件生成 gRPC 代码
# 修改 .proto 后必须重新生成，生成物与 .proto 一起提交。
proto:
	$(PROTOC) -I $(PROTO_DIR) \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		$(PROTO_FILES)

# 编译项目代码
build:
	go build ./...

# 运行单元测试
test:
	go test ./...

# 清理生成的 gRPC 代码
clean:
	rm -f $(PROTO_DIR)/*.pb.go
