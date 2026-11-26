package types

type Result struct {
	Err       error // 记录执行结果的错误信息
	Succeeded bool  // 针对 TXN，记录是否走了 Success 分支
}
