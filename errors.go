package zookeeper_storage

import "fmt"

var (

	// ErrKeyPrefixEmpty 参数中 KeyPrefix 为空
	ErrKeyPrefixEmpty = fmt.Errorf("KeyPrefix can not empty")

	// ErrConnectionManagerNil 连接管理器没有指定
	ErrConnectionManagerNil = fmt.Errorf("ConnectionManager can not nil")
)
