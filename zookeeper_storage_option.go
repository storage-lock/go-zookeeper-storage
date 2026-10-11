package zookeeper_storage

import (
	"github.com/go-zookeeper/zk"
	"github.com/storage-lock/go-storage"
)

// ZookeeperStorageOptions ZooKeeper 的存储选项
type ZookeeperStorageOptions struct {

	// 获取连接
	ConnectionManager storage.ConnectionManager[*zk.Conn]

	// KeyPrefix ZooKeeper 中路径的前缀，用于隔离不同的锁实例
	// 实际的路径格式为：/<KeyPrefix>/lock/<lockId>
	KeyPrefix string

	// TimeProvider 可选的时间源，默认使用 time.Now()
	// ZooKeeper 没有时间 API，默认使用本地时间
	// 在需要更可靠时间源的场景，可注入 NTP 时间源
	TimeProvider storage.TimeProvider
}

func NewZookeeperStorageOptions() *ZookeeperStorageOptions {
	return &ZookeeperStorageOptions{
		KeyPrefix: storage.DefaultStorageTableName,
	}
}

func NewZookeeperStorageOptionsWithHosts(hosts []string) *ZookeeperStorageOptions {
	return NewZookeeperStorageOptions().SetConnectionManager(NewZookeeperConnectionManagerFromHosts(hosts))
}

func (x *ZookeeperStorageOptions) SetKeyPrefix(keyPrefix string) *ZookeeperStorageOptions {
	x.KeyPrefix = keyPrefix
	return x
}

func (x *ZookeeperStorageOptions) SetConnectionManager(connectionManager storage.ConnectionManager[*zk.Conn]) *ZookeeperStorageOptions {
	x.ConnectionManager = connectionManager
	return x
}

func (x *ZookeeperStorageOptions) SetTimeProvider(timeProvider storage.TimeProvider) *ZookeeperStorageOptions {
	x.TimeProvider = timeProvider
	return x
}

func (x *ZookeeperStorageOptions) Check() error {
	if x.KeyPrefix == "" {
		return ErrKeyPrefixEmpty
	}
	if x.ConnectionManager == nil {
		return ErrConnectionManagerNil
	}
	return nil
}
