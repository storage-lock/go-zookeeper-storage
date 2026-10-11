package zookeeper_storage

import (
	"context"
	"fmt"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/go-zookeeper/zk"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"strconv"
	"strings"
	"time"
)

// ------------------------------------------------ ---------------------------------------------------------------------

// ZookeeperStorage 基于 ZooKeeper 的存储引擎实现
//
// ZooKeeper 通过 Create（节点不存在才创建）、Set（version 匹配才更新）
// 和 Delete（version 匹配才删除）提供原生 CAS 支持。
// 这是与 Redis Lua、etcd Txn、Memcache Add+CAS 不同的第四种 CAS 范式：
// 每个写操作都原生支持条件判断，无需组合多个原语。
//
// 存储模型：每个锁对应一个 ZooKeeper 持久节点（PERSISTENT），
// 节点路径 = /<KeyPrefix>/lock/<lockId>，
// 节点数据 = "<version>\x00<json>"（与 memcache 相同的编码格式）。
// ZooKeeper 节点的 dataVersion 即为 CAS version。
type ZookeeperStorage struct {
	options *ZookeeperStorageOptions
	client  *zk.Conn

	timeProvider storage.TimeProvider
}

var _ storage.Storage = &ZookeeperStorage{}

// NewZookeeperStorage 创建一个基于 ZooKeeper 的存储引擎
func NewZookeeperStorage(ctx context.Context, options *ZookeeperStorageOptions) (*ZookeeperStorage, error) {

	if err := options.Check(); err != nil {
		return nil, err
	}

	s := &ZookeeperStorage{
		options: options,
	}

	err := s.Init(ctx)
	if err != nil {
		return nil, err
	}

	return s, nil
}

const ZookeeperStorageName = "zookeeper-storage"

func (x *ZookeeperStorage) GetName() string {
	return ZookeeperStorageName
}

// Capabilities 声明 ZooKeeper 存储支持的能力
// ZooKeeper 原生支持条件创建（Create 返回 ErrNodeExists）、
// 条件更新（Set with version）、条件删除（Delete with version），
// 因此同时声明 CAS、AtomicDelete 能力。
// 时间源默认使用本地时间（ZooKeeper 无时间 API）。
func (x *ZookeeperStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
		storage.CapabilityAtomicDelete,
		storage.CapabilityWatch,
	}
}

func (x *ZookeeperStorage) Watch(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	return storage.NewPollingLockWatcher(x, 500*time.Millisecond).Watch(ctx, lockId)
}

func (x *ZookeeperStorage) Init(ctx context.Context) error {
	client, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	x.client = client

	// 确保根路径存在
	rootPath := x.rootPath()
	exists, _, err := client.Exists(rootPath)
	if err != nil {
		return fmt.Errorf("failed to check root path: %w", err)
	}
	if !exists {
		// 创建根路径（忽略已存在的错误，可能并发创建）
		_, err = client.Create(rootPath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return fmt.Errorf("failed to create root path: %w", err)
		}
	}

	// 设置时间源
	if x.options.TimeProvider != nil {
		x.timeProvider = x.options.TimeProvider
	} else {
		x.timeProvider = &localTimeProvider{}
	}

	return nil
}

// localTimeProvider 本地时间源
type localTimeProvider struct{}

func (p *localTimeProvider) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}

// rootPath 返回 ZooKeeper 中的根路径
func (x *ZookeeperStorage) rootPath() string {
	return "/" + x.options.KeyPrefix
}

// lockPath 返回 ZooKeeper 中存储锁信息的路径
func (x *ZookeeperStorage) lockPath(lockId string) string {
	return fmt.Sprintf("%s/lock/%s", x.rootPath(), lockId)
}

// encodeNodeData 将 version 和 json 编码为 ZooKeeper 节点数据
func encodeNodeData(version storage.Version, jsonStr string) []byte {
	return []byte(strconv.FormatUint(uint64(version), 10) + valueSeparator + jsonStr)
}

// decodeNodeData 从 ZooKeeper 节点数据解码 version 和 json
func decodeNodeData(data []byte) (storage.Version, string, error) {
	parts := strings.SplitN(string(data), valueSeparator, 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("invalid zookeeper node data format")
	}
	version, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, "", err
	}
	return storage.Version(version), parts[1], nil
}

// 存储在节点数据中的分隔符
const valueSeparator = "\x00"

// CreateWithVersion 原子创建锁记录
// ZooKeeper Create 命令：节点不存在时才创建，否则返回 ErrNodeExists
func (x *ZookeeperStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	path := x.lockPath(lockId)
	data := encodeNodeData(version, lockInformation.ToJsonString())

	// 确保父路径存在
	parentPath := x.rootPath() + "/lock"
	exists, _, err := x.client.Exists(parentPath)
	if err != nil {
		return fmt.Errorf("failed to check parent path: %w", err)
	}
	if !exists {
		_, err = x.client.Create(parentPath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return fmt.Errorf("failed to create parent path: %w", err)
		}
	}

	_, err = x.client.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		if err == zk.ErrNodeExists {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

// UpdateWithVersion 原子更新锁记录
// ZooKeeper Set 命令：version 匹配才更新，否则返回 ErrBadVersion
func (x *ZookeeperStorage) UpdateWithVersion(ctx context.Context, lockId string, exceptedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	path := x.lockPath(lockId)
	data := encodeNodeData(newVersion, lockInformation.ToJsonString())

	// ZooKeeper Set 的 version 参数是 dataVersion（从 0 开始的递增版本号）
	// 我们的业务版本号与 ZooKeeper 的 dataVersion 不同
	// 需要先读取当前数据验证业务版本号
	// 注意：节点不存在时映射为 ErrVersionMiss 而不是 ErrLockNotFound——core 的
	// lockExpired/rlockReclaim 等抢占路径只把 ErrVersionMiss 当作"CAS 失败，可重试"，
	// 其它错误会被当作硬错误直接返回。若真正的持有者恰好在这中间 DeleteWithVersion
	// 真删了节点（与本次抢占竞态），"节点不存在"和"版本不匹配"对调用方而言是同一件事：
	// 期望的前提已经不成立，应该重试而不是硬失败。
	currentData, stat, err := x.client.Get(path)
	if err != nil {
		if err == zk.ErrNoNode {
			return storage_lock.ErrVersionMiss
		}
		return err
	}

	// 解析当前业务版本号
	currentVersion, _, err := decodeNodeData(currentData)
	if err != nil {
		return err
	}

	// 检查业务版本是否匹配
	if currentVersion != exceptedVersion {
		return storage_lock.ErrVersionMiss
	}

	// 使用 ZooKeeper dataVersion 作为 CAS 条件写入新值
	_, err = x.client.Set(path, data, stat.Version)
	if err != nil {
		if err == zk.ErrBadVersion {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

// DeleteWithVersion 原子删除锁记录
// ZooKeeper Delete 命令：version 匹配才删除，否则返回 ErrBadVersion
// 这是真正的原子条件删除，不需要墓碑降级
func (x *ZookeeperStorage) DeleteWithVersion(ctx context.Context, lockId string, exceptedVersion storage.Version, lockInformation *storage.LockInformation) error {
	path := x.lockPath(lockId)

	// 先读取当前数据验证业务版本号
	currentData, stat, err := x.client.Get(path)
	if err != nil {
		if err == zk.ErrNoNode {
			return storage_lock.ErrVersionMiss
		}
		return err
	}

	// 解析当前业务版本号
	currentVersion, _, err := decodeNodeData(currentData)
	if err != nil {
		return err
	}

	// 检查业务版本是否匹配
	if currentVersion != exceptedVersion {
		return storage_lock.ErrVersionMiss
	}

	// 使用 ZooKeeper dataVersion 作为 CAS 条件删除节点
	err = x.client.Delete(path, stat.Version)
	if err != nil {
		if err == zk.ErrBadVersion || err == zk.ErrNoNode {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

// Get 获取锁信息
func (x *ZookeeperStorage) Get(ctx context.Context, lockId string) (string, error) {
	path := x.lockPath(lockId)
	data, _, err := x.client.Get(path)
	if err != nil {
		if err == zk.ErrNoNode {
			return "", storage_lock.ErrLockNotFound
		}
		return "", err
	}

	_, jsonStr, err := decodeNodeData(data)
	if err != nil {
		return "", err
	}
	return jsonStr, nil
}

// GetTime 获取时间
// ZooKeeper 没有时间 API，默认使用本地时间
func (x *ZookeeperStorage) GetTime(ctx context.Context) (time.Time, error) {
	return x.timeProvider.GetTime(ctx)
}

// Close 关闭存储引擎
// 连接由 ConnectionManager 管理，Storage 只清空引用
func (x *ZookeeperStorage) Close(ctx context.Context) error {
	x.client = nil
	return nil
}

// List 列出当前所有的锁信息
// ZooKeeper 支持通过 GetChildren 枚举子节点
func (x *ZookeeperStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	parentPath := x.rootPath() + "/lock"

	children, _, err := x.client.Children(parentPath)
	if err != nil {
		if err == zk.ErrNoNode {
			return go_iterator.FromSlice([]*storage.LockInformation{}), nil
		}
		return nil, err
	}

	slice := make([]*storage.LockInformation, 0, len(children))
	for _, lockId := range children {
		path := x.lockPath(lockId)
		data, _, err := x.client.Get(path)
		if err != nil {
			if err == zk.ErrNoNode {
				continue
			}
			return nil, err
		}
		_, jsonStr, err := decodeNodeData(data)
		if err != nil {
			continue
		}
		lockInfo, err := storage.LockInformationFromJsonString(jsonStr)
		if err != nil {
			continue
		}
		slice = append(slice, lockInfo)
	}
	return go_iterator.FromSlice(slice), nil
}

// ------------------------------------------------ ---------------------------------------------------------------------
