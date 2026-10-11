package zookeeper_storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/storage-lock/go-storage"
)

// ZookeeperConnectionManager 管理 ZooKeeper 连接
type ZookeeperConnectionManager struct {
	// Hosts ZooKeeper 服务器地址列表
	Hosts []string

	// SessionTimeout 会话超时时间
	SessionTimeout time.Duration

	// initMu 保证连接初始化只执行一次且失败可重试
	initMu      sync.Mutex
	initialized bool
	client      *zk.Conn

	// 预设客户端（FromClient 构造时使用）
	presetClient *zk.Conn
}

var _ storage.ConnectionManager[*zk.Conn] = &ZookeeperConnectionManager{}

const defaultSessionTimeout = 5 * time.Second

// NewZookeeperConnectionManagerFromHosts 从服务器地址创建连接管理器
func NewZookeeperConnectionManagerFromHosts(hosts []string) *ZookeeperConnectionManager {
	return &ZookeeperConnectionManager{
		Hosts:          hosts,
		SessionTimeout: defaultSessionTimeout,
	}
}

// NewZookeeperConnectionManagerFromHostsWithTimeout 从服务器地址和超时创建连接管理器
func NewZookeeperConnectionManagerFromHostsWithTimeout(hosts []string, sessionTimeout time.Duration) *ZookeeperConnectionManager {
	return &ZookeeperConnectionManager{
		Hosts:          hosts,
		SessionTimeout: sessionTimeout,
	}
}

// NewZookeeperConnectionManagerFromClient 从已有客户端创建连接管理器
func NewZookeeperConnectionManagerFromClient(client *zk.Conn) *ZookeeperConnectionManager {
	return &ZookeeperConnectionManager{
		presetClient:   client,
		initialized:    true,
		client:         client,
		SessionTimeout: defaultSessionTimeout,
	}
}

func (x *ZookeeperConnectionManager) Name() string {
	return "zookeeper-connection-manager"
}

func (x *ZookeeperConnectionManager) Take(ctx context.Context) (*zk.Conn, error) {
	x.initMu.Lock()
	defer x.initMu.Unlock()

	if x.initialized {
		return x.client, nil
	}

	// 连接 ZooKeeper
	conn, _, err := zk.Connect(x.Hosts, x.SessionTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ZooKeeper: %w", err)
	}

	x.client = conn
	x.initialized = true
	return x.client, nil
}

func (x *ZookeeperConnectionManager) Return(ctx context.Context, connection *zk.Conn) error {
	// ZooKeeper 连接是长连接，无需归还
	return nil
}

func (x *ZookeeperConnectionManager) Shutdown(ctx context.Context) error {
	x.initMu.Lock()
	defer x.initMu.Unlock()

	if x.client != nil {
		x.client.Close()
		x.client = nil
		x.initialized = false
	}
	return nil
}
