package zookeeper_storage

import (
	"context"
	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/stretchr/testify/assert"
	"os"
	"os/exec"
	"testing"
	"time"
)

var zkCleanup func()

func TestMain(m *testing.M) {
	envName := "STORAGE_LOCK_ZOOKEEPER_HOSTS"
	hosts := os.Getenv(envName)
	if hosts != "" {
		zkCleanup = func() {}
		os.Exit(m.Run())
	}

	// 尝试使用 Docker 启动临时 ZooKeeper
	port := "12181"
	cmd := exec.Command("docker", "run", "--rm",
		"-p", port+":2181",
		"-e", "ZOOKEEPER_CLIENT_PORT=2181",
		"--name", "zookeeper-storage-test",
		"confluentinc/cp-zookeeper:7.4.0",
	)
	if err := cmd.Start(); err != nil {
		// 无法启动 ZooKeeper，跳过测试
		os.Exit(0)
	}

	zkCleanup = func() {
		cmd.Process.Kill()
		cmd.Wait()
		exec.Command("docker", "rm", "-f", "zookeeper-storage-test").Run()
	}

	// 等待 ZooKeeper 就绪
	time.Sleep(time.Second * 15)
	os.Setenv(envName, "localhost:"+port)
	exitCode := m.Run()

	zkCleanup()
	os.Exit(exitCode)
}

func TestZookeeperStorage(t *testing.T) {
	envName := "STORAGE_LOCK_ZOOKEEPER_HOSTS"
	hosts := os.Getenv(envName)
	if hosts == "" {
		t.Skip("STORAGE_LOCK_ZOOKEEPER_HOSTS not set and ZooKeeper not available")
	}

	connectionManager := NewZookeeperConnectionManagerFromHosts([]string{hosts})
	s, err := NewZookeeperStorage(context.Background(), &ZookeeperStorageOptions{
		ConnectionManager: connectionManager,
		KeyPrefix:         storage_test_helper.TestTableName,
	})
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)
}
