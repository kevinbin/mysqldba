package cmd

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

// SSH免密登录工具
type SSHClient struct {
	Addr    string
	User    string
	KeyPath string
	Client  *ssh.Client
}

func NewSSHClient(addr, user, keyPath string) (*SSHClient, error) {
	key, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read private key: %w", err)
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         8 * time.Second,
	}
	client, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, fmt.Errorf("ssh dial: %w", err)
	}
	return &SSHClient{
		Addr:    addr,
		User:    user,
		KeyPath: keyPath,
		Client:  client,
	}, nil
}

func (c *SSHClient) Run(cmd string) (string, error) {
	session, err := c.Client.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()
	output, err := session.CombinedOutput(cmd)
	return string(output), err
}

func (c *SSHClient) RunWithStream(cmd string, outputWriter io.Writer) error {
	session, err := c.Client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	session.Stdout = outputWriter
	session.Stderr = outputWriter

	// 启动命令并实时输出
	err = session.Run(cmd)
	return err
}

func (c *SSHClient) CopyFile(localPath, remotePath string) error {
	// 获取本地文件信息
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("获取本地文件信息失败: %w", err)
	}

	if fileInfo.IsDir() {
		return fmt.Errorf("%s 是一个目录，请使用 CopyDir 函数", localPath)
	}

	// 构建 scp 命令
	scpCmd := exec.Command(
		"scp",
		"-i", c.KeyPath,
		"-o", "StrictHostKeyChecking=no",
		localPath,
		fmt.Sprintf("%s@%s:%s", c.User, strings.Split(c.Addr, ":")[0], remotePath),
	)

	// 执行命令
	output, err := scpCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("文件传输失败 %s -> %s: %w\n%s", localPath, remotePath, err, string(output))
	}

	return nil
}

// CopyDir 复制整个目录到远程服务器
func (c *SSHClient) CopyDir(localDir, remoteDir string) error {
	// 检查本地目录是否存在
	localStat, err := os.Stat(localDir)
	if err != nil {
		return fmt.Errorf("无法访问本地目录 %s: %w", localDir, err)
	}

	if !localStat.IsDir() {
		return fmt.Errorf("%s 不是一个目录", localDir)
	}

	// 构建 scp 命令 (使用 -r 参数递归复制目录)
	scpCmd := exec.Command(
		"scp",
		"-r",
		"-i", c.KeyPath,
		"-o", "StrictHostKeyChecking=no",
		localDir,
		fmt.Sprintf("%s@%s:%s", c.User, strings.Split(c.Addr, ":")[0], remoteDir),
	)

	// 执行命令
	output, err := scpCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("目录传输失败 %s -> %s: %w\n%s", localDir, remoteDir, err, string(output))
	}

	return nil
}

// Close 关闭SSH客户端连接
func (c *SSHClient) Close() error {
	if c.Client != nil {
		return c.Client.Close()
	}
	return nil
}
