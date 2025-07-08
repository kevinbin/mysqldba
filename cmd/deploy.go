// Copyright © 2017 Hong Bin <hongbin119@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"bytes"
	"text/template"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
)

// Constants for configuration
const (
	// Default values
	DefaultMySQLVersion = "8.0.35"
	DefaultInstallDir   = "/usr/local"
	DefaultDataDir      = "/data/mysql"
	DefaultMySQLPort    = 3306
	DefaultSSHPort      = 22
	DefaultSSHUser      = "root"
	DefaultMGRPort      = 33061
	DownloadUrl         = "https://dev.mysql.com/get/Downloads/MySQL-8.0"

	// Timeouts
	SSHTimeout        = 5
	MySQLStartTimeout = 60
	ServiceTimeout    = 30

	// Architecture types
	ArchMasterSlave = "master-slave"
	ArchMGR         = "mgr"

	// File patterns
	MySQLPackagePattern = "mysql-%s-linux-glibc2.17-x86_64-minimal.tar.xz"
	LogFilePattern      = "mysql_deploy_%s.log"
	PasswordFilePattern = "passwd_list_%s"
	BackupDirPattern    = "%s_backup_%s"
)

// DeployConfig holds all deployment configuration
type DeployConfig struct {
	MysqlVersion    string
	InstallPrefix   string
	DataDir         string
	ReplicationUser string
	ReplicationPass string
	AdminUser       string
	AdminPass       string
	RootPass        string
	MyCnfTemplate   string
	ServiceTemplate string
	HostFile        string
	Architecture    string
	SSHUser         string
	SSHPort         int
	MySQLPort       int
	MGRGroupName    string
	MGRPort         int
	ForceDownload   bool
	ForceDeploy     bool
	SkipDependency  bool
	SSHKeyPath      string
}

// HostInfo represents a single host configuration
type HostInfo struct {
	IP       string
	Role     string
	ServerID int
}

// CommandResult holds the result of a command execution
type CommandResult struct {
	Host   string
	Output string
	Error  error
}

// Logger wraps log file operations
type Logger struct {
	file *os.File
}

func (l *Logger) Log(host, command, output string) {
	if l.file != nil {
		l.file.WriteString(fmt.Sprintf("[%s] %s\n%s\n", host, command, output))
	}
}

func (l *Logger) Close() {
	if l.file != nil {
		l.file.Close()
	}
}

// Global variables
var deployConfig DeployConfig

// UserCreateRequest represents a database user creation request
type UserCreateRequest struct {
	Username    string
	Password    string
	Host        string
	Privileges  []string
	RequireSSL  bool
	Description string
}

// deployCmd represents the deploy command
var deployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "自动部署MySQL服务，智能识别架构类型",
	Long: `自动部署MySQL服务，具备以下功能：
- 自动下载指定版本的MySQL安装包
- SSH远程部署MySQL服务
- 智能识别主从复制架构和MGR架构  
- 批量部署，读取主机列表
- 可配置部署路径、数据路径、账号密码等`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := runDeploy(); err != nil {
			fmt.Printf("%s: %v\n", aurora.Red("部署失败"), err)
			os.Exit(1)
		}
	},
}

func init() {
	RootCmd.AddCommand(deployCmd)
	setupFlags()
}

// setupFlags configures command line flags
func setupFlags() {
	flags := deployCmd.Flags()

	flags.StringVarP(&deployConfig.MysqlVersion, "version", "v", DefaultMySQLVersion, "MySQL版本号")
	flags.StringVarP(&deployConfig.InstallPrefix, "install-dir", "i", DefaultInstallDir, "MySQL安装目录前缀")
	flags.StringVarP(&deployConfig.DataDir, "data-dir", "d", DefaultDataDir, "MySQL数据目录")
	flags.StringVarP(&deployConfig.ReplicationUser, "repl-user", "r", "repl", "复制账号用户名")
	flags.StringVarP(&deployConfig.ReplicationPass, "repl-pass", "", "repl123", "复制账号密码")
	flags.StringVarP(&deployConfig.AdminUser, "admin-user", "a", "admin", "管理员账号用户名")
	flags.StringVarP(&deployConfig.AdminPass, "admin-pass", "", "", "管理员账号密码(留空自动生成)")
	flags.StringVarP(&deployConfig.RootPass, "root-pass", "", "", "root密码(留空自动生成)")
	flags.StringVarP(&deployConfig.MyCnfTemplate, "config-template", "c", "my.cnf.template", "my.cnf配置模板文件路径")
	flags.StringVarP(&deployConfig.ServiceTemplate, "service-template", "s", "mysqld.service.template", "systemd服务模板文件路径")
	flags.StringVarP(&deployConfig.HostFile, "host-file", "f", "hosts.txt", "主机列表文件")
	flags.StringVarP(&deployConfig.SSHUser, "ssh-user", "", DefaultSSHUser, "SSH用户名")
	flags.IntVarP(&deployConfig.SSHPort, "ssh-port", "", DefaultSSHPort, "SSH端口")
	flags.IntVarP(&deployConfig.MySQLPort, "mysql-port", "", DefaultMySQLPort, "MySQL服务端口")
	flags.StringVarP(&deployConfig.MGRGroupName, "mgr-group-name", "", "", "MGR组名称(UUID格式，留空自动生成)")
	flags.IntVarP(&deployConfig.MGRPort, "mgr-port", "", DefaultMGRPort, "MGR通信端口")
	flags.BoolVarP(&deployConfig.ForceDownload, "force-download", "", false, "强制重新下载MySQL安装包")
	flags.BoolVarP(&deployConfig.ForceDeploy, "force-deploy", "", false, "强制部署，停止已运行的MySQL服务")
	flags.BoolVarP(&deployConfig.SkipDependency, "skip-dependency", "", false, "跳过依赖包安装检查")
	flags.StringVarP(&deployConfig.SSHKeyPath, "ssh-key", "", os.Getenv("HOME")+"/.ssh/id_rsa", "SSH私钥路径")
}

// runDeploy is the main deployment function
func runDeploy() error {
	fmt.Printf("%s\n", aurora.Green("=== MySQL自动部署工具 ==="))

	// Initialize passwords if not provided
	if deployConfig.AdminPass == "" {
		deployConfig.AdminPass = generateRandomPassword()
	}
	if deployConfig.RootPass == "" {
		deployConfig.RootPass = generateRandomPassword()
	}

	// Phase 1: Preflight checks
	printPhase("前置检查", 1)
	hosts, err := runPreflightChecks()
	if err != nil {
		return fmt.Errorf("前置检查失败: %w", err)
	}

	// Phase 2: Preparation
	printPhase("准备阶段", 2)
	mysqlPackage, logger, passwordFile, err := runPreparation(hosts)
	if err != nil {
		return fmt.Errorf("准备阶段失败: %w", err)
	}
	defer logger.Close()

	// Phase 3: Deployment
	printPhase("部署阶段", 3)
	if err := runDeployment(hosts, mysqlPackage, logger, passwordFile); err != nil {
		return fmt.Errorf("部署阶段失败: %w", err)
	}

	// Phase 4: Architecture configuration
	printPhase("架构配置", 4)
	if err := configureArchitecture(hosts, logger); err != nil {
		return fmt.Errorf("架构配置失败: %w", err)
	}

	printSuccess("部署完成", fmt.Sprintf("密码文件: %s", passwordFile))
	return nil
}

// runPreflightChecks performs all preflight validation
func runPreflightChecks() ([]HostInfo, error) {
	steps := []struct {
		name string
		fn   func() error
	}{
		{"验证配置文件和参数", validateConfig},
		{"检查SSH连接状态", func() error {
			hosts, err := loadHosts()
			if err != nil {
				return err
			}
			return checkAllHostsSSH(hosts)
		}},
		{"检查MySQL运行状态", func() error {
			hosts, err := loadHosts()
			if err != nil {
				return err
			}
			return checkAllHostsMySQL(hosts)
		}},
	}

	for i, step := range steps {
		printStep(step.name, i+1, len(steps))
		if err := step.fn(); err != nil {
			return nil, err
		}
	}

	hosts, err := loadHosts()
	if err != nil {
		return nil, err
	}

	deployConfig.Architecture = detectArchitecture(hosts)
	fmt.Printf("%s 检测到架构类型: %s\n",
		aurora.Blue("4/4"), aurora.Green(deployConfig.Architecture))

	printSuccess("前置检查完成", "")
	return hosts, nil
}

// runPreparation handles all preparation tasks
func runPreparation(hosts []HostInfo) (string, *Logger, string, error) {

	// Download MySQL package
	mysqlPackage, err := downloadMySQL()
	if err != nil {
		return "", nil, "", fmt.Errorf("下载MySQL失败: %w", err)
	}

	// Create log file and password file
	logFile := fmt.Sprintf(LogFilePattern, time.Now().Format("20060102_150405"))
	logger, err := createLogger(logFile)
	if err != nil {
		return "", nil, "", fmt.Errorf("创建日志文件失败: %w", err)
	}

	passwordFile := fmt.Sprintf(PasswordFilePattern, time.Now().Format("20060102_150405"))

	// Display configuration
	displayConfig(logFile, len(hosts))

	printSuccess("准备阶段完成", "")
	return mysqlPackage, logger, passwordFile, nil
}

// runDeployment deploys MySQL to all hosts
func runDeployment(hosts []HostInfo, mysqlPackage string, logger *Logger, passwordFile string) error {
	for i, host := range hosts {
		printHostProgress(host.IP, i+1, len(hosts))

		if err := deployToHost(host, mysqlPackage, logger); err != nil {
			fmt.Printf("%s %s: %v\n",
				aurora.Red(fmt.Sprintf("[%s]:", host.IP)),
				aurora.Red("部署失败"), err)
			continue
		}

		// Record password information
		passwordInfo := fmt.Sprintf("%s root:%s admin:%s\n",
			host.IP, deployConfig.RootPass, deployConfig.AdminPass)
		appendToFile(passwordFile, passwordInfo)

		fmt.Printf("%s %s\n",
			aurora.Green(fmt.Sprintf("[%s]:", host.IP)),
			aurora.Green("部署完成!"))
	}

	printSuccess("部署阶段完成", "")
	return nil
}

// Utility functions for output formatting
func printPhase(name string, phase int) {
	fmt.Printf("\n%s\n", aurora.Blue(fmt.Sprintf("=== 阶段%d: %s ===", phase, name)))
}

func printStep(name string, current, total int) {
	fmt.Printf("%s %s\n", aurora.Blue(fmt.Sprintf("%d/%d", current, total)), name)
}

func printSuccess(title, message string) {
	fmt.Printf("\n%s %s\n", aurora.Green("✓"), aurora.Green(title))
	if message != "" {
		fmt.Printf("%s\n", message)
	}
}

func printHostProgress(host string, current, total int) {
	fmt.Printf("\n%s %s [%d/%d] %s\n",
		aurora.Green(fmt.Sprintf("[%s]:", host)),
		aurora.Blue("部署进度"), current, total,
		strings.Repeat("=", 40))
}

// SSH and remote command execution utilities
func executeSSHCommand(host, command string, logger *Logger) (string, error) {
	client, err := NewSSHClient(
		fmt.Sprintf("%s:%d", host, deployConfig.SSHPort),
		deployConfig.SSHUser,
		deployConfig.SSHKeyPath,
	)
	if err != nil {
		return "", err
	}
	defer client.Close()

	output, err := client.Run(command)

	// 如果提供了日志记录器，则记录命令和输出
	if logger != nil {
		// 打印命令到控制台
		fmt.Printf("%s %s\n", aurora.Blue(fmt.Sprintf("[%s]:", host)), command)
		// 记录到日志文件
		logger.Log(host, command, output)
		// 如果命令执行失败，打印错误信息
		if err != nil {
			fmt.Printf("%s %s\n", aurora.Red("命令执行失败:"), output)
		}
	}

	return output, err
}

// 为了向后兼容保留函数签名，使用新的统一函数实现
func executeSSHCommandWithLogger(host, command string, logger *Logger) error {
	_, err := executeSSHCommand(host, command, logger)
	return err
}

// Parallel execution utilities
func executeParallel(hosts []HostInfo, fn func(HostInfo) error) []error {
	var wg sync.WaitGroup
	errors := make([]error, len(hosts))

	for i, host := range hosts {
		wg.Add(1)
		go func(index int, h HostInfo) {
			defer wg.Done()
			errors[index] = fn(h)
		}(i, host)
	}

	wg.Wait()
	return errors
}

// Validation functions
func validateConfig() error {
	checks := []struct {
		condition bool
		message   string
	}{
		{fileExists(deployConfig.HostFile), fmt.Sprintf("主机文件不存在: %s", deployConfig.HostFile)},
		{fileExists(deployConfig.MyCnfTemplate), fmt.Sprintf("MySQL配置模板文件不存在: %s", deployConfig.MyCnfTemplate)},
		{fileExists(deployConfig.ServiceTemplate), fmt.Sprintf("systemd服务模板文件不存在: %s", deployConfig.ServiceTemplate)},
	}

	for _, check := range checks {
		if !check.condition {
			return fmt.Errorf(check.message)
		}
	}
	return nil
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

// Host loading and architecture detection
func loadHosts() ([]HostInfo, error) {
	file, err := os.Open(deployConfig.HostFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var hosts []HostInfo
	scanner := bufio.NewScanner(file)
	serverID := 1

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		hosts = append(hosts, HostInfo{
			IP:       parts[0],
			Role:     parts[1],
			ServerID: serverID,
		})
		serverID++
	}

	return hosts, scanner.Err()
}

func detectArchitecture(hosts []HostInfo) string {
	roleCount := make(map[string]int)
	for _, host := range hosts {
		roleCount[host.Role]++
	}

	if roleCount["mgr"] > 0 {
		return ArchMGR
	}
	return ArchMasterSlave
}

// SSH connection checking
func checkAllHostsSSH(hosts []HostInfo) error {
	errors := executeParallel(hosts, func(host HostInfo) error {
		if err := testSSHConnection(host.IP); err != nil {
			fmt.Printf("    %s %s\n",
				aurora.Red(fmt.Sprintf("[%s]:", host.IP)),
				aurora.Red("SSH连接失败"))
			return err
		}
		fmt.Printf("    %s %s\n",
			aurora.Green(fmt.Sprintf("[%s]:", host.IP)),
			aurora.Green("SSH连接正常"))
		return nil
	})

	var failedHosts []string
	for i, err := range errors {
		if err != nil {
			failedHosts = append(failedHosts, hosts[i].IP)
		}
	}

	if len(failedHosts) > 0 {
		return fmt.Errorf("以下主机SSH连接失败: %v", failedHosts)
	}
	return nil
}

func testSSHConnection(host string) error {
	// 直接使用统一的SSH执行函数，不记录日志
	_, err := executeSSHCommand(host, "echo 'SSH连接测试成功'", nil)
	return err
}

// MySQL status checking
func checkAllHostsMySQL(hosts []HostInfo) error {
	type mysqlResult struct {
		host      string
		isRunning bool
		err       error
	}

	results := make(chan mysqlResult, len(hosts))
	var wg sync.WaitGroup

	for _, host := range hosts {
		wg.Add(1)
		go func(h HostInfo) {
			defer wg.Done()
			isRunning, err := checkMySQLRunning(h.IP)
			results <- mysqlResult{host: h.IP, isRunning: isRunning, err: err}
		}(host)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var runningHosts []string
	for result := range results {
		switch {
		case result.err != nil:
			fmt.Printf("    %s %s\n",
				aurora.Yellow(fmt.Sprintf("[%s]:", result.host)),
				aurora.Yellow("无法检查MySQL状态"))
		case result.isRunning:
			runningHosts = append(runningHosts, result.host)
			fmt.Printf("    %s %s\n",
				aurora.Red(fmt.Sprintf("[%s]:", result.host)),
				aurora.Red("MySQL运行中"))
		default:
			fmt.Printf("    %s %s\n",
				aurora.Green(fmt.Sprintf("[%s]:", result.host)),
				aurora.Green("MySQL未运行"))
		}
	}

	if len(runningHosts) > 0 && !deployConfig.ForceDeploy {
		return fmt.Errorf("检测到MySQL正在运行的主机: %v，请使用 --force-deploy 强制部署", runningHosts)
	}

	if len(runningHosts) > 0 {
		fmt.Printf("    %s 检测到 --force-deploy 参数，将强制停止现有MySQL服务\n",
			aurora.Yellow("警告:"))
	}

	return nil
}

func checkMySQLRunning(host string) (bool, error) {
	// Check systemd status first
	if output, err := executeSSHCommand(host, "systemctl is-active mysqld 2>/dev/null", nil); err == nil {
		return strings.TrimSpace(output) == "active", nil
	}

	// Fallback to process check
	if output, err := executeSSHCommand(host, "pgrep -f mysqld", nil); err == nil {
		return len(strings.TrimSpace(output)) > 0, nil
	}

	return false, nil
}

// Download and package management
func downloadMySQL() (string, error) {
	fileName := fmt.Sprintf(MySQLPackagePattern, deployConfig.MysqlVersion)
	filePath := filepath.Join(".", fileName)

	if !deployConfig.ForceDownload && fileExists(filePath) {
		fmt.Printf("%s %s\n", aurora.Blue("使用已存在的安装包:"), filePath)
		return filePath, nil
	}

	url := fmt.Sprintf("%s/%s", DownloadUrl, fileName)
	fmt.Printf("%s %s\n", aurora.Blue("开始下载MySQL安装包:"), url)

	return downloadFile(url, filePath)
}

func downloadFile(url, filePath string) (string, error) {
	out, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer out.Close()

	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("下载失败，HTTP状态: %s", resp.Status)
	}

	if size := resp.Header.Get("Content-Length"); size != "" {
		if sizeInt, err := strconv.ParseInt(size, 10, 64); err == nil {
			fmt.Printf("%s %s\n", aurora.Blue("文件大小:"), byteHumen(sizeInt))
		}
	}

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return "", err
	}

	// Verify downloaded file
	if fileInfo, err := os.Stat(filePath); err == nil {
		fmt.Printf("%s 文件大小: %s\n",
			aurora.Green("下载完成:"), byteHumen(fileInfo.Size()))
	}

	return filePath, nil
}

// Utility functions
func generateRandomPassword() string {
	bytes := make([]byte, 32)
	rand.Read(bytes)
	return base64.StdEncoding.EncodeToString(bytes)[:24]
}

func createLogger(filename string) (*Logger, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return &Logger{file: file}, nil
}

func appendToFile(filename, content string) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(content)
	return err
}

func displayConfig(logFile string, hostCount int) {
	fmt.Printf("    日志文件: %s\n", logFile)
	fmt.Printf("    Root密码: %s\n", deployConfig.RootPass)
	fmt.Printf("    管理员密码: %s\n", deployConfig.AdminPass)
	fmt.Printf("    部署主机数量: %d\n", hostCount)
}

// Host deployment functions
func deployToHost(host HostInfo, mysqlPackage string, logger *Logger) error {
	steps := []struct {
		name string
		fn   func() error
	}{
		{"检查部署状态", func() error { return checkDeploymentStatus(host, logger) }},
		{"环境准备", func() error { return prepareEnvironment(host.IP, logger) }},
		{"安装依赖", func() error { return installDependencies(host.IP, logger) }},
		{"文件操作", func() error { return handleFileOperations(host, mysqlPackage, logger) }},
		{"MySQL配置", func() error { return configureMySQLInstance(host, logger) }},
		{"服务配置", func() error { return configureAndStartService(host.IP, logger) }},
		{"初始化数据库", func() error { return initializeDatabaseAndUsers(host.IP, logger) }},
	}

	for i, step := range steps {
		fmt.Printf("%s 步骤%d: %s\n",
			aurora.Blue(fmt.Sprintf("[%s]:", host.IP)), i+1, step.name)

		if err := step.fn(); err != nil {
			return fmt.Errorf("%s失败: %w", step.name, err)
		}
	}

	return nil
}

// Implementation functions
func checkDeploymentStatus(host HostInfo, logger *Logger) error {
	fmt.Printf("%s 检查MySQL完整部署状态\n", aurora.Blue(fmt.Sprintf("[%s]:", host.IP)))

	mysqlPackage := fmt.Sprintf(MySQLPackagePattern, deployConfig.MysqlVersion)
	mysqlDir := strings.Replace(mysqlPackage, ".tar.xz", "", 1)

	// Check if MySQL is installed
	checkInstallCmd := fmt.Sprintf("test -d %s/%s && test -L %s/mysql && echo 'installed' || echo 'not installed'",
		deployConfig.InstallPrefix, mysqlDir, deployConfig.InstallPrefix)

	output, err := executeSSHCommand(host.IP, checkInstallCmd, logger)
	if err != nil || strings.TrimSpace(output) != "installed" {
		fmt.Printf("%s  MySQL未安装或符号链接缺失\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)))
		return nil // Not an error, just not installed
	}

	// Check if database is initialized
	checkInitCmd := getMySQLInitializeCheckCmd(deployConfig.DataDir)

	output, err = executeSSHCommand(host.IP, checkInitCmd, logger)
	if err != nil || strings.TrimSpace(output) != "initialized" {
		fmt.Printf("%s  数据库未初始化\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)))
		return nil
	}

	// Check systemd service
	output, err = executeSSHCommand(host.IP, "test -f /etc/systemd/system/mysqld.service && echo 'exists' || echo 'not exists'", logger)
	if err != nil || strings.TrimSpace(output) != "exists" {
		fmt.Printf("%s  systemd服务文件不存在\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)))
		return nil
	}

	// Check if MySQL is running
	if running, err := checkMySQLRunning(host.IP); err != nil || !running {
		fmt.Printf("%s  MySQL服务未运行\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)))
		return nil
	}

	fmt.Printf("%s  ✓ MySQL部署完整\n", aurora.Green(fmt.Sprintf("[%s]:", host.IP)))
	logger.Log(host.IP, "deployment status", "complete deployment detected")

	if !deployConfig.ForceDeploy {
		return fmt.Errorf("MySQL已完整部署，如需重新部署请使用 --force-deploy")
	}

	return nil
}

func prepareEnvironment(host string, logger *Logger) error {
	// Stop existing MySQL service if force deploy
	if deployConfig.ForceDeploy {
		fmt.Printf("%s   强制停止MySQL服务\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)))
		executeSSHCommandWithLogger(host, "systemctl stop mysqld 2>/dev/null || killall -9 mysqld_safe mysqld 2>/dev/null", logger)

		// Wait for service to stop completely
		fmt.Printf("%s   等待MySQL服务完全停止\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
		time.Sleep(3 * time.Second)
	}

	// Clean old installation directories
	fmt.Printf("%s   清理旧安装目录\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
	if err := executeSSHCommandWithLogger(host, fmt.Sprintf("rm -rf %s/mysql %s/mysql-*", deployConfig.InstallPrefix, deployConfig.InstallPrefix), logger); err != nil {
		return err
	}

	// Force deploy mode: clean data directory
	if deployConfig.ForceDeploy {
		fmt.Printf("%s   强制清理数据目录\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)))

		// Backup data directory if exists and not empty
		backupDataDir := fmt.Sprintf(BackupDirPattern, deployConfig.DataDir, time.Now().Format("20060102_150405"))
		checkDataCmd := fmt.Sprintf("if [ -d \"%s\" ] && [ \"$(ls -A %s 2>/dev/null)\" ]; then mv %s %s && echo 'backed up to %s'; else echo 'no data to backup'; fi",
			deployConfig.DataDir, deployConfig.DataDir, deployConfig.DataDir, backupDataDir, backupDataDir)
		executeSSHCommandWithLogger(host, checkDataCmd, logger)

		// Clean systemd service files
		fmt.Printf("%s   清理systemd服务文件\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
		executeSSHCommandWithLogger(host, "rm -f /etc/systemd/system/mysqld.service", logger)
	}

	// Create necessary directories
	fmt.Printf("%s   创建必要目录\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
	if err := executeSSHCommandWithLogger(host, fmt.Sprintf("mkdir -p %s %s", deployConfig.InstallPrefix, deployConfig.DataDir), logger); err != nil {
		return err
	}

	// Create mysql user (idempotent)
	fmt.Printf("%s   检查并创建mysql用户\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
	if err := executeSSHCommandWithLogger(host, "id mysql >/dev/null 2>&1 || useradd mysql -d /dev/null -s /bin/false", logger); err != nil {
		return err
	}

	// 修改主机名，格式为主机IP，点换成横线
	newHostname := strings.ReplaceAll(host, ".", "-")
	fmt.Printf("%s   修改主机名为: %s\n", aurora.Blue(fmt.Sprintf("[%s]:", host)), newHostname)
	setHostnameCmd := fmt.Sprintf("hostnamectl set-hostname %s", newHostname)
	if err := executeSSHCommandWithLogger(host, setHostnameCmd, logger); err != nil {
		return err
	}

	return nil
}

func installDependencies(host string, logger *Logger) error {
	if deployConfig.SkipDependency {
		fmt.Printf("%s   跳过依赖安装\n",
			aurora.Yellow(fmt.Sprintf("[%s]:", host)))
		return nil
	}

	fmt.Printf("%s 检查并安装MySQL依赖包\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))

	// Detect OS type
	osType, err := detectOSType(host)
	if err != nil {
		fmt.Printf("%s 无法检测操作系统类型: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)), err)
		return nil // Not blocking, continue execution
	}

	fmt.Printf("%s 检测到操作系统: %s\n", aurora.Blue(fmt.Sprintf("[%s]:", host)), osType)

	// Install dependencies based on OS type
	var installCmds []string

	switch osType {
	case "centos", "rhel", "fedora":
		installCmds = []string{
			"yum install -y libaio libaio-devel numactl-libs ncurses-compat-libs",
		}
	case "ubuntu", "debian":
		installCmds = []string{
			"apt-get update && apt-get install -y libaio1 libaio-dev libnuma1 libncurses5",
		}
	case "suse", "opensuse":
		installCmds = []string{
			"zypper install -y libaio1 libaio-devel libnuma1 libncurses5",
		}
	default:
		fmt.Printf("%s 未知操作系统类型: %s，跳过依赖安装\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)), osType)
		return nil
	}

	// Execute install commands
	for _, cmd := range installCmds {
		fmt.Printf("%s 安装依赖\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
		if err := executeSSHCommandWithLogger(host, cmd, logger); err != nil {
			fmt.Printf("%s 安装命令失败: %s，但继续执行\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)), cmd)
		}
	}

	// Verify key dependencies
	fmt.Printf("%s 验证依赖安装\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
	executeSSHCommandWithLogger(host, "ldconfig -p | grep libaio || echo 'libaio not found'", logger)

	return nil
}

func detectOSType(host string) (string, error) {
	// OS detection checks
	osChecks := []struct {
		name string
		cmd  string
	}{
		{"centos", "test -f /etc/centos-release"},
		{"rhel", "test -f /etc/redhat-release"},
		{"ubuntu", "test -f /etc/lsb-release && grep -i ubuntu /etc/lsb-release"},
		{"debian", "test -f /etc/debian_version"},
		{"fedora", "test -f /etc/fedora-release"},
		{"suse", "test -f /etc/SuSE-release"},
		{"opensuse", "test -f /etc/os-release && grep -i opensuse /etc/os-release"},
	}

	for _, check := range osChecks {
		_, err := executeSSHCommand(host, check.cmd, nil)
		if err == nil {
			return check.name, nil
		}
	}

	return "unknown", fmt.Errorf("无法识别操作系统类型")
}

func handleFileOperations(host HostInfo, mysqlPackage string, logger *Logger) error {
	// Copy configuration file - now required, no default generation
	if err := copyFileToRemote(host.IP, deployConfig.MyCnfTemplate, "/etc/my.cnf", logger); err != nil {
		return fmt.Errorf("复制配置文件失败: %w", err)
	}

	// Update placeholders in configuration file
	if err := updateConfigPlaceholders(host, logger); err != nil {
		return fmt.Errorf("更新配置占位符失败: %w", err)
	}

	// Copy and extract MySQL package
	if err := copyAndExtractMySQL(host.IP, mysqlPackage, logger); err != nil {
		return err
	}

	// Create symbolic link
	mysqlDir := strings.Replace(filepath.Base(mysqlPackage), ".tar.xz", "", 1)
	if err := executeSSHCommandWithLogger(host.IP, fmt.Sprintf("cd %s && ln -sf %s mysql", deployConfig.InstallPrefix, mysqlDir), logger); err != nil {
		return err
	}

	return nil
}

func copyFileToRemote(host, localFile, remoteFile string, logger *Logger) error {
	fmt.Printf("%s 复制文件: %s -> %s\n", aurora.Blue(fmt.Sprintf("[%s]:", host)), localFile, remoteFile)
	client, err := NewSSHClient(
		fmt.Sprintf("%s:%d", host, deployConfig.SSHPort),
		deployConfig.SSHUser,
		deployConfig.SSHKeyPath,
	)
	if err != nil {
		return err
	}
	defer client.Close()
	err = client.CopyFile(localFile, remoteFile)
	logger.Log(host, fmt.Sprintf("copy file %s -> %s", localFile, remoteFile), fmt.Sprintf("err: %v", err))
	if err != nil {
		return fmt.Errorf("文件复制失败: %w", err)
	}
	return nil
}

func updateConfigPlaceholders(host HostInfo, logger *Logger) error {
	fmt.Printf("%s   更新配置文件占位符\n", aurora.Blue(fmt.Sprintf("[%s]:", host.IP)))

	// Replace placeholders in my.cnf
	placeholders := map[string]string{
		"{{SERVER_ID}}":      fmt.Sprintf("%d", host.ServerID),
		"{{MYSQL_PORT}}":     fmt.Sprintf("%d", deployConfig.MySQLPort),
		"{{DATA_DIR}}":       deployConfig.DataDir,
		"{{INSTALL_PREFIX}}": deployConfig.InstallPrefix,
		"{{MGR_PORT}}":       fmt.Sprintf("%d", deployConfig.MGRPort),
	}

	for placeholder, value := range placeholders {
		replaceCmd := fmt.Sprintf("sed -i 's|%s|%s|g' /etc/my.cnf", placeholder, value)
		if err := executeSSHCommandWithLogger(host.IP, replaceCmd, logger); err != nil {
			return err
		}
	}

	return nil
}

func copyAndExtractMySQL(host, mysqlPackage string, logger *Logger) error {
	fmt.Printf("%s 检查MySQL安装状态\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))

	remoteFile := fmt.Sprintf("/tmp/%s", filepath.Base(mysqlPackage))
	mysqlDir := strings.Replace(filepath.Base(mysqlPackage), ".tar.xz", "", 1)

	// Check if MySQL is already extracted
	checkDirCmd := fmt.Sprintf("test -d %s/%s && echo 'extracted' || echo 'not extracted'", deployConfig.InstallPrefix, mysqlDir)
	output, err := executeSSHCommand(host, checkDirCmd, nil)

	if err == nil && strings.TrimSpace(output) == "extracted" {
		fmt.Printf("%s MySQL已解压，跳过复制和解压\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
		return nil
	}

	// Check if remote package file exists and is complete
	fmt.Printf("%s 检查远程安装包文件\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
	localStat, err := os.Stat(mysqlPackage)
	if err != nil {
		return fmt.Errorf("无法获取本地文件信息: %w", err)
	}
	localSize := localStat.Size()

	needCopy := true
	if remoteOutput, err := executeSSHCommand(host, fmt.Sprintf("stat -c%%s %s 2>/dev/null || echo '0'", remoteFile), nil); err == nil {
		if remoteSize, parseErr := strconv.ParseInt(strings.TrimSpace(remoteOutput), 10, 64); parseErr == nil && remoteSize == localSize && remoteSize > 0 {
			fmt.Printf("%s 远程安装包已存在且完整，跳过传输\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
			needCopy = false
		}
	}

	if needCopy {
		fmt.Printf("%s 复制MySQL安装包到远程主机\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
		if err := copyFileToRemote(host, mysqlPackage, remoteFile, logger); err != nil {
			return err
		}
	}

	// Extract package
	return extractMySQLPackage(host, remoteFile, deployConfig.InstallPrefix, logger)
}

func extractMySQLPackage(host, packageFile, targetDir string, logger *Logger) error {
	fmt.Printf("%s 解压MySQL安装包\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))

	// Use tar -J parameter to extract .tar.xz files
	extractCmd := fmt.Sprintf("cd %s && tar -Jxf %s", targetDir, packageFile)
	if err := executeSSHCommandWithLogger(host, extractCmd, logger); err != nil {
		return fmt.Errorf("解压失败: %w", err)
	}

	// Verify extraction result
	checkCmd := fmt.Sprintf("ls -la %s/mysql-*/bin/mysqld", targetDir)
	if err := executeSSHCommandWithLogger(host, checkCmd, logger); err != nil {
		return fmt.Errorf("解压后验证失败，未找到MySQL可执行文件")
	}

	fmt.Printf("%s 解压完成\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
	return nil
}

func configureMySQLInstance(host HostInfo, logger *Logger) error {
	// Check and initialize database if needed
	if err := initializeDatabaseIfNeeded(host.IP, logger); err != nil {
		return err
	}

	// Set data directory permissions
	if err := executeSSHCommandWithLogger(host.IP, fmt.Sprintf("chown -R mysql:mysql %s", deployConfig.DataDir), logger); err != nil {
		return err
	}

	return nil
}

func initializeDatabaseIfNeeded(host string, logger *Logger) error {
	// MySQL 8.0 check if database is already initialized
	checkInitCmd := getMySQLInitializeCheckCmd(deployConfig.DataDir)

	output, err := executeSSHCommand(host, checkInitCmd, nil)
	initStatus := strings.TrimSpace(output)

	if err == nil && initStatus == "initialized" {
		fmt.Printf("%s   数据库已初始化，跳过初始化步骤\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
		return nil
	}

	// Initialize database
	fmt.Printf("%s   初始化MySQL数据库\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))

	// Ensure data directory exists and is empty
	if err := executeSSHCommandWithLogger(host, fmt.Sprintf("mkdir -p %s", deployConfig.DataDir), logger); err != nil {
		return err
	}

	// Check if data directory is empty
	checkEmptyCmd := fmt.Sprintf("ls -A %s | wc -l", deployConfig.DataDir)
	if emptyOutput, err := executeSSHCommand(host, checkEmptyCmd, nil); err == nil {
		if fileCount := strings.TrimSpace(emptyOutput); fileCount != "0" {
			fmt.Printf("%s   数据目录不为空，清理旧数据\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)))
			if err := executeSSHCommandWithLogger(host, fmt.Sprintf("rm -rf %s/*", deployConfig.DataDir), logger); err != nil {
				return err
			}
		}
	}

	// Execute initialization
	if err := executeSSHCommandWithLogger(host, fmt.Sprintf("cd %s/mysql && ./bin/mysqld --initialize-insecure --user=mysql --datadir=%s", deployConfig.InstallPrefix, deployConfig.DataDir), logger); err != nil {
		return err
	}

	// Generate SSL certificates
	fmt.Printf("%s   生成SSL证书\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
	if err := generateSSLCertificates(host, logger); err != nil {
		fmt.Printf("%s   SSL证书生成失败: %v，但继续执行\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)), err)
	} else {
		fmt.Printf("%s   SSL证书生成完成\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
	}

	fmt.Printf("%s   数据库初始化完成\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
	return nil
}

func generateSSLCertificates(host string, logger *Logger) error {
	// Use mysql_ssl_rsa_setup to generate SSL certificates
	sslSetupCmd := fmt.Sprintf("cd %s && ./bin/mysql_ssl_rsa_setup --datadir=%s --uid=mysql",
		deployConfig.InstallPrefix+"/mysql", deployConfig.DataDir)

	if err := executeSSHCommandWithLogger(host, sslSetupCmd, logger); err != nil {
		return fmt.Errorf("SSL证书生成失败: %w", err)
	}

	// Set certificate file permissions
	chownCmd := fmt.Sprintf("chown mysql:mysql %s/*.pem", deployConfig.DataDir)
	if err := executeSSHCommandWithLogger(host, chownCmd, logger); err != nil {
		return fmt.Errorf("设置SSL证书权限失败: %w", err)
	}

	return nil
}

func configureAndStartService(host string, logger *Logger) error {
	// Create systemd service file
	if err := createMySQLSystemdService(host, logger); err != nil {
		return err
	}

	// Enable MySQL service
	executeSSHCommandWithLogger(host, "systemctl daemon-reload && systemctl enable mysqld", logger)

	// Add PATH environment variable (idempotent)
	pathCheckCmd := fmt.Sprintf("grep -q '%s/mysql/bin' /etc/bashrc || echo 'export PATH=$PATH:%s/mysql/bin' >> /etc/bashrc", deployConfig.InstallPrefix, deployConfig.InstallPrefix)
	if err := executeSSHCommandWithLogger(host, pathCheckCmd, logger); err != nil {
		return err
	}

	// Start MySQL service
	if err := startMySQLService(host, logger); err != nil {
		return err
	}

	// Wait and verify service startup
	if err := waitForMySQLStart(host, logger); err != nil {
		return err
	}

	return nil
}

func createMySQLSystemdService(host string, logger *Logger) error {
	// Check if systemd service file already exists
	output, err := executeSSHCommand(host, "test -f /etc/systemd/system/mysqld.service && echo 'exists' || echo 'not exists'", nil)

	if err == nil && strings.TrimSpace(output) == "exists" {
		fmt.Printf("%s systemd服务文件已存在，跳过创建\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
		return nil
	}

	fmt.Printf("%s 使用模板创建systemd服务文件\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))

	// Copy service template file to remote host
	if err := copyFileToRemote(host, deployConfig.ServiceTemplate, "/etc/systemd/system/mysqld.service", logger); err != nil {
		return fmt.Errorf("复制systemd服务文件失败: %w", err)
	}

	// Replace placeholders in service file
	if err := replaceServicePlaceholders(host, logger); err != nil {
		return fmt.Errorf("替换服务文件占位符失败: %w", err)
	}

	fmt.Printf("%s systemd服务文件创建完成\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
	return nil
}

func startMySQLService(host string, logger *Logger) error {
	// Check if MySQL service is already running
	if output, err := executeSSHCommand(host, "systemctl is-active mysqld 2>/dev/null", nil); err == nil {
		if strings.TrimSpace(output) == "active" {
			fmt.Printf("%s MySQL服务已在运行，跳过启动\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
			return nil
		}
	}

	// Start MySQL service using systemd
	fmt.Printf("%s 使用systemd启动MySQL服务\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
	if err := executeSSHCommandWithLogger(host, "systemctl start mysqld", logger); err != nil {
		fmt.Printf("%s MySQL启动命令失败: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)), err)
		return nil // Don't block, continue to check status
	}

	fmt.Printf("%s MySQL服务启动命令执行完成\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
	return nil
}

func waitForMySQLStart(host string, logger *Logger) error {
	fmt.Printf("%s 等待MySQL服务启动\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))

	maxRetries := 12 // Wait up to 60 seconds (12 * 5 seconds)
	for i := 0; i < maxRetries; i++ {
		// Check MySQL process
		if output, err := executeSSHCommand(host, "pgrep -f mysqld > /dev/null 2>&1 && echo 'running' || echo 'not running'", nil); err == nil && strings.Contains(output, "running") {
			fmt.Printf("%s MySQL进程检查: 运行中\n", aurora.Green(fmt.Sprintf("[%s]:", host)))

			// Check if MySQL port is listening
			portCheckCmd := fmt.Sprintf("netstat -tlnp 2>/dev/null | grep :%d", deployConfig.MySQLPort)
			if portOutput, portErr := executeSSHCommand(host, portCheckCmd, nil); portErr == nil && len(portOutput) > 0 {
				fmt.Printf("%s MySQL端口检查: 监听中\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
				return nil
			}
		}

		if i < maxRetries-1 {
			fmt.Printf("%s 等待MySQL启动... (%d/%d)\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)), i+1, maxRetries)
			time.Sleep(5 * time.Second)
		}
	}

	return fmt.Errorf("MySQL启动超时，请检查服务状态")
}

func initializeDatabaseAndUsers(host string, logger *Logger) error {
	// Set root password
	if err := executeSSHCommandWithLogger(host, fmt.Sprintf("%s/mysql/bin/mysqladmin -uroot password '%s'", deployConfig.InstallPrefix, deployConfig.RootPass), logger); err != nil {
		return err
	}

	// Create admin account using the new abstracted function
	adminUserReq := UserCreateRequest{
		Username:    deployConfig.AdminUser,
		Password:    deployConfig.AdminPass,
		Host:        "%",
		Privileges:  []string{"ALL ON *.*"},
		RequireSSL:  false,
		Description: "管理员账号",
	}

	if err := createMySQLUser(host, adminUserReq, logger); err != nil {
		return err
	}

	return nil
}

func createMySQLUser(host string, userReq UserCreateRequest, logger *Logger) error {
	fmt.Printf("%s   创建数据库用户: %s@%s (%s)\n",
		aurora.Blue(fmt.Sprintf("[%s]:", host)),
		userReq.Username, userReq.Host, userReq.Description)

	var commands []string

	// Build CREATE USER command
	createUserCmd := fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED WITH caching_sha2_password BY '%s'",
		userReq.Username, userReq.Host, userReq.Password)

	if userReq.RequireSSL {
		createUserCmd += " REQUIRE SSL"
	}
	createUserCmd += ";"
	commands = append(commands, createUserCmd)

	// Build GRANT commands
	if len(userReq.Privileges) > 0 {
		for _, privilege := range userReq.Privileges {
			grantCmd := fmt.Sprintf("GRANT %s TO '%s'@'%s';", privilege, userReq.Username, userReq.Host)
			commands = append(commands, grantCmd)
		}
	}

	// Execute all commands
	for _, cmd := range commands {
		if err := runMySQLCommand(host, cmd, logger); err != nil {
			return fmt.Errorf("创建用户 %s 失败: %w", userReq.Username, err)
		}
	}

	fmt.Printf("%s   用户 %s@%s 创建完成\n",
		aurora.Green(fmt.Sprintf("[%s]:", host)),
		userReq.Username, userReq.Host)

	// Reset master to clear binary logs and prepare for replication setup
	fmt.Printf("%s   执行RESET MASTER清理二进制日志\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
	if err := runMySQLCommand(host, "RESET MASTER;", logger); err != nil {
		fmt.Printf("%s   RESET MASTER执行失败: %v，但继续执行\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)), err)
	} else {
		fmt.Printf("%s   RESET MASTER执行完成\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
	}

	return nil
}

func runMySQLCommand(host, sqlCommand string, logger *Logger) error {
	cmd := fmt.Sprintf("echo \"%s\" | %s/mysql/bin/mysql -uroot -p%s",
		sqlCommand, deployConfig.InstallPrefix, deployConfig.RootPass)
	return executeSSHCommandWithLogger(host, cmd, logger)
}

func configureArchitecture(hosts []HostInfo, logger *Logger) error {
	switch deployConfig.Architecture {
	case ArchMasterSlave:
		return configureMasterSlave(hosts, logger)
	case ArchMGR:
		return configureMGR(hosts, logger)
	default:
		return fmt.Errorf("未知的架构类型: %s", deployConfig.Architecture)
	}
}

func configureMasterSlave(hosts []HostInfo, logger *Logger) error {
	var masterHost HostInfo
	var slaveHosts []HostInfo

	// Separate master and slave nodes
	for _, host := range hosts {
		if host.Role == "master" {
			masterHost = host
		} else if host.Role == "slave" {
			slaveHosts = append(slaveHosts, host)
		}
	}

	if masterHost.IP == "" {
		return fmt.Errorf("未找到主节点")
	}

	fmt.Printf("\n%s\n", aurora.Green("=== 配置主从复制 ==="))

	// Create replication user on master node using the new abstracted function
	replUserReq := UserCreateRequest{
		Username:    deployConfig.ReplicationUser,
		Password:    deployConfig.ReplicationPass,
		Host:        "%",
		Privileges:  []string{"BACKUP_ADMIN, REPLICATION SLAVE ON *.*"},
		RequireSSL:  true,
		Description: "主从复制账号",
	}

	if err := createMySQLUser(masterHost.IP, replUserReq, logger); err != nil {
		return fmt.Errorf("创建复制用户失败: %w", err)
	}

	// Configure slave nodes
	for _, slave := range slaveHosts {
		changeSQL := fmt.Sprintf(`
			CHANGE MASTER TO 
				MASTER_HOST='%s',
				MASTER_USER='%s',
				MASTER_PASSWORD='%s',
				MASTER_AUTO_POSITION=1;
			START SLAVE;
		`, masterHost.IP, deployConfig.ReplicationUser, deployConfig.ReplicationPass)

		if err := runMySQLCommand(slave.IP, changeSQL, logger); err != nil {
			fmt.Printf("%s 配置从节点失败: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", slave.IP)), err)
		} else {
			fmt.Printf("%s 主从复制配置完成\n", aurora.Green(fmt.Sprintf("[%s]:", slave.IP)))
		}
	}

	return nil
}

func configureMGR(hosts []HostInfo, logger *Logger) error {
	fmt.Printf("\n%s\n", aurora.Green("=== 配置MGR集群 ==="))

	// Generate or use specified MGR group name
	if deployConfig.MGRGroupName == "" {
		deployConfig.MGRGroupName = generateMGRGroupName()
		fmt.Printf("自动生成MGR组名: %s\n", deployConfig.MGRGroupName)
	}

	// Create replication users for all nodes
	fmt.Printf("%s 创建复制用户\n", aurora.Blue("步骤2:"))
	for _, host := range hosts {
		if err := createMGRReplicationUser(host.IP, logger); err != nil {
			fmt.Printf("%s 创建复制用户失败: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)), err)
			return err
		}
	}

	// Build cluster address list
	groupSeeds := buildMGRGroupSeeds(hosts)
	fmt.Printf("%s 集群地址列表: %s\n", aurora.Blue("步骤3:"), groupSeeds)

	// Configure MGR parameters and start cluster
	fmt.Printf("%s 配置并启动MGR集群\n", aurora.Blue("步骤4:"))
	for i, host := range hosts {
		isBootstrap := (i == 0)
		if err := configureMGRNode(host, groupSeeds, isBootstrap, logger); err != nil {
			fmt.Printf("%s MGR节点配置失败: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)), err)
			return err
		}
	}

	// 检查每个节点MGR状态
	fmt.Printf("\n%s\n", aurora.Blue("=== 检查MGR节点状态 ==="))
	checkMGROnlineStatus(hosts, logger)

	return nil
}

// 检查每个节点MGR状态是否为ONLINE
func checkMGROnlineStatus(hosts []HostInfo, logger *Logger) {
	for _, host := range hosts {
		fmt.Printf("%s 检查MGR状态...\n", aurora.Blue(fmt.Sprintf("[%s]:", host.IP)))
		query := "SELECT MEMBER_HOST, MEMBER_STATE FROM performance_schema.replication_group_members;"
		cmd := fmt.Sprintf("echo \"%s\" | %s/mysql/bin/mysql -uroot -p%s -N -e \"%s\"", query, deployConfig.InstallPrefix, deployConfig.RootPass, query)
		output, err := executeSSHCommand(host.IP, cmd, logger)
		if err != nil {
			fmt.Printf("%s 查询失败: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)), err)
			continue
		}
		lines := strings.Split(strings.TrimSpace(output), "\n")
		allOnline := true
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) != 2 {
				continue
			}
			hostname, state := fields[0], fields[1]
			if state != "ONLINE" {
				fmt.Printf("%s 节点: %s 状态: %s\n", aurora.Red(fmt.Sprintf("[%s]:", host.IP)), hostname, aurora.Red(state))
				allOnline = false
			} else {
				fmt.Printf("%s 节点: %s 状态: %s\n", aurora.Green(fmt.Sprintf("[%s]:", host.IP)), hostname, aurora.Green(state))
			}
		}
		if allOnline {
			fmt.Printf("%s 所有成员ONLINE\n", aurora.Green(fmt.Sprintf("[%s]:", host.IP)))
		} else {
			fmt.Printf("%s 存在非ONLINE成员，请检查！\n", aurora.Red(fmt.Sprintf("[%s]:", host.IP)))
		}
	}
}

func generateMGRGroupName() string {
	// Generate UUID format group name
	b := make([]byte, 16)
	rand.Read(b)
	b[8] = (b[8] | 0x40) & 0x7F
	b[6] = (b[6] & 0x0f) | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func createMGRReplicationUser(host string, logger *Logger) error {
	replUserReq := UserCreateRequest{
		Username:    deployConfig.ReplicationUser,
		Password:    deployConfig.ReplicationPass,
		Host:        "%",
		Privileges:  []string{"BACKUP_ADMIN, REPLICATION SLAVE ON *.*"},
		RequireSSL:  true,
		Description: "MGR复制账号",
	}

	return createMySQLUser(host, replUserReq, logger)
}

func buildMGRGroupSeeds(hosts []HostInfo) string {
	var groupSeeds []string
	for _, host := range hosts {
		groupSeeds = append(groupSeeds, fmt.Sprintf("%s:%d", host.IP, deployConfig.MGRPort))
	}
	return strings.Join(groupSeeds, ",")
}

func configureMGRNode(host HostInfo, groupSeeds string, isBootstrap bool, logger *Logger) error {
	// 1. Write MGR config to my.cnf first
	if err := writeMGRConfigToMyCnf(host, groupSeeds, logger); err != nil {
		return fmt.Errorf("写入MGR参数到my.cnf失败: %w", err)
	}

	// 2. Restart MySQL service to apply config
	fmt.Printf("%s Restarting MySQL service to apply MGR config...\n", aurora.Blue(fmt.Sprintf("[%s]:", host.IP)))
	restartCmd := "systemctl restart mysqld"
	if err := executeSSHCommandWithLogger(host.IP, restartCmd, logger); err != nil {
		return fmt.Errorf("重启MySQL服务失败: %w", err)
	}

	// 3. Wait for MySQL to start
	if err := waitForMySQLStart(host.IP, logger); err != nil {
		return fmt.Errorf("MySQL启动失败: %w", err)
	}

	// 4. Set bootstrap if needed, then start group replication
	if isBootstrap {
		if err := runMySQLCommand(host.IP, "SET GLOBAL group_replication_bootstrap_group=ON;", logger); err != nil {
			return fmt.Errorf("设置bootstrap模式失败: %w", err)
		}
	}

	changeMasterCmd := fmt.Sprintf("CHANGE MASTER TO MASTER_USER='%s',MASTER_PASSWORD='%s' FOR CHANNEL 'group_replication_recovery';",
		deployConfig.ReplicationUser, deployConfig.ReplicationPass)
	if err := runMySQLCommand(host.IP, changeMasterCmd, logger); err != nil {
		return fmt.Errorf("配置MGR复制通道失败: %w", err)
	}

	if err := runMySQLCommand(host.IP, "START GROUP_REPLICATION;", logger); err != nil {
		return fmt.Errorf("启动组复制失败: %w", err)
	}

	// Turn off bootstrap mode after first node
	if isBootstrap {
		time.Sleep(3 * time.Second)
		runMySQLCommand(host.IP, "SET GLOBAL group_replication_bootstrap_group=OFF;", logger)
	}

	fmt.Printf("%s MGR节点配置完成\n", aurora.Green(fmt.Sprintf("[%s]:", host.IP)))
	return nil
}

const mgrCnfTemplate = `
# group replication config
report_host={{ .Host }}
plugin-load-add = group_replication.so
group_replication_group_name = {{.GroupName}}
group_replication_start_on_boot = OFF
group_replication_local_address = {{.LocalAddr}}
group_replication_group_seeds = "{{.GroupSeeds}}"
group_replication_bootstrap_group = OFF
group_replication_recovery_use_ssl = on
`

func writeMGRConfigToMyCnf(host HostInfo, groupSeeds string, logger *Logger) error {
	groupName := deployConfig.MGRGroupName
	localAddr := fmt.Sprintf("%s:%d", host.IP, deployConfig.MGRPort)
	tmpl, _ := template.New("mgr").Parse(mgrCnfTemplate)
	var buf bytes.Buffer
	tmpl.Execute(&buf, map[string]string{
		"Host":       host.IP,
		"GroupName":  groupName,
		"LocalAddr":  localAddr,
		"GroupSeeds": groupSeeds,
	})
	cmd := fmt.Sprintf("echo '%s' >> /etc/my.cnf", strings.ReplaceAll(buf.String(), "'", "'\\''"))
	return executeSSHCommandWithLogger(host.IP, cmd, logger)
}

func replaceServicePlaceholders(host string, logger *Logger) error {
	// Replace placeholders in service file
	placeholders := map[string]string{
		"{{INSTALL_PREFIX}}": deployConfig.InstallPrefix,
		"{{DATA_DIR}}":       deployConfig.DataDir,
		"{{MYSQL_PORT}}":     fmt.Sprintf("%d", deployConfig.MySQLPort),
	}

	for placeholder, value := range placeholders {
		replaceCmd := fmt.Sprintf("sed -i 's|%s|%s|g' /etc/systemd/system/mysqld.service", placeholder, value)
		if err := executeSSHCommandWithLogger(host, replaceCmd, logger); err != nil {
			return err
		}
	}

	return nil
}

// getMySQLInitializeCheckCmd 返回检查MySQL数据库是否已初始化的命令
func getMySQLInitializeCheckCmd(dataDir string) string {
	return fmt.Sprintf(`test -f "%s/ibdata1" && echo "initialized" || echo "not_initialized"`, dataDir)
}
