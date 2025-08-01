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
	"bytes"
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
	"text/template"
	"time"

	"github.com/logrusorgru/aurora"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

// Constants for configuration
const (
	// Default values
	DefaultInstallDir = "/usr/local"
	DefaultDataDir    = "/data/mysql"
	DefaultMySQLPort  = 3306
	DefaultSSHPort    = 22
	DefaultSSHUser    = "root"
	DefaultMGRPort    = 33061
	DownloadMysqlUrl  = "https://dev.mysql.com/get/Downloads/MySQL-8.0"

	// Timeouts
	SSHTimeout        = 5
	MySQLStartTimeout = 60
	ServiceTimeout    = 30

	// Architecture types
	ArchMasterSlave = "master-slave"
	ArchMGR         = "mgr"
	ArchPXC         = "pxc"

	// File patterns
	MySQLPackagePatternMinimal = "mysql-%s-linux-glibc2.28-x86_64-minimal.tar.xz"
	MySQLPackagePatternFull    = "mysql-%s-linux-glibc2.28-x86_64.tar.xz"
	LogFilePattern             = "mysql_deploy_%s.log"
	BackupDirPattern           = "%s_backup_%s"
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
	SSHKeyPath      string
	MinimalPackage  bool   // 是否使用minimal安装包
	AppUser         string // 应用账号用户名
	AppPass         string // 应用账号密码
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
	Short: "Automatically deploy MySQL service with intelligent architecture detection",
	Long: `Automatically deploy MySQL service with the following features:
- Automatically download MySQL installation package of specified version
- Detect master-slave replication and MGR architecture, support PXC cluster deployment, support ProxySQL deployment`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := main(); err != nil {
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

	flags.StringVarP(&deployConfig.MysqlVersion, "version", "v", "8.0.42", "MySQL version")
	flags.StringVarP(&deployConfig.InstallPrefix, "install-dir", "i", DefaultInstallDir, "MySQL installation directory prefix")
	flags.StringVarP(&deployConfig.DataDir, "datadir", "d", DefaultDataDir, "MySQL data directory")
	flags.StringVarP(&deployConfig.ReplicationUser, "repl-user", "r", "repl", "Replication account username")
	flags.StringVarP(&deployConfig.ReplicationPass, "repl-pass", "", "repl123", "Replication account password")
	flags.StringVarP(&deployConfig.AdminUser, "admin-user", "a", "admin", "Admin account username")
	flags.StringVarP(&deployConfig.AdminPass, "admin-pass", "", "", "Admin account password (auto-generated if empty)")
	flags.StringVarP(&deployConfig.RootPass, "root-pass", "", "", "Root password (auto-generated if empty)")
	flags.StringVarP(&deployConfig.MyCnfTemplate, "config-template", "c", "my.cnf.template", "my.cnf configuration template file path")
	flags.StringVarP(&deployConfig.ServiceTemplate, "service-template", "s", "mysql.service.template", "systemd service template file path")
	flags.StringVarP(&deployConfig.HostFile, "host-file", "f", "hosts.txt", "Host list file")
	flags.StringVarP(&deployConfig.SSHUser, "ssh-user", "", DefaultSSHUser, "SSH username")
	flags.IntVarP(&deployConfig.SSHPort, "ssh-port", "", DefaultSSHPort, "SSH port")
	flags.IntVarP(&deployConfig.MySQLPort, "mysql-port", "", DefaultMySQLPort, "MySQL service port")
	flags.StringVarP(&deployConfig.MGRGroupName, "mgr-group-name", "", "", "MGR group name (UUID format, auto-generated if empty)")
	flags.BoolVarP(&deployConfig.ForceDownload, "force-download", "", false, "Force re-download MySQL installation package")
	flags.BoolVarP(&deployConfig.ForceDeploy, "force-deploy", "", false, "Force deployment, stop running MySQL service")
	flags.StringVarP(&deployConfig.SSHKeyPath, "ssh-key", "", os.Getenv("HOME")+"/.ssh/id_rsa", "SSH private key path")
	flags.BoolVarP(&deployConfig.MinimalPackage, "minimal", "m", false, "Use minimal installation package (default: no, use full version)")
	flags.StringVar(&deployConfig.AppUser, "app-user", "appuser", "Application account username (ProxySQL backend application account)")
	flags.StringVar(&deployConfig.AppPass, "app-pass", "", "Application account password (auto-generated if empty)")
}

// main is the main deployment function
func main() error {
	// Initialize passwords if not provided
	if deployConfig.AdminPass == "" {
		deployConfig.AdminPass = generateRandomPassword()
	}
	if deployConfig.RootPass == "" {
		deployConfig.RootPass = generateRandomPassword()
	}
	if deployConfig.AppPass == "" {
		deployConfig.AppPass = generateRandomPassword()
	}

	// Phase 1: Preflight checks
	printPhase("前置检查", 1)
	hosts, err := runPreflightChecks()
	if err != nil {
		return fmt.Errorf("前置检查失败: %w", err)
	}

	// 过滤出 MySQL 节点
	mysqlHosts := filterMySQLHosts(hosts)

	if deployConfig.Architecture == ArchPXC {
		printPhase("PXC集群部署", 2)
		if err := deployPXC(mysqlHosts, nil); err != nil {
			return err
		}

		var proxysqlHosts []HostInfo
		for _, h := range hosts {
			if strings.ToLower(h.Role) == "proxysql" {
				proxysqlHosts = append(proxysqlHosts, h)
			}
		}
		if len(proxysqlHosts) > 0 {
			printPhase("ProxySQL部署", 5)
			for i, px := range proxysqlHosts {
				printHostProgress(px.IP, i+1, len(proxysqlHosts))
				if err := deployProxySQL(px, mysqlHosts, nil); err != nil {
					fmt.Printf("%s %s: %v\n",
						aurora.Red(fmt.Sprintf("[%s]:", px.IP)),
						aurora.Red("ProxySQL部署失败"), err)
					continue
				}
				printSuccess(fmt.Sprintf("ProxySQL部署完成! 访问地址: mysql -u%s -p%s -h%s -P6033", deployConfig.AppUser, deployConfig.AppPass, px.IP), "")
			}
		}
	} else {
		// Phase 2: Preparation
		printPhase("准备阶段", 2)
		var mysqlPackage string
		mysqlPackage, err = downloadPackage(deployConfig.MysqlVersion, deployConfig.MinimalPackage)
		if err != nil {
			return fmt.Errorf("下载MySQL失败: %w", err)
		}

		// Phase 3: Deployment
		printPhase("部署阶段", 3)
		if err := deployMySQL(mysqlHosts, mysqlPackage, nil); err != nil {
			return fmt.Errorf("部署阶段失败: %w", err)
		}

		// Phase 4: Architecture configuration
		printPhase("架构配置", 4)
		if deployConfig.Architecture == ArchMasterSlave {
			return configureMasterSlave(mysqlHosts, nil)
		}

		if deployConfig.Architecture == ArchMGR {
			return configureMGR(mysqlHosts, nil)
		}
	}

	firstHost := mysqlHosts[0]
	fmt.Printf("在节点 %s 上配置管理用户和密码\n", firstHost.IP)
	if err := createAdminUser(firstHost.IP, nil); err != nil {
		return fmt.Errorf("在 %s 上配置用户失败: %w", firstHost.IP, err)
	}

	printSuccess("部署完成!", "")
	displayConfig()
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
		aurora.Blue(fmt.Sprintf("%d/%d", len(steps)+1, len(steps)+1)), aurora.Green(deployConfig.Architecture))
	return hosts, nil
}

// runDeployment deploys MySQL to all hosts
func deployMySQL(hosts []HostInfo, mysqlPackage string, _ interface{}) error {
	for i, host := range hosts {
		printHostProgress(host.IP, i+1, len(hosts))

		if err := deployToHost(host, mysqlPackage, nil); err != nil {
			fmt.Printf("%s %s: %v\n",
				aurora.Red(fmt.Sprintf("[%s]:", host.IP)),
				aurora.Red("部署失败"), err)
			continue
		}
		fmt.Printf("%s %s\n",
			aurora.Green(fmt.Sprintf("[%s]:", host.IP)),
			aurora.Green("部署完成!"))
	}
	return nil
}

// Utility functions for output formatting
func printPhase(name string, phase int) {
	fmt.Printf("%s\n", aurora.Blue(fmt.Sprintf("=== 阶段%d: %s ===", phase, name)))
}

func printStep(name string, current, total int) {
	fmt.Printf("%s %s\n", aurora.Blue(fmt.Sprintf("%d/%d", current, total)), name)
}

func printSuccess(title, message string) {
	fmt.Printf("%s %s\n", aurora.Green("✓"), aurora.Green(title))
	if message != "" {
		fmt.Printf("%s\n", message)
	}
}

func printHostProgress(host string, current, total int) {
	fmt.Printf("%s %s [%d/%d] %s\n",
		aurora.Green(fmt.Sprintf("[%s]:", host)),
		aurora.Blue("部署进度"), current, total,
		strings.Repeat("=", 40))
}

// SSH and remote command execution utilities
func executeSSHCommand(host, command string, streamOutput bool) (string, error) {
	fmt.Printf("%s %s\n", aurora.Blue(fmt.Sprintf("[%s]:", host)), command) // 每次都打印命令
	client, err := NewSSHClient(
		fmt.Sprintf("%s:%d", host, deployConfig.SSHPort),
		deployConfig.SSHUser,
		deployConfig.SSHKeyPath,
	)
	if err != nil {
		return "", err
	}
	defer client.Close()

	if streamOutput {
		// 实时输出到本地终端
		err := client.RunWithStream(command, os.Stdout)
		return "", err
	} else {
		output, err := client.Run(command)
		if err != nil {
			fmt.Printf("%s %s\n", aurora.Red("命令执行失败:"), output)
		}
		return strings.TrimSpace(output), err
	}
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
	if roleCount["pxc"] > 0 {
		return ArchPXC
	}
	if roleCount["mgr"] > 0 {
		return ArchMGR
	}
	if roleCount["master"] > 0 && roleCount["slave"] > 0 {
		return ArchMasterSlave
	}
	return "single"
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
	_, err := executeSSHCommand(host, "echo", false)
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
		return fmt.Errorf("检测到MySQL正在运行的主机: %v，可使用 --force-deploy 强制部署", runningHosts)
	}

	if len(runningHosts) > 0 {
		fmt.Printf("    %s 检测到 --force-deploy 参数，将强制停止现有MySQL服务\n",
			aurora.Yellow("警告:"))
	}

	return nil
}

func checkMySQLRunning(host string) (bool, error) {
	// Check systemd status first
	if output, err := executeSSHCommand(host, "systemctl is-active mysql 2>/dev/null", false); err == nil {
		return strings.TrimSpace(output) == "active", nil
	}

	// Fallback to process check
	if output, err := executeSSHCommand(host, "pgrep -f mysqld 2>/dev/null", false); err == nil {
		return len(strings.TrimSpace(output)) > 0, nil
	}

	return false, nil
}

// Download and package management
func downloadPackage(version string, minimal bool) (string, error) {
	var fileName string
	if minimal {
		fileName = fmt.Sprintf(MySQLPackagePatternMinimal, version)
	} else {
		fileName = fmt.Sprintf(MySQLPackagePatternFull, version)
	}

	filePath := filepath.Join(".", fileName)
	if !deployConfig.ForceDownload && fileExists(filePath) {
		fmt.Printf("%s %s\n", aurora.Blue("使用已存在的安装包:"), filePath)
		return filePath, nil
	}
	fullUrl := fmt.Sprintf("%s/%s", DownloadMysqlUrl, fileName)
	fmt.Printf("%s %s\n", aurora.Blue("开始下载安装包:"), fullUrl)
	return downloadFile(fullUrl, filePath)
}

// Utility functions
// downloadFile 下载文件到本地，带进度显示
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

	var bar *progressbar.ProgressBar
	if resp.ContentLength > 0 {
		bar = progressbar.DefaultBytes(
			resp.ContentLength,
			"下载进度",
		)
	}
	io.Copy(io.MultiWriter(out, bar), resp.Body)
	bar.Finish()

	// Verify downloaded file
	if fileInfo, err := os.Stat(filePath); err == nil {
		fmt.Printf("%s 文件大小: %s\n",
			aurora.Green("下载完成:"), byteHumen(fileInfo.Size()))
	}

	return filePath, nil
}

func generateRandomPassword() string {
	bytes := make([]byte, 32)
	rand.Read(bytes)
	return base64.StdEncoding.EncodeToString(bytes)[:24]
}

func displayConfig() {
	fmt.Printf("    Root密码: %s\n", deployConfig.RootPass)
	fmt.Printf("    管理员密码: %s\n", deployConfig.AdminPass)
	fmt.Printf("    应用账号: %s\n", deployConfig.AppUser)
	fmt.Printf("    应用密码: %s\n", deployConfig.AppPass)
}

// Host deployment functions
// 修改 deployToHost 签名，增加 pkgType 参数
func deployToHost(host HostInfo, mysqlPackage string, _ interface{}) error {
	steps := []struct {
		name string
		fn   func() error
	}{
		{"检查部署状态", func() error { return checkDeploymentStatus(host) }},
		{"环境准备", func() error { return prepareEnvironment(host.IP) }},
		{"安装依赖", func() error { return installDependencies(host.IP) }},
		{"文件操作", func() error { return handleFileOperations(host, mysqlPackage) }},
		{"初始化数据库", func() error { return configureMySQLInstance(host) }},
		{"服务配置", func() error { return configureAndStartService(host.IP) }},
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
func checkDeploymentStatus(host HostInfo) error {
	fmt.Printf("%s 检查MySQL完整部署状态\n", aurora.Blue(fmt.Sprintf("[%s]:", host.IP)))

	mysqlDir := getInstallDirName(deployConfig.MysqlVersion, deployConfig.MinimalPackage)

	// Check if MySQL is installed
	checkInstallCmd := fmt.Sprintf("test -d %s/%s && test -L %s/mysql && echo 'installed' || echo 'not installed'",
		deployConfig.InstallPrefix, mysqlDir, deployConfig.InstallPrefix)

	output, err := executeSSHCommand(host.IP, checkInstallCmd, false)
	if err != nil || strings.TrimSpace(output) != "installed" {
		fmt.Printf("%s  MySQL未安装或符号链接缺失\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)))
		return nil // Not an error, just not installed
	}

	// Check if database is initialized
	checkInitCmd := fmt.Sprintf(`test -f "%s/ibdata1" && echo "initialized" || echo "not_initialized"`, deployConfig.DataDir)

	output, err = executeSSHCommand(host.IP, checkInitCmd, false)
	if err != nil || strings.TrimSpace(output) != "initialized" {
		fmt.Printf("%s  数据库未初始化\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)))
		return nil
	}

	// Check systemd service
	output, err = executeSSHCommand(host.IP, "test -f /etc/systemd/system/mysqld.service && echo 'exists' || echo 'not exists'", false)
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

	if !deployConfig.ForceDeploy {
		return fmt.Errorf("MySQL已完整部署，如需重新部署请使用 --force-deploy")
	}

	return nil
}

func prepareEnvironment(host string) error {
	// Stop existing MySQL service if force deploy
	if deployConfig.ForceDeploy {
		fmt.Printf("%s   强制停止MySQL服务\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)))
		executeSSHCommand(host, "pkill mysqld* 2>/dev/null", false)

		// Wait for service to stop completely
		fmt.Printf("%s   等待MySQL服务完全停止\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
		time.Sleep(3 * time.Second)
	}

	// Clean old installation directories
	fmt.Printf("%s   清理软链接目录\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
	executeSSHCommand(host, fmt.Sprintf("rm -f %s/mysql", deployConfig.InstallPrefix), false)

	// Force deploy mode: clean data directory
	if deployConfig.ForceDeploy {
		fmt.Printf("%s   强制清理数据目录\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)))

		// Backup data directory if exists and not empty
		backupDataDir := fmt.Sprintf(BackupDirPattern, deployConfig.DataDir, time.Now().Format("20060102_150405"))
		checkDataCmd := fmt.Sprintf("if [ -d \"%s\" ] && [ \"$(ls -A %s 2>/dev/null)\" ]; then mv %s %s; fi",
			deployConfig.DataDir, deployConfig.DataDir, deployConfig.DataDir, backupDataDir)
		executeSSHCommand(host, checkDataCmd, false)

		// Clean systemd service files
		fmt.Printf("%s   清理systemd服务文件\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
		executeSSHCommand(host, "rm -f /etc/systemd/system/mysql.service", false)

		executeSSHCommand(host, "> /var/log/mysql* && chown -R mysql:mysql /var/log/mysql*", false)
	}

	// Create necessary directories
	fmt.Printf("%s   创建必要目录\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
	executeSSHCommand(host, fmt.Sprintf("mkdir -p %s %s", deployConfig.InstallPrefix, deployConfig.DataDir), false)

	// Create mysql user (idempotent)
	fmt.Printf("%s   检查并创建mysql用户\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
	executeSSHCommand(host, "id mysql >/dev/null 2>&1 || useradd mysql -d /dev/null -s /bin/false", false)

	// 修改主机名，格式为主机IP，点换成横线
	newHostname := strings.ReplaceAll(host, ".", "-")
	fmt.Printf("%s   修改主机名为: %s\n", aurora.Blue(fmt.Sprintf("[%s]:", host)), newHostname)
	setHostnameCmd := fmt.Sprintf("hostnamectl set-hostname %s", newHostname)
	executeSSHCommand(host, setHostnameCmd, false)

	return nil
}

func installDependencies(host string) error {
	fmt.Printf("%s 安装依赖\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
	executeSSHCommand(host, "yum install -y libaio libaio-devel numactl-libs ncurses-compat-libs", false)

	// Verify key dependencies
	fmt.Printf("%s 验证依赖安装\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
	executeSSHCommand(host, "ldconfig -p | grep libaio", false)

	return nil
}

func handleFileOperations(host HostInfo, mysqlPackage string) error {
	// Copy configuration file - now required, no default generation
	if err := copyFileToRemote(host.IP, deployConfig.MyCnfTemplate, "/etc/my.cnf", false); err != nil {
		return fmt.Errorf("复制配置文件失败: %w", err)
	}

	// Update placeholders in configuration file
	if err := updateConfigPlaceholders(host); err != nil {
		return fmt.Errorf("更新配置占位符失败: %w", err)
	}

	// Copy and extract MySQL package
	if err := copyAndExtractMySQL(host.IP, mysqlPackage); err != nil {
		return err
	}

	// Create symbolic link
	mysqlDir := getInstallDirName(deployConfig.MysqlVersion, deployConfig.MinimalPackage)
	if _, err := executeSSHCommand(host.IP, fmt.Sprintf("cd %s && ln -sf %s mysql", deployConfig.InstallPrefix, mysqlDir), false); err != nil {
		return err
	}

	return nil
}

func copyFileToRemote(host, localFile, remoteFile string, stream bool) error {
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
	if err != nil {
		return fmt.Errorf("文件复制失败: %w", err)
	}
	return nil
}

func updateConfigPlaceholders(host HostInfo) error {
	fmt.Printf("%s   更新配置文件占位符\n", aurora.Blue(fmt.Sprintf("[%s]:", host.IP)))

	// Replace placeholders in my.cnf
	placeholders := map[string]string{
		"{{SERVER_ID}}":      fmt.Sprintf("%d", host.ServerID),
		"{{MYSQL_PORT}}":     fmt.Sprintf("%d", deployConfig.MySQLPort),
		"{{DATA_DIR}}":       deployConfig.DataDir,
		"{{INSTALL_PREFIX}}": deployConfig.InstallPrefix,
	}

	for placeholder, value := range placeholders {
		replaceCmd := fmt.Sprintf("sed -i 's|%s|%s|g' /etc/my.cnf", placeholder, value)
		if _, err := executeSSHCommand(host.IP, replaceCmd, false); err != nil {
			return err
		}
	}

	return nil
}

func copyAndExtractMySQL(host, mysqlPackage string) error {
	fmt.Printf("%s 检查MySQL安装状态\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))

	remoteFile := fmt.Sprintf("/tmp/%s", filepath.Base(mysqlPackage))
	mysqlDir := getInstallDirName(deployConfig.MysqlVersion, deployConfig.MinimalPackage)

	// Check if MySQL is already extracted
	checkDirCmd := fmt.Sprintf("test -d %s/%s && echo 'extracted' || echo 'not extracted'", deployConfig.InstallPrefix, mysqlDir)
	output, err := executeSSHCommand(host, checkDirCmd, false)

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
	if remoteOutput, err := executeSSHCommand(host, fmt.Sprintf("stat -c%%s %s 2>/dev/null || echo '0'", remoteFile), false); err == nil {
		if remoteSize, parseErr := strconv.ParseInt(strings.TrimSpace(remoteOutput), 10, 64); parseErr == nil && remoteSize == localSize && remoteSize > 0 {
			fmt.Printf("%s 远程安装包已存在且完整，跳过传输\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
			needCopy = false
		}
	}

	if needCopy {
		fmt.Printf("%s 复制MySQL安装包到远程主机\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
		if err := copyFileToRemote(host, mysqlPackage, remoteFile, false); err != nil {
			return err
		}
	}

	// Extract package
	return extractMySQLPackage(host, remoteFile, deployConfig.InstallPrefix)
}

func extractMySQLPackage(host, packageFile, targetDir string) error {
	fmt.Printf("%s 解压MySQL安装包\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))

	// Use tar -J parameter to extract .tar.xz files
	extractCmd := fmt.Sprintf("cd %s && tar -xf %s", targetDir, packageFile)
	if _, err := executeSSHCommand(host, extractCmd, false); err != nil {
		return fmt.Errorf("解压失败: %w", err)
	}

	mysqlDir := getInstallDirName(deployConfig.MysqlVersion, deployConfig.MinimalPackage)
	checkCmd := fmt.Sprintf("ls -la %s/%s/bin/mysqld", targetDir, mysqlDir)
	if _, err := executeSSHCommand(host, checkCmd, false); err != nil {
		return fmt.Errorf("解压后验证失败，未找到MySQL可执行文件")
	}

	fmt.Printf("%s 解压完成\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
	return nil
}

func configureMySQLInstance(host HostInfo) error {
	// Add PATH environment variable (idempotent)
	pathCheckCmd := fmt.Sprintf("grep -q '%s/mysql/bin' /etc/bashrc || echo 'export PATH=$PATH:%s/mysql/bin' >> /etc/bashrc", deployConfig.InstallPrefix, deployConfig.InstallPrefix)
	if _, err := executeSSHCommand(host.IP, pathCheckCmd, false); err != nil {
		return err
	}

	// Check and initialize database if needed
	if err := initializeDatabaseIfNeeded(host.IP); err != nil {
		return err
	}

	// Set data directory permissions
	if _, err := executeSSHCommand(host.IP, fmt.Sprintf("chown -R mysql:mysql %s", deployConfig.DataDir), false); err != nil {
		return err
	}

	return nil
}

func initializeDatabaseIfNeeded(host string) error {
	checkInitCmd := fmt.Sprintf(`test -f "%s/ibdata1" && echo "initialized" || echo "not_initialized"`, deployConfig.DataDir)

	output, err := executeSSHCommand(host, checkInitCmd, false)
	initStatus := strings.TrimSpace(output)

	if err == nil && initStatus == "initialized" {
		fmt.Printf("%s   数据库已初始化，跳过初始化步骤\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
		return nil
	}

	// Initialize database
	fmt.Printf("%s   初始化MySQL数据库\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))

	// Ensure data directory exists and is empty
	mkdirCmd := fmt.Sprintf("mkdir -p %s", deployConfig.DataDir)
	executeSSHCommand(host, mkdirCmd, false)

	// Check if data directory is empty
	checkEmptyCmd := fmt.Sprintf("ls -A %s | wc -l", deployConfig.DataDir)
	if emptyOutput, err := executeSSHCommand(host, checkEmptyCmd, false); err == nil {
		if fileCount := strings.TrimSpace(emptyOutput); fileCount != "0" {
			fmt.Printf("%s   数据目录不为空，清理旧数据\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)))
			rmCmd := fmt.Sprintf("rm -rf %s/*", deployConfig.DataDir)
			executeSSHCommand(host, rmCmd, false)
		}
	}

	// 初始化数据库
	cmd := fmt.Sprintf("mysqld --initialize-insecure --user=mysql --datadir=%s", deployConfig.DataDir)
	if _, err := executeSSHCommand(host, cmd, false); err != nil {
		return err
	}

	// Generate SSL certificates
	if deployConfig.Architecture != ArchPXC {
		if err := generateSSLCertificates(host); err != nil {
			fmt.Printf("%s   SSL证书生成失败: %v，但继续执行\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)), err)
		} else {
			fmt.Printf("%s   SSL证书生成完成\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
		}
	}
	fmt.Printf("%s   数据库初始化完成\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
	return nil
}

// 清理二进制日志
func resetBinlog(host string) error {
	var major, minor int
	var resetSQL string
	fmt.Sscanf(deployConfig.MysqlVersion, "%d.%d", &major, &minor)
	if major == 8 && minor >= 4 {
		resetSQL = "RESET BINARY LOGS AND GTIDS;"
	} else {
		resetSQL = "RESET MASTER;"
	}
	fmt.Printf("%s   执行%s清理二进制日志\n", aurora.Blue(fmt.Sprintf("[%s]:", host)), resetSQL)
	if _, err := runMySQLCommand(host, resetSQL); err != nil {
		return err
	}
	return nil
}

func generateSSLCertificates(host string) error {
	// 8.0.x 执行 mysql_ssl_rsa_setup，8.4及以上不再执行
	ver := deployConfig.MysqlVersion
	major, minor, patch := 0, 0, 0
	fmt.Sscanf(ver, "%d.%d.%d", &major, &minor, &patch)

	if major == 8 && minor == 0 {
		// 8.0.x 需要手动生成
		sslSetupCmd := fmt.Sprintf("mysql_ssl_rsa_setup --datadir=%s --uid=mysql",
			deployConfig.DataDir)
		if _, err := executeSSHCommand(host, sslSetupCmd, false); err != nil {
			return fmt.Errorf("SSL证书生成失败: %w", err)
		}

		chownCmd := fmt.Sprintf("chown mysql:mysql %s/*.pem", deployConfig.DataDir)
		if _, err := executeSSHCommand(host, chownCmd, false); err != nil {
			return fmt.Errorf("设置SSL证书权限失败: %w", err)
		}
	}
	return nil
}

func configureAndStartService(host string) error {
	// Create systemd service file
	if err := createMySQLSystemdService(host); err != nil {
		return err
	}

	// Enable MySQL service
	executeSSHCommand(host, "systemctl daemon-reload && systemctl enable mysql", false)

	// Start MySQL service
	if err := startMySQLService(host); err != nil {
		return err
	}

	// Wait and verify service startup
	if err := waitForMySQLStart(host); err != nil {
		return err
	}

	return nil
}

func createMySQLSystemdService(host string) error {
	// Check if systemd service file already exists
	output, err := executeSSHCommand(host, "test -f /etc/systemd/system/mysql.service && echo 'exists' || echo 'not exists'", false)

	if err == nil && strings.TrimSpace(output) == "exists" {
		fmt.Printf("%s systemd服务文件已存在，跳过创建\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
		return nil
	}

	fmt.Printf("%s 使用模板创建systemd服务文件\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))

	// Copy service template file to remote host
	if err := copyFileToRemote(host, deployConfig.ServiceTemplate, "/etc/systemd/system/mysql.service", false); err != nil {
		return fmt.Errorf("复制systemd服务文件失败: %w", err)
	}

	// Replace placeholders in service file
	if err := replaceServicePlaceholders(host); err != nil {
		return fmt.Errorf("替换服务文件占位符失败: %w", err)
	}

	fmt.Printf("%s systemd服务文件创建完成\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
	return nil
}

func startMySQLService(host string) error {
	// Check if MySQL service is already running
	if output, err := executeSSHCommand(host, "systemctl is-active mysql 2>/dev/null", false); err == nil {
		if strings.TrimSpace(output) == "active" {
			fmt.Printf("%s MySQL服务已在运行，跳过启动\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
			return nil
		}
	}

	// Start MySQL service using systemd
	fmt.Printf("%s 使用systemd启动MySQL服务\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))
	if _, err := executeSSHCommand(host, "systemctl start mysql", false); err != nil {
		fmt.Printf("%s MySQL启动命令失败: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", host)), err)
		return nil // Don't block, continue to check status
	}

	fmt.Printf("%s MySQL服务启动命令执行完成\n", aurora.Green(fmt.Sprintf("[%s]:", host)))
	return nil
}

func waitForMySQLStart(host string) error {
	fmt.Printf("%s 等待MySQL服务启动\n", aurora.Blue(fmt.Sprintf("[%s]:", host)))

	maxRetries := 12 // Wait up to 60 seconds (12 * 5 seconds)
	for i := 0; i < maxRetries; i++ {
		// Check MySQL process
		if output, err := executeSSHCommand(host, "pgrep -f mysqld > /dev/null 2>&1 && echo 'running' || echo 'not running'", false); err == nil && strings.Contains(output, "running") {
			fmt.Printf("%s MySQL进程检查: 运行中\n", aurora.Green(fmt.Sprintf("[%s]:", host)))

			// Check if MySQL port is listening
			portCheckCmd := fmt.Sprintf("netstat -tlnp 2>/dev/null | grep :%d", deployConfig.MySQLPort)
			if portOutput, portErr := executeSSHCommand(host, portCheckCmd, false); portErr == nil && len(portOutput) > 0 {
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

func createAdminUser(host string, _ interface{}) error {
	adminUserReq := UserCreateRequest{
		Username:    deployConfig.AdminUser,
		Password:    deployConfig.AdminPass,
		Host:        "%",
		Privileges:  []string{"ALL ON *.*"},
		RequireSSL:  false,
		Description: "Admin account",
	}

	if err := createMySQLUser(host, adminUserReq); err != nil {
		return err
	}

	setRootPassCmd := fmt.Sprintf("ALTER USER 'root'@'localhost' IDENTIFIED BY '%s';", deployConfig.RootPass)
	if _, err := runMySQLCommand(host, setRootPassCmd); err != nil {
		return fmt.Errorf("设置root密码失败: %w", err)
	}

	return nil
}

func createMySQLUser(host string, userReq UserCreateRequest) error {
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
		if _, err := runMySQLCommand(host, cmd); err != nil {
			return fmt.Errorf("创建用户 %s 失败: %w", userReq.Username, err)
		}
	}

	fmt.Printf("%s   用户 %s@%s 创建完成\n",
		aurora.Green(fmt.Sprintf("[%s]:", host)),
		userReq.Username, userReq.Host)

	return nil
}

func runMySQLCommand(host, sqlCommand string) (string, error) {
	cmd := fmt.Sprintf("mysql -uroot -h127.0.0.1 -N -e \"%s\"", sqlCommand)
	output, err := executeSSHCommand(host, cmd, false)
	return strings.TrimSpace(output), err
}

func configureMasterSlave(hosts []HostInfo, _ interface{}) error {
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
		Description: "Master-slave replication account",
	}

	if err := createMySQLUser(masterHost.IP, replUserReq); err != nil {
		return fmt.Errorf("创建复制用户失败: %w", err)
	}

	// 创建完所有用户后，统一清理binlog
	resetBinlog(masterHost.IP)

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

		if _, err := runMySQLCommand(slave.IP, changeSQL); err != nil {
			fmt.Printf("%s 配置从节点失败: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", slave.IP)), err)
		} else {
			fmt.Printf("%s 主从复制配置完成\n", aurora.Green(fmt.Sprintf("[%s]:", slave.IP)))
		}
	}

	return nil
}

func configureMGR(hosts []HostInfo, _ interface{}) error {
	fmt.Printf("\n%s\n", aurora.Green("=== 配置MGR集群 ==="))

	// Generate or use specified MGR group name
	if deployConfig.MGRGroupName == "" {
		deployConfig.MGRGroupName = generateMGRGroupName()
		fmt.Printf("自动生成MGR组名: %s\n", deployConfig.MGRGroupName)
	}

	// Create replication users for all nodes
	fmt.Printf("%s 创建复制用户\n", aurora.Blue("步骤2:"))
	for _, host := range hosts {
		if err := createMGRReplicationUser(host.IP); err != nil {
			fmt.Printf("%s 创建复制用户失败: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)), err)
			return err
		}
	}

	// 创建完所有用户后，统一清理binlog（对所有节点都执行一次）
	for _, host := range hosts {
		resetBinlog(host.IP)
	}

	// Build cluster address list
	groupSeeds := buildMGRGroupSeeds(hosts)
	fmt.Printf("%s 集群地址列表: %s\n", aurora.Blue("步骤3:"), groupSeeds)

	// Configure MGR parameters and start cluster
	fmt.Printf("%s 配置并启动MGR集群\n", aurora.Blue("步骤4:"))
	for i, host := range hosts {
		isBootstrap := (i == 0)
		if err := configureMGRNode(host, groupSeeds, isBootstrap); err != nil {
			fmt.Printf("%s MGR节点配置失败: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)), err)
			return err
		}
	}

	// 检查每个节点MGR状态
	fmt.Printf("\n%s\n", aurora.Blue("=== 检查MGR节点状态 ==="))
	checkMGROnlineStatus(hosts)

	return nil
}

// 检查每个节点MGR状态是否为ONLINE
func checkMGROnlineStatus(hosts []HostInfo) {
	for _, host := range hosts {
		fmt.Printf("%s 检查MGR状态...\n", aurora.Blue(fmt.Sprintf("[%s]:", host.IP)))
		query := "SELECT MEMBER_HOST, MEMBER_STATE FROM performance_schema.replication_group_members;"
		cmd := fmt.Sprintf("echo \"%s\" | %s/mysql/bin/mysql -uroot -p%s -N -e \"%s\"", query, deployConfig.InstallPrefix, deployConfig.RootPass, query)
		output, err := executeSSHCommand(host.IP, cmd, false)
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

func createMGRReplicationUser(host string) error {
	replUserReq := UserCreateRequest{
		Username:    deployConfig.ReplicationUser,
		Password:    deployConfig.ReplicationPass,
		Host:        "%",
		Privileges:  []string{"BACKUP_ADMIN, REPLICATION SLAVE ON *.*"},
		RequireSSL:  true,
		Description: "MGR replication account",
	}

	return createMySQLUser(host, replUserReq)
}

func buildMGRGroupSeeds(hosts []HostInfo) string {
	var groupSeeds []string
	for _, host := range hosts {
		groupSeeds = append(groupSeeds, fmt.Sprintf("%s:%d", host.IP, deployConfig.MGRPort))
	}
	return strings.Join(groupSeeds, ",")
}

func configureMGRNode(host HostInfo, groupSeeds string, isBootstrap bool) error {
	// 1. Write MGR config to my.cnf first
	if err := writeMGRConfigToMyCnf(host, groupSeeds); err != nil {
		return fmt.Errorf("写入MGR参数到my.cnf失败: %w", err)
	}

	// 2. Restart MySQL service to apply config
	fmt.Printf("%s Restarting MySQL service to apply MGR config...", aurora.Blue(fmt.Sprintf("[%s]:", host.IP)))
	restartCmd := "systemctl restart mysqld"
	if _, err := executeSSHCommand(host.IP, restartCmd, false); err != nil {
		return fmt.Errorf("重启MySQL服务失败: %w", err)
	}

	// 3. Wait for MySQL to start
	if err := waitForMySQLStart(host.IP); err != nil {
		return fmt.Errorf("MySQL启动失败: %w", err)
	}

	// 4. Set bootstrap if needed, then start group replication
	if isBootstrap {
		if _, err := runMySQLCommand(host.IP, "SET GLOBAL group_replication_bootstrap_group=ON;"); err != nil {
			return fmt.Errorf("设置bootstrap模式失败: %w", err)
		}
	}

	changeMasterCmd := fmt.Sprintf("CHANGE MASTER TO MASTER_USER='%s',MASTER_PASSWORD='%s' FOR CHANNEL 'group_replication_recovery';",
		deployConfig.ReplicationUser, deployConfig.ReplicationPass)
	if _, err := runMySQLCommand(host.IP, changeMasterCmd); err != nil {
		return fmt.Errorf("配置MGR复制通道失败: %w", err)
	}

	if _, err := runMySQLCommand(host.IP, "START GROUP_REPLICATION;"); err != nil {
		return fmt.Errorf("启动组复制失败: %w", err)
	}

	// Turn off bootstrap mode after first node
	if isBootstrap {
		time.Sleep(3 * time.Second)
		if _, err := runMySQLCommand(host.IP, "SET GLOBAL group_replication_bootstrap_group=OFF;"); err != nil {
			return fmt.Errorf("关闭bootstrap模式失败: %w", err)
		}
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

func writeMGRConfigToMyCnf(host HostInfo, groupSeeds string) error {
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
	_, err := executeSSHCommand(host.IP, cmd, false)
	return err
}

func replaceServicePlaceholders(host string) error {
	// Replace placeholders in service file
	placeholders := map[string]string{
		"{{INSTALL_PREFIX}}": deployConfig.InstallPrefix,
		"{{DATA_DIR}}":       deployConfig.DataDir,
		"{{MYSQL_PORT}}":     fmt.Sprintf("%d", deployConfig.MySQLPort),
	}

	for placeholder, value := range placeholders {
		replaceCmd := fmt.Sprintf("sed -i 's|%s|%s|g' /etc/systemd/system/mysql.service", placeholder, value)
		if _, err := executeSSHCommand(host, replaceCmd, false); err != nil {
			return err
		}
	}

	return nil
}

// getInstallDirName 返回解压出来的目录名（不带路径）
func getInstallDirName(version string, minimal bool) string {
	if minimal {
		return fmt.Sprintf(strings.TrimSuffix(MySQLPackagePatternMinimal, ".tar.xz"), version)
	} else {
		return fmt.Sprintf(strings.TrimSuffix(MySQLPackagePatternFull, ".tar.xz"), version)
	}
}

// writePXCConfigToMyCnf 追加PXC wsrep配置到my.cnf
func writePXCConfigToMyCnf(host HostInfo, wsrepClusterAddress string) error {
	// 1. 替换或插入 wsrep_provider_options
	replaceOrInsertCmd := "grep -q '^wsrep_provider_options' /etc/my.cnf " +
		"&& sed -i \"s|^wsrep_provider_options.*|wsrep_provider_options='gcache.size=1G;socket.ssl=off'|g\" /etc/my.cnf " +
		"|| echo \"wsrep_provider_options='gcache.size=1G;socket.ssl=no'\" >> /etc/my.cnf"
	if _, err := executeSSHCommand(host.IP, replaceOrInsertCmd, false); err != nil {
		return err
	}
	// 2. 替换 gcomm://
	replaceGcommCmd := fmt.Sprintf("sed -i \"s|gcomm://|gcomm://%s|g\" /etc/my.cnf", wsrepClusterAddress)
	if _, err := executeSSHCommand(host.IP, replaceGcommCmd, false); err != nil {
		return err
	}
	// 3. 追加 pxc_encrypt_cluster_traffic=off
	appendEncryptTrafficCmd := "echo 'pxc_encrypt_cluster_traffic=off' >> /etc/my.cnf"
	if _, err := executeSSHCommand(host.IP, appendEncryptTrafficCmd, false); err != nil {
		return err
	}
	// 4. 替换 wsrep_node_address
	replaceNodeAddrCmd := fmt.Sprintf(
		"sed -i 's|^[[:space:]]*#*[[:space:]]*wsrep_node_address.*|wsrep_node_address=%s|' /etc/my.cnf || echo 'wsrep_node_address=%s' >> /etc/my.cnf",
		host.IP, host.IP)
	if _, err := executeSSHCommand(host.IP, replaceNodeAddrCmd, false); err != nil {
		return err
	}
	// 5. 替换 wsrep_node_name
	wsrepNodeName := strings.ReplaceAll(host.IP, ".", "-")
	replaceNodeNameCmd := fmt.Sprintf("sed -i 's|^wsrep_node_name.*|wsrep_node_name=%s|' /etc/my.cnf || echo 'wsrep_node_name=%s' >> /etc/my.cnf", wsrepNodeName, wsrepNodeName)
	if _, err := executeSSHCommand(host.IP, replaceNodeNameCmd, false); err != nil {
		return err
	}
	return nil
}

// checkPXCClusterStatus 检查PXC集群状态
func checkPXCClusterStatus(hosts []HostInfo) {
	for _, host := range hosts {
		fmt.Printf("%s 检查PXC集群状态...\n", aurora.Blue(fmt.Sprintf("[%s]:", host.IP)))
		query := "SHOW STATUS LIKE 'wsrep_cluster_size';"
		output, err := runMySQLCommand(host.IP, query)
		if err != nil {
			fmt.Printf("%s 查询失败: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)), err)
			continue
		}
		fmt.Printf("%s %s\n", aurora.Green(fmt.Sprintf("[%s]:", host.IP)), output)
	}
}

// PXC独立部署主流程
func deployPXC(hosts []HostInfo, _ interface{}) error {

	var clusterIPs []string
	for _, h := range hosts {
		clusterIPs = append(clusterIPs, h.IP)
	}
	wsrepClusterAddress := strings.Join(clusterIPs, ",")

	// 逐台主机安装PXC
	for i, host := range hosts {
		printHostProgress(host.IP, i+1, len(hosts))
		if err := deployPXCToHost(host, wsrepClusterAddress); err != nil {
			fmt.Printf("%s %s: %v\n",
				aurora.Red(fmt.Sprintf("[%s]:", host.IP)),
				aurora.Red("PXC部署失败"), err)
			continue
		}
		fmt.Printf("%s %s\n",
			aurora.Green(fmt.Sprintf("[%s]:", host.IP)),
			aurora.Green("PXC节点部署完成!"))
	}

	// 集群引导与加入
	printPhase("PXC集群引导", 3)
	if err := bootstrapAndJoinPXCCluster(hosts); err != nil {
		return fmt.Errorf("PXC集群引导失败: %w", err)
	}

	return nil
}

// 单台主机PXC安装与初始化
func deployPXCToHost(host HostInfo, wsrepClusterAddress string) error {
	// 1. 准备环境
	if err := prepareEnvironment(host.IP); err != nil {
		return fmt.Errorf("PXC环境准备失败: %w", err)
	}

	// 2. 配置Percona官方yum源并安装
	cmds := []string{
		"yum module disable -y mysql",
		"yum install -y https://repo.percona.com/yum/percona-release-latest.noarch.rpm",
		"percona-release setup pxc-84-lts",
		"sed -i 's|http://repo.percona.com|https://mirrors.tuna.tsinghua.edu.cn/percona|g' /etc/yum.repos.d/percona-*.repo",
		"yum install -y percona-xtradb-cluster",
	}
	for _, cmd := range cmds {
		stream := (cmd == "yum install -y percona-xtradb-cluster")
		if _, err := executeSSHCommand(host.IP, cmd, stream); err != nil {
			return fmt.Errorf("PXC依赖安装失败: %w, cmd: %s", err, cmd)
		}
	}

	// 3. 追加PXC wsrep配置
	if err := writePXCConfigToMyCnf(host, wsrepClusterAddress); err != nil {
		return fmt.Errorf("写入PXC配置失败: %w", err)
	}

	// 4. 初始化数据目录
	if err := initializeDatabaseIfNeeded(host.IP); err != nil {
		return err
	}

	return nil
}

// PXC集群引导与加入
func bootstrapAndJoinPXCCluster(hosts []HostInfo) error {
	if len(hosts) == 0 {
		return fmt.Errorf("无PXC节点")
	}
	first := hosts[0]
	// 第一个节点引导集群
	fmt.Printf("%s PXC集群引导节点: %s\n", aurora.Blue("[PXC]"), first.IP)

	if _, err := executeSSHCommand(first.IP, "systemctl start mysql@bootstrap", false); err != nil {
		return err
	}
	// 等待第一个节点启动完成
	time.Sleep(15 * time.Second)

	// 其它节点依次加入
	for _, host := range hosts[1:] {
		fmt.Printf("%s PXC集群加入节点: %s\n", aurora.Blue("[PXC]"), host.IP)
		if _, err := executeSSHCommand(host.IP, "systemctl start mysql", false); err != nil {
			return err
		}
		time.Sleep(5 * time.Second)
	}
	// 检查集群状态
	checkPXCClusterStatus(hosts)
	return nil
}

// ProxySQL自动部署与配置
func deployProxySQL(host HostInfo, mysqlBackends []HostInfo, _ interface{}) error {
	// 清理旧的ProxySQL环境
	fmt.Printf("%s 清理旧的ProxySQL环境\n", aurora.Blue(fmt.Sprintf("[%s]:", host.IP)))
	cleanupCmds := []string{
		"systemctl stop proxysql || true",
		"yum remove -y proxysql2 || true",
		"rm -rf /etc/proxysql.cnf /var/lib/proxysql /var/log/proxysql*",
	}
	for _, cmd := range cleanupCmds {
		executeSSHCommand(host.IP, cmd, false)
	}

	fmt.Printf("%s ProxySQL自动部署开始\n", aurora.Blue(fmt.Sprintf("[%s]:", host.IP)))
	// 1. 下载并安装rpm包
	installCmds := "yum install -y https://mirrors.tuna.tsinghua.edu.cn/percona/proxysql/yum/release/8/RPMS/x86_64/proxysql2-2.7.3-1.1.el8.x86_64.rpm"
	if _, err := executeSSHCommand(host.IP, installCmds, false); err != nil {
		return fmt.Errorf("ProxySQL安装失败: %w, cmd: %s", err, installCmds)
	}
	// 2. 启动服务
	if _, err := executeSSHCommand(host.IP, "systemctl enable proxysql && systemctl restart proxysql", false); err != nil {
		return fmt.Errorf("ProxySQL服务启动失败: %w", err)
	}
	fmt.Printf("%s ProxySQL服务已启动\n", aurora.Green(fmt.Sprintf("[%s]:", host.IP)))
	// 3. 注册MySQL后端节点
	for _, backend := range mysqlBackends {
		addSQL := fmt.Sprintf(`INSERT INTO mysql_servers(hostgroup_id,hostname,port) VALUES (0,'%s',%d);`, backend.IP, deployConfig.MySQLPort)
		if err := runProxySQLAdminSQL(host.IP, addSQL); err != nil {
			fmt.Printf("%s 注册MySQL后端失败: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)), err)
		}
	}

	// 仅在第一个MySQL后端节点创建monitor和appuser用户，并注册appuser到ProxySQL
	if len(mysqlBackends) > 0 {
		// 创建monitor账号
		monitorUserReq := UserCreateRequest{
			Username:    "monitor",
			Password:    "monitor",
			Host:        "%",
			Privileges:  []string{"USAGE ON *.*"},
			RequireSSL:  false,
			Description: "ProxySQL monitoring account",
		}
		createMySQLUser(mysqlBackends[0].IP, monitorUserReq)
		// 创建应用账号
		appUserReq := UserCreateRequest{
			Username:    deployConfig.AppUser,
			Password:    deployConfig.AppPass,
			Host:        "%",
			Privileges:  []string{"ALL ON *.*"},
			RequireSSL:  false,
			Description: "ProxySQL application account",
		}
		createMySQLUser(mysqlBackends[0].IP, appUserReq)
	}
	// 注册应用账号到ProxySQL
	addUserSQL := fmt.Sprintf("INSERT INTO mysql_users(username,password,default_hostgroup) VALUES ('%s','%s',0);", deployConfig.AppUser, deployConfig.AppPass)
	if err := runProxySQLAdminSQL(host.IP, addUserSQL); err != nil {
		fmt.Printf("%s 注册应用账号到ProxySQL失败: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)), err)
	}
	// 保存并加载用户配置
	saveUserSQL := "LOAD MYSQL USERS TO RUNTIME; SAVE MYSQL USERS TO DISK;"
	if err := runProxySQLAdminSQL(host.IP, saveUserSQL); err != nil {
		fmt.Printf("%s ProxySQL用户配置保存失败: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)), err)
	}

	// 4. 保存并加载配置
	saveLoadCmds := "LOAD MYSQL SERVERS TO RUNTIME; SAVE MYSQL SERVERS TO DISK;"
	if err := runProxySQLAdminSQL(host.IP, saveLoadCmds); err != nil {
		fmt.Printf("%s ProxySQL配置保存失败: %v\n", aurora.Yellow(fmt.Sprintf("[%s]:", host.IP)), err)
	}
	fmt.Printf("%s ProxySQL后端注册完成，服务端口: 6033, 管理端口: 6032, 管理用户: admin, 密码: admin\n", aurora.Green(fmt.Sprintf("[%s]:", host.IP)))
	return nil
}

// 通过mysql客户端连接ProxySQL管理端口执行SQL
func runProxySQLAdminSQL(host, sql string) error {
	cmd := fmt.Sprintf("mysql -uadmin -padmin -h127.0.0.1 -P6032 --connect-timeout=5 -e \"%s\"", sql)
	output, err := executeSSHCommand(host, cmd, false)
	if err != nil {
		return fmt.Errorf("执行SQL失败: %w, cmd: %s, output: %s", err, cmd, output)
	}
	return nil
}

func filterMySQLHosts(hosts []HostInfo) []HostInfo {
	var filtered []HostInfo
	for _, h := range hosts {
		role := strings.ToLower(h.Role)
		if role != "proxysql" {
			filtered = append(filtered, h)
		}
	}
	return filtered
}
