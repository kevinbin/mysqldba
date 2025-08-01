// Copyright © 2024 Hong Bin <hongbin119@gmail.com>
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
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
)

// SSHConfig holds SSH configuration
type SSHConfig struct {
	HostFile   string
	Command    string
	LocalFile  string
	RemotePath string
	SSHUser    string
	SSHPort    int
	SSHKeyPath string
	Timeout    int
	Parallel   int
}

var sshConfig SSHConfig

// sshCmd represents the ssh command
var sshCmd = &cobra.Command{
	Use:   "ssh",
	Short: "SSH batch operation",
}

// sshSetupCmd represents the ssh setup command for setting up passwordless SSH
var sshSetupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Batch setup SSH passwordless login",
	Long:  `Batch setup SSH passwordless login, read host list file and setup SSH passwordless login`,
	Run: func(cmd *cobra.Command, args []string) {
		hosts, err := loadSSHHosts()
		if err != nil {
			fmt.Printf("%s: %v\n", aurora.Red("加载主机列表失败"), err)
			os.Exit(1)
		}

		err = setupSSHKeys(hosts)
		if err != nil {
			fmt.Printf("%s: %v\n", aurora.Red("设置SSH免密登录失败"), err)
			os.Exit(1)
		}

		fmt.Printf("%s\n", aurora.Green("SSH免密登录设置完成"))
	},
}

// sshExecCmd represents the ssh exec command for executing commands on multiple hosts
var sshExecCmd = &cobra.Command{
	Use:   "exec",
	Short: "Batch execute SSH commands",
	Long:  `Batch execute SSH commands, execute specified commands in parallel on multiple hosts`,
	Run: func(cmd *cobra.Command, args []string) {
		if sshConfig.Command == "" {
			fmt.Printf("%s\n", aurora.Red("请指定要执行的命令"))
			os.Exit(1)
		}

		hosts, err := loadSSHHosts()
		if err != nil {
			fmt.Printf("%s: %v\n", aurora.Red("加载主机列表失败"), err)
			os.Exit(1)
		}

		results := executeSSHCommandOnHosts(hosts, sshConfig.Command)
		displayExecutionResults(results)
	},
}

// sshCopyCmd represents the ssh copy command for copying files to multiple hosts
var sshCopyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Batch transfer files",
	Long:  `Batch transfer files, copy local files or directories to multiple remote hosts`,
	Run: func(cmd *cobra.Command, args []string) {
		if sshConfig.LocalFile == "" {
			fmt.Printf("%s\n", aurora.Red("请指定要传输的本地文件或目录"))
			os.Exit(1)
		}

		if sshConfig.RemotePath == "" {
			fmt.Printf("%s\n", aurora.Red("请指定远程目标路径"))
			os.Exit(1)
		}

		hosts, err := loadSSHHosts()
		if err != nil {
			fmt.Printf("%s: %v\n", aurora.Red("加载主机列表失败"), err)
			os.Exit(1)
		}

		results := copyToHosts(hosts, sshConfig.LocalFile, sshConfig.RemotePath)
		displayCopyResults(results)
	},
}

func init() {
	RootCmd.AddCommand(sshCmd)
	sshCmd.AddCommand(sshSetupCmd)
	sshCmd.AddCommand(sshExecCmd)
	sshCmd.AddCommand(sshCopyCmd)

	// 全局标志
	sshCmd.PersistentFlags().StringVarP(&sshConfig.HostFile, "host-file", "f", "hosts.txt", "Host list file")
	sshCmd.PersistentFlags().StringVar(&sshConfig.SSHUser, "ssh-user", "root", "SSH username")
	sshCmd.PersistentFlags().IntVar(&sshConfig.SSHPort, "ssh-port", 22, "SSH port")
	sshCmd.PersistentFlags().StringVarP(&sshConfig.SSHKeyPath, "ssh-key", "k", os.Getenv("HOME")+"/.ssh/id_rsa", "SSH private key path")
	sshCmd.PersistentFlags().IntVarP(&sshConfig.Timeout, "timeout", "t", 30, "SSH operation timeout (seconds)")
	sshCmd.PersistentFlags().IntVarP(&sshConfig.Parallel, "parallel", "n", 5, "Maximum parallel executions")

	// exec子命令标志
	sshExecCmd.Flags().StringVarP(&sshConfig.Command, "command", "c", "", "Command to execute")
	sshExecCmd.MarkFlagRequired("command")

	// copy子命令标志
	sshCopyCmd.Flags().StringVarP(&sshConfig.LocalFile, "local", "l", "", "Local file path")
	sshCopyCmd.Flags().StringVarP(&sshConfig.RemotePath, "remote", "r", "", "Remote target path")
	sshCopyCmd.MarkFlagRequired("local")
	sshCopyCmd.MarkFlagRequired("remote")
}

// loadSSHHosts loads hosts from the hosts file
func loadSSHHosts() ([]string, error) {
	file, err := os.Open(sshConfig.HostFile)
	if err != nil {
		return nil, fmt.Errorf("无法打开主机文件: %w", err)
	}
	defer file.Close()

	var hosts []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Extract IP from the line (first column)
		fields := strings.Fields(line)
		if len(fields) > 0 {
			hosts = append(hosts, fields[0])
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取主机文件错误: %w", err)
	}

	if len(hosts) == 0 {
		return nil, fmt.Errorf("主机文件为空或格式不正确")
	}

	return hosts, nil
}

// setupSSHKeys sets up passwordless SSH login
func setupSSHKeys(hosts []string) error {
	// Check if ssh-key exists
	pubKeyPath := sshConfig.SSHKeyPath + ".pub"
	if _, err := os.Stat(pubKeyPath); os.IsNotExist(err) {
		// Generate SSH key pair if it doesn't exist
		fmt.Printf("%s\n", aurora.Yellow("SSH密钥不存在，正在生成..."))
		cmd := exec.Command("ssh-keygen", "-t", "rsa", "-b", "2048", "-f", sshConfig.SSHKeyPath, "-N", "")
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("生成SSH密钥失败: %w", err)
		}
	}

	// Read public key
	pubKey, err := os.ReadFile(pubKeyPath)
	if err != nil {
		return fmt.Errorf("读取公钥文件失败: %w", err)
	}

	// Setup passwordless login for each host
	for i, host := range hosts {
		fmt.Printf("[%d/%d] 设置主机 %s 免密登录...\n", i+1, len(hosts), host)

		// ssh-copy-id equivalent
		cmd := exec.Command("ssh", "-o", "StrictHostKeyChecking=no",
			"-p", fmt.Sprintf("%d", sshConfig.SSHPort),
			fmt.Sprintf("%s@%s", sshConfig.SSHUser, host),
			"mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys && echo '"+
				strings.TrimSpace(string(pubKey))+"' >> ~/.ssh/authorized_keys")

		if err := cmd.Run(); err != nil {
			fmt.Printf("%s: %v\n", aurora.Red(fmt.Sprintf("主机 %s 设置失败", host)), err)
		} else {
			fmt.Printf("%s\n", aurora.Green(fmt.Sprintf("主机 %s 设置成功", host)))
		}
	}

	return nil
}

// executeSSHCommandOnHosts executes a command on multiple hosts in parallel
func executeSSHCommandOnHosts(hosts []string, command string) map[string]CommandResult {
	results := make(map[string]CommandResult)
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Use semaphore to limit concurrency
	semaphore := make(chan struct{}, sshConfig.Parallel)

	for _, host := range hosts {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire semaphore

		go func(host string) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release semaphore

			fmt.Printf("执行命令 on %s: %s\n", host, command)

			client, err := NewSSHClient(
				fmt.Sprintf("%s:%d", host, sshConfig.SSHPort),
				sshConfig.SSHUser,
				sshConfig.SSHKeyPath,
			)
			if err != nil {
				mu.Lock()
				results[host] = CommandResult{Host: host, Error: err}
				mu.Unlock()
				return
			}
			defer client.Close()

			output, err := client.Run(command)
			mu.Lock()
			results[host] = CommandResult{Host: host, Output: output, Error: err}
			mu.Unlock()
		}(host)
	}

	wg.Wait()
	return results
}

// copyToHosts copies a file or directory to multiple hosts in parallel
func copyToHosts(hosts []string, localPath, remotePath string) map[string]CommandResult {
	results := make(map[string]CommandResult)
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Check if local path exists
	fileInfo, err := os.Stat(localPath)
	if os.IsNotExist(err) {
		for _, host := range hosts {
			results[host] = CommandResult{
				Host:  host,
				Error: fmt.Errorf("本地路径不存在: %s", localPath),
			}
		}
		return results
	}

	// Use semaphore to limit concurrency
	semaphore := make(chan struct{}, sshConfig.Parallel)

	for _, host := range hosts {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire semaphore

		go func(host string) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release semaphore

			if fileInfo.IsDir() {
				fmt.Printf("传输目录到 %s: %s -> %s\n", host, localPath, remotePath)
			} else {
				fmt.Printf("传输文件到 %s: %s -> %s\n", host, localPath, remotePath)
			}

			client, err := NewSSHClient(
				fmt.Sprintf("%s:%d", host, sshConfig.SSHPort),
				sshConfig.SSHUser,
				sshConfig.SSHKeyPath,
			)
			if err != nil {
				mu.Lock()
				results[host] = CommandResult{Host: host, Error: err}
				mu.Unlock()
				return
			}
			defer client.Close()

			// 根据本地路径类型选择复制方法
			if fileInfo.IsDir() {
				err = client.CopyDir(localPath, remotePath)
			} else {
				err = client.CopyFile(localPath, remotePath)
			}

			mu.Lock()
			if err != nil {
				results[host] = CommandResult{Host: host, Error: err}
			} else {
				if fileInfo.IsDir() {
					results[host] = CommandResult{
						Host:   host,
						Output: fmt.Sprintf("目录成功传输: %s -> %s", localPath, remotePath),
					}
				} else {
					results[host] = CommandResult{
						Host:   host,
						Output: fmt.Sprintf("文件成功传输: %s -> %s", localPath, remotePath),
					}
				}
			}
			mu.Unlock()
		}(host)
	}

	wg.Wait()
	return results
}

// displayExecutionResults displays the results of command execution
func displayExecutionResults(results map[string]CommandResult) {
	fmt.Println("\n=== 执行结果 ===")
	fmt.Printf("总主机数: %d\n", len(results))

	var success, failed int
	for host, result := range results {
		if result.Error != nil {
			failed++
			fmt.Printf("\n%s (%s):\n", aurora.Red(host), aurora.Red("失败"))
			fmt.Printf("错误: %v\n", result.Error)
		} else {
			success++
			fmt.Printf("\n%s (%s):\n", aurora.Green(host), aurora.Green("成功"))
			fmt.Println(result.Output)
		}
	}

	fmt.Printf("\n%s: %d\n", aurora.Green("成功"), success)
	fmt.Printf("%s: %d\n", aurora.Red("失败"), failed)
}

// displayCopyResults displays the results of file copy operation
func displayCopyResults(results map[string]CommandResult) {
	fmt.Println("\n=== 传输结果 ===")
	fmt.Printf("总主机数: %d\n", len(results))

	var success, failed int
	for host, result := range results {
		if result.Error != nil {
			failed++
			fmt.Printf("%s: %s (%v)\n", host, aurora.Red("失败"), result.Error)
		} else {
			success++
			fmt.Printf("%s: %s\n", host, aurora.Green("成功"))
		}
	}

	fmt.Printf("\n%s: %d\n", aurora.Green("成功"), success)
	fmt.Printf("%s: %d\n", aurora.Red("失败"), failed)
}
