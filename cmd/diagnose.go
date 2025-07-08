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
	"archive/tar"
	"bufio"
	"compress/gzip"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

// diagnoseCmd represents the diagnose command
var diagnoseCmd = &cobra.Command{
	Use:   "sos",
	Short: "Collect MySQL system diagnostic data",
	Long: `Collect various diagnostic data for MySQL systems, including:
- System basic information (CPU, memory, disk, etc.)
- MySQL configuration and status information
- Performance data
- Log files
- Process information
Generate compressed package for problem analysis`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := runDiagnose(); err != nil {
			fmt.Fprintf(os.Stderr, "Error occurred during diagnosis: %v\n", err)
			os.Exit(1)
		}
	},
}

var (
	mycnfPath  string
	enableGdb  bool
	outputDir  string
	timeoutSec int
)

type DiagnosticInfo struct {
	OutputDir   string
	MyCnfPath   string
	DataDir     string
	ErrorLog    string
	WarningsLog string
	StartTime   time.Time
}

func init() {
	RootCmd.AddCommand(diagnoseCmd)

	// Configuration file path
	diagnoseCmd.Flags().StringVarP(&mycnfPath, "config", "c", "/etc/my.cnf", "MySQL configuration file path")

	// Enable gdb to collect stack traces
	diagnoseCmd.Flags().BoolVarP(&enableGdb, "stack", "s", false, "Use eu-stack to collect mysqld stack traces (will pause mysqld for a few seconds) ")

	// Output directory
	diagnoseCmd.Flags().StringVarP(&outputDir, "output", "o", "", "Output directory (default: /tmp/mysql-diags-<timestamp>)")

	// Timeout
	diagnoseCmd.Flags().IntVarP(&timeoutSec, "timeout", "t", 300, "Command execution timeout in seconds")
}

func runDiagnose() error {
	// Check if running as root user (Linux/Unix systems only)
	if runtime.GOOS != "windows" {
		if os.Getuid() != 0 {
			fmt.Println("Warning: Recommend running with root privileges to get complete system information")
		}
	}

	// Check if configuration file exists
	if _, err := os.Stat(mycnfPath); os.IsNotExist(err) {
		return fmt.Errorf("cannot read configuration file %s", mycnfPath)
	}

	// Create diagnostic information structure
	diag, err := initDiagnostic()
	if err != nil {
		return fmt.Errorf("failed to initialize diagnostic environment: %v", err)
	}

	// Check if required commands exist
	if err := checkRequiredCommands(); err != nil {
		return err
	}

	fmt.Printf("Starting MySQL system diagnostic data collection, based on configuration file: %s\n", mycnfPath)
	fmt.Printf("Output directory: %s\n", diag.OutputDir)
	fmt.Println("Please wait patiently, a file with diagnostic information will be provided upon completion...")
	fmt.Println()

	// Collect various information step by step
	steps := []struct {
		name string
		fn   func(*DiagnosticInfo) error
	}{
		{"Copy configuration file", copyMyCnf},
		{"Collect system basic information", collectSystemInfo},
		{"Collect disk information", collectDiskInfo},
		{"Collect process information", collectProcessInfo},
		{"Collect memory information", collectMemoryInfo},
		{"Collect network information", collectNetworkInfo},
		{"Collect performance data", collectPerformanceData},
		{"Collect NUMA information", collectNumaInfo},
		{"Collect system logs", collectSystemLogs},
		{"Collect MySQL data file information", collectDataFileInfo},
		{"Collect MySQL error log", collectMySQLErrorLog},
		{"Collect MySQL status information", collectMySQLStatus},
	}

	if enableGdb {
		steps = append(steps, struct {
			name string
			fn   func(*DiagnosticInfo) error
		}{"Collect MySQL stack trace", collectMySQLBacktrace})
	}

	for _, step := range steps {
		fmt.Printf("%s...\n", step.name)
		if err := step.fn(diag); err != nil {
			fmt.Printf("Warning: %s failed: %v\n", step.name, err)
			// Record to warnings file
			writeWarning(diag, fmt.Sprintf("%s failed: %v", step.name, err))
		}
	}

	// Create tarball
	fmt.Println("\nCreating tarball...")
	tarballPath, err := createTarball(diag)
	if err != nil {
		return fmt.Errorf("failed to create tarball: %v", err)
	}

	fmt.Println("\nCollection complete!")
	fmt.Printf("Diagnostic file: %s\n", tarballPath)
	fmt.Printf("Recommend deleting temporary directory: %s\n", diag.OutputDir)

	return nil
}

// CommandInfo represents information about a command
type CommandInfo struct {
	Name        string
	Required    bool
	Description string
	InstallCmd  map[string]string // OS -> install command
}

// checkRequiredCommands checks if all required commands exist and provides installation guidance
func checkRequiredCommands() error {
	commands := getRequiredCommands()
	missingCommands := []CommandInfo{}

	for _, cmd := range commands {
		if !isCommandAvailable(cmd.Name) {
			if cmd.Required {
				missingCommands = append(missingCommands, cmd)
			} else {
				fmt.Printf("Optional command '%s' not found: %s\n", cmd.Name, cmd.Description)
			}
		}
	}

	if len(missingCommands) > 0 {
		fmt.Println("The following required commands are missing:")
		fmt.Println()

		// Group by installation method
		installGroups := make(map[string][]string)
		for _, cmd := range missingCommands {
			osKey := runtime.GOOS
			if installCmd, exists := cmd.InstallCmd[osKey]; exists {
				installGroups[installCmd] = append(installGroups[installCmd], cmd.Name)
			} else if installCmd, exists := cmd.InstallCmd["default"]; exists {
				installGroups[installCmd] = append(installGroups[installCmd], cmd.Name)
			} else {
				fmt.Printf("- %s: %s (no installation info available)\n", cmd.Name, cmd.Description)
			}
		}

		// Display installation instructions
		for installCmd, cmdNames := range installGroups {
			sort.Strings(cmdNames)
			fmt.Printf("Missing commands: %s\n", strings.Join(cmdNames, ", "))
			fmt.Printf("Install with: %s\n\n", installCmd)
		}

		// Ask user if they want to continue
		fmt.Print("Do you want to continue anyway? (y/N): ")
		var response string
		fmt.Scanln(&response)
		response = strings.ToLower(strings.TrimSpace(response))

		if response != "y" && response != "yes" {
			return fmt.Errorf("diagnostic collection cancelled by user")
		}
		fmt.Println()
	}

	return nil
}

// isCommandAvailable checks if a command is available in PATH
func isCommandAvailable(command string) bool {
	_, err := exec.LookPath(command)
	return err == nil
}

// getRequiredCommands returns list of commands needed for diagnosis
func getRequiredCommands() []CommandInfo {
	commands := []CommandInfo{
		// Basic system commands (cross-platform)
		{Name: "ps", Required: true, Description: "Process information", InstallCmd: map[string]string{
			"default": "Usually pre-installed on Unix systems",
		}},
		{Name: "free", Required: runtime.GOOS == "linux", Description: "Memory usage information", InstallCmd: map[string]string{
			"linux":   "Usually pre-installed on Linux",
			"default": "Install procps package",
		}},
		{Name: "df", Required: true, Description: "Disk space information", InstallCmd: map[string]string{
			"default": "Usually pre-installed on Unix systems",
		}},
		{Name: "mount", Required: true, Description: "Mount point information", InstallCmd: map[string]string{
			"default": "Usually pre-installed on Unix systems",
		}},
		{Name: "uname", Required: true, Description: "System information", InstallCmd: map[string]string{
			"default": "Usually pre-installed on Unix systems",
		}},
		{Name: "uptime", Required: true, Description: "System uptime", InstallCmd: map[string]string{
			"default": "Usually pre-installed on Unix systems",
		}},
	}

	// Add Linux-specific commands
	if runtime.GOOS == "linux" {
		linuxCommands := []CommandInfo{
			{Name: "lscpu", Required: true, Description: "CPU information", InstallCmd: map[string]string{
				"linux": "sudo apt-get install util-linux (Ubuntu/Debian) or sudo yum install util-linux (RHEL/CentOS)",
			}},
			{Name: "lsblk", Required: true, Description: "Block device information", InstallCmd: map[string]string{
				"linux": "sudo apt-get install util-linux (Ubuntu/Debian) or sudo yum install util-linux (RHEL/CentOS)",
			}},
			{Name: "dmesg", Required: true, Description: "Kernel messages", InstallCmd: map[string]string{
				"linux": "Usually pre-installed on Linux",
			}},
			{Name: "lsof", Required: true, Description: "Open files information", InstallCmd: map[string]string{
				"linux": "sudo apt-get install lsof (Ubuntu/Debian) or sudo yum install lsof (RHEL/CentOS)",
			}},
			{Name: "iostat", Required: false, Description: "I/O statistics", InstallCmd: map[string]string{
				"linux": "sudo apt-get install sysstat (Ubuntu/Debian) or sudo yum install sysstat (RHEL/CentOS)",
			}},
			{Name: "vmstat", Required: false, Description: "Virtual memory statistics", InstallCmd: map[string]string{
				"linux": "sudo apt-get install procps (Ubuntu/Debian) or sudo yum install procps-ng (RHEL/CentOS)",
			}},
			{Name: "mpstat", Required: false, Description: "Multi-processor statistics", InstallCmd: map[string]string{
				"linux": "sudo apt-get install sysstat (Ubuntu/Debian) or sudo yum install sysstat (RHEL/CentOS)",
			}},
			{Name: "sar", Required: false, Description: "System activity reporter", InstallCmd: map[string]string{
				"linux": "sudo apt-get install sysstat (Ubuntu/Debian) or sudo yum install sysstat (RHEL/CentOS)",
			}},
			{Name: "top", Required: false, Description: "Process activity", InstallCmd: map[string]string{
				"linux": "sudo apt-get install procps (Ubuntu/Debian) or sudo yum install procps-ng (RHEL/CentOS)",
			}},
			{Name: "numastat", Required: false, Description: "NUMA statistics", InstallCmd: map[string]string{
				"linux": "sudo apt-get install numactl (Ubuntu/Debian) or sudo yum install numactl (RHEL/CentOS)",
			}},
			{Name: "numactl", Required: false, Description: "NUMA control", InstallCmd: map[string]string{
				"linux": "sudo apt-get install numactl (Ubuntu/Debian) or sudo yum install numactl (RHEL/CentOS)",
			}},
		}
		commands = append(commands, linuxCommands...)

		// Network commands - prefer newer tools
		if isCommandAvailable("ip") {
			commands = append(commands, CommandInfo{
				Name: "ip", Required: true, Description: "Network interface information", InstallCmd: map[string]string{
					"linux": "sudo apt-get install iproute2 (Ubuntu/Debian) or sudo yum install iproute (RHEL/CentOS)",
				},
			})
		} else {
			commands = append(commands, CommandInfo{
				Name: "ifconfig", Required: true, Description: "Network interface information", InstallCmd: map[string]string{
					"linux": "sudo apt-get install net-tools (Ubuntu/Debian) or sudo yum install net-tools (RHEL/CentOS)",
				},
			})
		}

		if isCommandAvailable("ss") {
			commands = append(commands, CommandInfo{
				Name: "ss", Required: true, Description: "Socket statistics", InstallCmd: map[string]string{
					"linux": "sudo apt-get install iproute2 (Ubuntu/Debian) or sudo yum install iproute (RHEL/CentOS)",
				},
			})
		} else {
			commands = append(commands, CommandInfo{
				Name: "netstat", Required: true, Description: "Network statistics", InstallCmd: map[string]string{
					"linux": "sudo apt-get install net-tools (Ubuntu/Debian) or sudo yum install net-tools (RHEL/CentOS)",
				},
			})
		}

		// Optional LVM commands
		lvsCommands := []CommandInfo{
			{Name: "pvdisplay", Required: false, Description: "LVM physical volume display", InstallCmd: map[string]string{
				"linux": "sudo apt-get install lvm2 (Ubuntu/Debian) or sudo yum install lvm2 (RHEL/CentOS)",
			}},
			{Name: "vgdisplay", Required: false, Description: "LVM volume group display", InstallCmd: map[string]string{
				"linux": "sudo apt-get install lvm2 (Ubuntu/Debian) or sudo yum install lvm2 (RHEL/CentOS)",
			}},
			{Name: "lvdisplay", Required: false, Description: "LVM logical volume display", InstallCmd: map[string]string{
				"linux": "sudo apt-get install lvm2 (Ubuntu/Debian) or sudo yum install lvm2 (RHEL/CentOS)",
			}},
		}
		commands = append(commands, lvsCommands...)

		// Optional SELinux commands
		if isCommandAvailable("getenforce") {
			commands = append(commands, CommandInfo{
				Name: "getenforce", Required: false, Description: "SELinux status", InstallCmd: map[string]string{
					"linux": "sudo apt-get install selinux-utils (Ubuntu/Debian) or sudo yum install policycoreutils (RHEL/CentOS)",
				},
			})
		}
	}

	// MySQL client
	commands = append(commands, CommandInfo{
		Name: "mysql", Required: false, Description: "MySQL client for status collection", InstallCmd: map[string]string{
			"linux":   "sudo apt-get install mysql-client (Ubuntu/Debian) or sudo yum install mysql (RHEL/CentOS)",
			"darwin":  "brew install mysql-client",
			"default": "Install MySQL client package",
		},
	})

	// Stack trace collection tool
	if enableGdb {
		commands = append(commands, CommandInfo{
			Name: "eu-stack", Required: false, Description: "Stack trace collection", InstallCmd: map[string]string{
				"linux":   "sudo apt-get install elfutils (Ubuntu/Debian) or sudo yum install elfutils (RHEL/CentOS)",
				"default": "Install elfutils package",
			},
		})
	}

	// Tail command for log collection
	commands = append(commands, CommandInfo{
		Name: "tail", Required: true, Description: "Text file processing", InstallCmd: map[string]string{
			"default": "Usually pre-installed on Unix systems",
		},
	})

	return commands
}

func initDiagnostic() (*DiagnosticInfo, error) {
	// Generate output directory name
	if outputDir == "" {
		timestamp := time.Now().Format("2006-01-02_15.04.05")
		outputDir = fmt.Sprintf("/tmp/mysql-diags-%d-%s", os.Getpid(), timestamp)
	}

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, err
	}

	diag := &DiagnosticInfo{
		OutputDir:   outputDir,
		MyCnfPath:   mycnfPath,
		WarningsLog: filepath.Join(outputDir, "WARNINGS"),
		StartTime:   time.Now(),
	}

	return diag, nil
}

func copyMyCnf(diag *DiagnosticInfo) error {
	destPath := filepath.Join(diag.OutputDir, "my.cnf")
	return copyFile(diag.MyCnfPath, destPath)
}

func collectSystemInfo(diag *DiagnosticInfo) error {
	outputFile := filepath.Join(diag.OutputDir, "sysinfo.txt")
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	commands := []struct {
		title string
		cmd   string
		args  []string
	}{
		{"System information", "uname", []string{"-a"}},
		{"Uptime", "uptime", []string{}},
		{"CPU information", "lscpu", []string{}},
		{"System limits", "ulimit", []string{"-a"}},
	}

	// Add more commands for Linux systems
	if runtime.GOOS == "linux" {
		linuxCommands := []struct {
			title string
			cmd   string
			args  []string
		}{
			{"SELinux status", "getenforce", []string{}},
			{"Swap partition", "swapon", []string{"-s"}},
			{"Tuning configuration", "tuned-adm", []string{"list"}},
			{"Hardware information", "dmidecode", []string{}},
		}
		commands = append(commands, linuxCommands...)
	}

	for _, cmdInfo := range commands {
		fmt.Fprintf(f, "========= %s ===========\n", cmdInfo.title)
		if err := runCommandToFile(f, cmdInfo.cmd, cmdInfo.args); err != nil {
			fmt.Fprintf(f, "Error running %s: %v\n", cmdInfo.cmd, err)
		}
		fmt.Fprintf(f, "\n")
	}

	// Collect system parameters
	if runtime.GOOS == "linux" {
		fmt.Fprintf(f, "========= System parameters ==========\n")
		sysctlCmd := exec.Command("sysctl", "-a")
		grepCmd := exec.Command("egrep", "-i", "dirty|swappiness|file-max|threads-max")

		sysctlOut, _ := sysctlCmd.StdoutPipe()
		grepCmd.Stdin = sysctlOut
		grepOut, _ := grepCmd.StdoutPipe()

		if err := sysctlCmd.Start(); err == nil {
			if err := grepCmd.Start(); err == nil {
				io.Copy(f, grepOut)
				grepCmd.Wait()
			}
			sysctlCmd.Wait()
		}
	}

	return nil
}

func collectDiskInfo(diag *DiagnosticInfo) error {
	outputFile := filepath.Join(diag.OutputDir, "disk.txt")
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	commands := []struct {
		title string
		cmd   string
		args  []string
	}{
		{"Disk usage", "df", []string{"-Th"}},
		{"Mount information", "mount", []string{}},
	}

	// Linux-specific commands
	if runtime.GOOS == "linux" {
		linuxCommands := []struct {
			title string
			cmd   string
			args  []string
		}{
			{"LVM physical volumes", "pvdisplay", []string{}},
			{"LVM volume groups", "vgdisplay", []string{}},
			{"LVM logical volumes", "lvdisplay", []string{}},
			{"Block device information", "lsblk", []string{}},
		}
		commands = append(commands, linuxCommands...)
	}

	for _, cmdInfo := range commands {
		fmt.Fprintf(f, "========= %s ===========\n", cmdInfo.title)
		if err := runCommandToFile(f, cmdInfo.cmd, cmdInfo.args); err != nil {
			fmt.Fprintf(f, "Error running %s: %v\n", cmdInfo.cmd, err)
		}
		fmt.Fprintf(f, "\n")
	}

	// Disk scheduler information (Linux only)
	if runtime.GOOS == "linux" {
		fmt.Fprintf(f, "========= Disk scheduler ===========\n")
		schedulerFiles, _ := filepath.Glob("/sys/block/sd*/queue/scheduler")
		for _, file := range schedulerFiles {
			fmt.Fprintf(f, "%s: ", file)
			if content, err := ioutil.ReadFile(file); err == nil {
				f.Write(content)
			} else {
				fmt.Fprintf(f, "Error reading: %v", err)
			}
			fmt.Fprintf(f, "\n")
		}
	}

	return nil
}

func collectProcessInfo(diag *DiagnosticInfo) error {
	outputFile := filepath.Join(diag.OutputDir, "processes.txt")
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Basic process information
	if err := runCommandToFile(f, "ps", []string{"aux"}); err != nil {
		return err
	}

	fmt.Fprintf(f, "\n========= MySQL related processes ===========\n")
	psCmd := exec.Command("ps", "auxfww")
	grepCmd := exec.Command("grep", "-E", "mysqld|ndbd|ndbmtd")

	psOut, _ := psCmd.StdoutPipe()
	grepCmd.Stdin = psOut
	grepOut, _ := grepCmd.StdoutPipe()

	if err := psCmd.Start(); err == nil {
		if err := grepCmd.Start(); err == nil {
			io.Copy(f, grepOut)
			grepCmd.Wait()
		}
		psCmd.Wait()
	}

	// MySQL process file descriptors and limits information (Linux only)
	if runtime.GOOS == "linux" {
		mysqlPids := getMySQLPids()
		for _, pid := range mysqlPids {
			fmt.Fprintf(f, "\n========= MySQL process %s file descriptors ===========\n", pid)
			if err := runCommandToFile(f, "lsof", []string{"-s", "-p" + pid}); err != nil {
				fmt.Fprintf(f, "Error running lsof: %v\n", err)
			}

			fmt.Fprintf(f, "\n========= MySQL process %s limits ===========\n", pid)
			limitsFile := fmt.Sprintf("/proc/%s/limits", pid)
			if content, err := ioutil.ReadFile(limitsFile); err == nil {
				f.Write(content)
			} else {
				fmt.Fprintf(f, "Error reading limits: %v\n", err)
			}
		}

		// Display processes with highest swap memory usage
		fmt.Fprintf(f, "\n========= Process swap memory usage ===========\n")
		statusFiles, _ := filepath.Glob("/proc/*/status")
		type procSwap struct {
			name string
			swap int64
		}
		var procs []procSwap

		for _, file := range statusFiles {
			if content, err := ioutil.ReadFile(file); err == nil {
				lines := strings.Split(string(content), "\n")
				var name string
				var swap int64
				for _, line := range lines {
					if strings.HasPrefix(line, "Name:") {
						parts := strings.Fields(line)
						if len(parts) > 1 {
							name = parts[1]
						}
					} else if strings.HasPrefix(line, "VmSwap:") {
						parts := strings.Fields(line)
						if len(parts) > 1 {
							if s, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
								swap = s
							}
						}
					}
				}
				if swap > 0 {
					procs = append(procs, procSwap{name: name, swap: swap})
				}
			}
		}

		// Sort by swap memory usage
		for i := 0; i < len(procs)-1; i++ {
			for j := i + 1; j < len(procs); j++ {
				if procs[i].swap < procs[j].swap {
					procs[i], procs[j] = procs[j], procs[i]
				}
			}
		}

		// Display top 20
		count := len(procs)
		if count > 20 {
			count = 20
		}
		for i := 0; i < count; i++ {
			fmt.Fprintf(f, "%s %d kB\n", procs[i].name, procs[i].swap)
		}
	}

	return nil
}

func collectMemoryInfo(diag *DiagnosticInfo) error {
	outputFile := filepath.Join(diag.OutputDir, "memory.txt")
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Memory usage
	if err := runCommandToFile(f, "free", []string{"-m"}); err != nil {
		return err
	}

	// Linux-specific memory information
	if runtime.GOOS == "linux" {
		fmt.Fprintf(f, "\n========= /proc/meminfo ===========\n")
		if content, err := ioutil.ReadFile("/proc/meminfo"); err == nil {
			f.Write(content)
		}

		fmt.Fprintf(f, "\n========= /proc/vmstat ===========\n")
		if content, err := ioutil.ReadFile("/proc/vmstat"); err == nil {
			f.Write(content)
		}

		fmt.Fprintf(f, "\n========= nr_pdflush_threads ===========\n")
		if content, err := ioutil.ReadFile("/proc/sys/vm/nr_pdflush_threads"); err == nil {
			fmt.Fprintf(f, "nr_pdflush_threads: %s", string(content))
		}
	}

	return nil
}

func collectNetworkInfo(diag *DiagnosticInfo) error {
	outputFile := filepath.Join(diag.OutputDir, "network.txt")
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Network interface information
	if runtime.GOOS == "linux" {
		if err := runCommandToFile(f, "ip", []string{"addr", "show"}); err != nil {
			// If ip command is not available, try ifconfig
			if err := runCommandToFile(f, "ifconfig", []string{}); err != nil {
				fmt.Fprintf(f, "Error getting network interface info: %v\n", err)
			}
		}
	} else {
		if err := runCommandToFile(f, "ifconfig", []string{}); err != nil {
			fmt.Fprintf(f, "Error getting network interface info: %v\n", err)
		}
	}

	fmt.Fprintf(f, "\n========= Listening ports ===========\n")
	if err := runCommandToFile(f, "netstat", []string{"-lnput"}); err != nil {
		// If netstat is not available, try ss
		if err := runCommandToFile(f, "ss", []string{"-lnput"}); err != nil {
			fmt.Fprintf(f, "Error getting listening ports: %v\n", err)
		}
	}

	return nil
}

func collectPerformanceData(diag *DiagnosticInfo) error {
	outputFile := filepath.Join(diag.OutputDir, "performance.txt")
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "Collecting system performance data (2-second intervals, 20 samples)\n")
	fmt.Fprintf(f, "Start time: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))

	// Run performance monitoring commands in background
	commands := []struct {
		name string
		cmd  string
		args []string
	}{
		{"top", "top", []string{"-b", "-n", "2", "-d", "2"}},
		{"iostat", "iostat", []string{"-xctdk", "2", "20"}},
		{"vmstat", "vmstat", []string{"2", "20"}},
		{"mpstat", "mpstat", []string{"-P", "ALL", "2", "20"}},
		{"sar_dev", "sar", []string{"-n", "DEV", "2", "20"}},
		{"sar_tcp", "sar", []string{"-n", "TCP,ETCP", "2", "20"}},
	}

	for _, cmdInfo := range commands {
		fmt.Fprintf(f, "========= %s ===========\n", cmdInfo.name)
		if err := runCommandToFile(f, cmdInfo.cmd, cmdInfo.args); err != nil {
			fmt.Fprintf(f, "Error running %s: %v\n", cmdInfo.cmd, err)
		}
		fmt.Fprintf(f, "\n")
	}

	return nil
}

func collectNumaInfo(diag *DiagnosticInfo) error {
	if runtime.GOOS != "linux" {
		return nil // NUMA information is mainly applicable to Linux
	}

	outputFile := filepath.Join(diag.OutputDir, "numa.txt")
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	commands := []struct {
		title string
		cmd   string
		args  []string
	}{
		{"NUMA statistics", "numastat", []string{}},
		{"NUMA control", "numactl", []string{"--show"}},
		{"NUMA hardware", "numactl", []string{"--hardware"}},
	}

	for _, cmdInfo := range commands {
		fmt.Fprintf(f, "========= %s ===========\n", cmdInfo.title)
		if err := runCommandToFile(f, cmdInfo.cmd, cmdInfo.args); err != nil {
			fmt.Fprintf(f, "Error running %s: %v\n", cmdInfo.cmd, err)
		}
		fmt.Fprintf(f, "\n")
	}

	// MySQL NUMA statistics
	fmt.Fprintf(f, "========= MySQL NUMA statistics ===========\n")
	if err := runCommandToFile(f, "numastat", []string{"mysqld", "-v"}); err != nil {
		fmt.Fprintf(f, "Error running numastat mysqld: %v\n", err)
	}

	// NUMA zone list order
	fmt.Fprintf(f, "\n========= NUMA zone list order ===========\n")
	if content, err := ioutil.ReadFile("/proc/sys/vm/numa_zonelist_order"); err == nil {
		f.Write(content)
	}

	return nil
}

func collectSystemLogs(diag *DiagnosticInfo) error {
	outputFile := filepath.Join(diag.OutputDir, "dmesg.txt")
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// System kernel logs
	if err := runCommandToFile(f, "dmesg", []string{}); err != nil {
		fmt.Fprintf(f, "Error running dmesg: %v\n", err)
	}

	// Search for related error logs
	fmt.Fprintf(f, "\n========= Error information in system logs ===========\n")
	if runtime.GOOS == "linux" {
		logFiles := []string{"/var/log/messages", "/var/log/syslog", "/var/log/kern.log"}
		for _, logFile := range logFiles {
			if _, err := os.Stat(logFile); err == nil {
				fmt.Fprintf(f, "\n--- Related information in %s ---\n", logFile)
				grepCmd := exec.Command("grep", "-i", "-E", "error|fault|mysql|ndb|denied", logFile)
				if output, err := grepCmd.Output(); err == nil {
					f.Write(output)
				}
			}
		}
	}

	return nil
}

func collectDataFileInfo(diag *DiagnosticInfo) error {
	// Get data directories from configuration file
	dataDirs, err := getDataDirsFromConfig(diag.MyCnfPath)
	if err != nil {
		return err
	}

	if len(dataDirs) == 0 {
		writeWarning(diag, "No datadir configuration found in configuration file")
		return nil
	}

	// Check for multiple datadir definitions
	if len(dataDirs) > 1 {
		warning := fmt.Sprintf("Multiple datadir definitions found in configuration file: %v", dataDirs)
		writeWarning(diag, warning)
	}

	// Collect file information for each data directory
	for _, dataDir := range dataDirs {
		// Remove trailing slash
		dataDir = strings.TrimSuffix(dataDir, "/")

		// File listing
		dataFileOutput := filepath.Join(diag.OutputDir, "datafile.txt")
		if err := collectDirectoryListing(dataDir, dataFileOutput); err != nil {
			writeWarning(diag, fmt.Sprintf("Failed to collect file listing for data directory %s: %v", dataDir, err))
		}

		// SELinux contexts (Linux only)
		if runtime.GOOS == "linux" {
			contextsOutput := filepath.Join(diag.OutputDir, "contexts.txt")
			if err := collectSELinuxContexts(dataDir, contextsOutput); err != nil {
				writeWarning(diag, fmt.Sprintf("Failed to collect SELinux contexts for data directory %s: %v", dataDir, err))
			}
		}

		// Save first data directory for subsequent use
		if diag.DataDir == "" {
			diag.DataDir = dataDir
		}
	}

	return nil
}

func collectMySQLErrorLog(diag *DiagnosticInfo) error {
	// Get error log path
	errorLogPath, err := getMySQLErrorLogPath(diag.MyCnfPath, diag.DataDir)
	if err != nil {
		return err
	}

	diag.ErrorLog = errorLogPath

	// Copy last 20000 lines of error log
	outputFile := filepath.Join(diag.OutputDir, "mysqld.log")
	return copyLastLines(errorLogPath, outputFile, 20000)
}

func collectMySQLBacktrace(diag *DiagnosticInfo) error {
	if runtime.GOOS == "windows" {
		return fmt.Errorf("Windows system does not support stack trace collection")
	}

	// Check if eu-stack is available
	if !isCommandAvailable("eu-stack") {
		return fmt.Errorf("eu-stack command not available")
	}

	// Get MySQL PID file path
	pidFile, err := getMySQLPidFile(diag.MyCnfPath)
	if err != nil {
		return err
	}

	// Read PID
	pidContent, err := ioutil.ReadFile(pidFile)
	if err != nil {
		return fmt.Errorf("failed to read PID file: %v", err)
	}

	pid := strings.TrimSpace(string(pidContent))

	// Use eu-stack to collect stack trace
	outputFile := filepath.Join(diag.OutputDir, "backtrace.txt")
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Printf("Using eu-stack to collect stack trace for MySQL process %s, MySQL will pause for a few seconds...\n", pid)

	cmd := exec.Command("eu-stack", "-p", pid)
	cmd.Stdout = f
	cmd.Stderr = f

	return cmd.Run()
}

func collectMySQLStatus(diag *DiagnosticInfo) error {
	dsn := buildDSN()
	db := mysqlConnect(dsn)
	defer db.Close()

	outputFile := filepath.Join(diag.OutputDir, "mysql_status.txt")
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Get MySQL version to determine which lock wait query to use
	var version string
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		return err
	}

	var lockQuery string
	if strings.Contains(version, "8.0") {
		lockQuery = "SELECT * FROM sys.innodb_lock_waits\\G"
	} else {
		lockQuery = `SELECT r.trx_wait_started AS wait_started, TIMEDIFF(NOW(), r.trx_wait_started) AS wait_age,
rl.lock_table AS locked_table, rl.lock_index AS locked_index, rl.lock_type AS locked_type,
r.trx_id AS waiting_trx_id, r.trx_mysql_thread_id AS waiting_pid, r.trx_query AS waiting_query,
rl.lock_id AS waiting_lock_id, rl.lock_mode AS waiting_lock_mode, b.trx_id AS blocking_trx_id,
b.trx_mysql_thread_id AS blocking_pid, b.trx_query AS blocking_query, bl.lock_id AS blocking_lock_id,
bl.lock_mode AS blocking_lock_mode
FROM information_schema.INNODB_LOCK_WAITS w
INNER JOIN information_schema.INNODB_TRX b ON b.trx_id = w.blocking_trx_id
INNER JOIN information_schema.INNODB_TRX r ON r.trx_id = w.requesting_trx_id
INNER JOIN information_schema.INNODB_LOCKS bl ON bl.lock_id = w.blocking_lock_id
INNER JOIN information_schema.INNODB_LOCKS rl ON rl.lock_id = w.requested_lock_id
ORDER BY r.trx_wait_started\\G`
	}

	// MySQL status query sequence
	queries := []string{
		"SELECT NOW();",
		"SHOW GLOBAL VARIABLES;",
		"SHOW MASTER STATUS;",
		"SHOW SLAVE STATUS\\G",
		"SHOW GLOBAL STATUS;",
		"SHOW FULL PROCESSLIST;",
		"SHOW ENGINE INNODB STATUS\\G",
		"SHOW FULL PROCESSLIST;",
		lockQuery,
		"SHOW FULL PROCESSLIST;",
		"SELECT SLEEP(30);",
		"SHOW MASTER STATUS;",
		"SHOW SLAVE STATUS\\G",
		"SHOW GLOBAL STATUS;",
		"SHOW FULL PROCESSLIST;",
		"SHOW ENGINE INNODB STATUS\\G",
		"SHOW FULL PROCESSLIST;",
		lockQuery,
		"SHOW FULL PROCESSLIST;",
		`SELECT ENGINE, COUNT(*), SUM(DATA_LENGTH), SUM(INDEX_LENGTH)
FROM information_schema.TABLES
GROUP BY ENGINE;`,
		"STATUS;",
	}

	fmt.Fprintf(f, "MySQL status collection start time: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(f, "MySQL version: %s\n\n", version)

	for i, query := range queries {
		fmt.Fprintf(f, "========= Query %d: %s =========\n", i+1, query)

		if strings.Contains(query, "SLEEP") {
			fmt.Fprintf(f, "Waiting 30 seconds...\n")
			time.Sleep(30 * time.Second)
			continue
		}

		if err := executeMySQLQuery(db, query, f); err != nil {
			fmt.Fprintf(f, "Query execution failed: %v\n", err)
		}
		fmt.Fprintf(f, "\n")
	}

	return nil
}

func createTarball(diag *DiagnosticInfo) (string, error) {
	tarballPath := diag.OutputDir + ".tgz"

	// Create tar.gz file
	tarFile, err := os.Create(tarballPath)
	if err != nil {
		return "", err
	}
	defer tarFile.Close()

	gzWriter := gzip.NewWriter(tarFile)
	defer gzWriter.Close()

	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	// Get base name of output directory
	baseName := filepath.Base(diag.OutputDir)

	// Walk through all files in output directory
	return tarballPath, filepath.Walk(diag.OutputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Create tar header information
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}

		// Set relative path
		relPath, err := filepath.Rel(diag.OutputDir, path)
		if err != nil {
			return err
		}
		header.Name = filepath.Join(baseName, relPath)

		// Write tar header
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		// If it's a directory, no need to write content
		if info.IsDir() {
			return nil
		}

		// Write file content
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		_, err = io.Copy(tarWriter, file)
		return err
	})
}

// Helper functions

// cleanTextOutput cleans unprintable characters and control characters from text output
func cleanTextOutput(input []byte) []byte {
	// Create a new buffer to store cleaned content
	var result []byte

	for _, b := range input {
		// Keep printable characters, newlines, tabs and carriage returns
		if b == '\n' || b == '\t' || b == '\r' || (b >= 32 && b <= 126) {
			result = append(result, b)
		} else if b > 126 {
			// For high-bit characters, try to keep UTF-8 characters
			result = append(result, b)
		}
		// Ignore other control characters
	}

	return result
}

// runCommandToFileClean runs command and writes cleaned output to file
func runCommandToFileClean(f *os.File, command string, args []string) error {
	cmd := exec.Command(command, args...)

	// Set timeout
	timeout := time.Duration(timeoutSec) * time.Second
	timer := time.AfterFunc(timeout, func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	})
	defer timer.Stop()

	// Capture command output
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Even if command fails, try to write output
		fmt.Fprintf(f, "Command failed: %v\nOutput:\n", err)
	}

	// Clean output and write to file
	cleanOutput := cleanTextOutput(output)
	_, writeErr := f.Write(cleanOutput)

	return writeErr
}

func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

func runCommandToFile(f *os.File, command string, args []string) error {
	// Use cleaned version of the function
	return runCommandToFileClean(f, command, args)
}

func writeWarning(diag *DiagnosticInfo, warning string) {
	f, err := os.OpenFile(diag.WarningsLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	warningText := fmt.Sprintf("[%s] %s\n", timestamp, warning)

	// Clean warning text to ensure it's plain text
	cleanedText := cleanTextOutput([]byte(warningText))
	f.Write(cleanedText)
}

func getMySQLPids() []string {
	var pids []string

	cmd := exec.Command("pidof", "mysqld")
	output, err := cmd.Output()
	if err != nil {
		return pids
	}

	pidStr := strings.TrimSpace(string(output))
	if pidStr != "" {
		pids = strings.Fields(pidStr)
	}

	return pids
}

func getDataDirsFromConfig(configPath string) ([]string, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var dataDirs []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "datadir") && strings.Contains(line, "=") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				dataDir := strings.TrimSpace(parts[1])
				dataDirs = append(dataDirs, dataDir)
			}
		}
	}

	return dataDirs, scanner.Err()
}

func collectDirectoryListing(dir, outputFile string) error {
	f, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "\n========= Directory %s file listing =========\n", dir)
	return runCommandToFile(f, "ls", []string{"-lhR", dir})
}

func collectSELinuxContexts(dir, outputFile string) error {
	f, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "\n========= Directory %s SELinux contexts =========\n", dir)
	return runCommandToFile(f, "ls", []string{"-lRZ", dir})
}

func getMySQLErrorLogPath(configPath, dataDir string) (string, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	inMysqldSection := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "[mysqld]" {
			inMysqldSection = true
			continue
		}

		if strings.HasPrefix(line, "[") && line != "[mysqld]" {
			inMysqldSection = false
			continue
		}

		if inMysqldSection && strings.HasPrefix(line, "log-error") && strings.Contains(line, "=") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				logPath := strings.TrimSpace(parts[1])
				if !filepath.IsAbs(logPath) {
					logPath = filepath.Join(dataDir, logPath)
				}
				return logPath, nil
			}
		}
	}

	// If no configuration found, use default path
	hostname, _ := os.Hostname()
	return filepath.Join(dataDir, hostname+".err"), nil
}

func getMySQLPidFile(configPath string) (string, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	inMysqldSection := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "[mysqld]" {
			inMysqldSection = true
			continue
		}

		if strings.HasPrefix(line, "[") && line != "[mysqld]" {
			inMysqldSection = false
			continue
		}

		if inMysqldSection && strings.HasPrefix(line, "pid-file") && strings.Contains(line, "=") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				return strings.TrimSpace(parts[1]), nil
			}
		}
	}

	return "", fmt.Errorf("pid-file configuration not found in configuration file")
}

func copyLastLines(src, dst string, lines int) error {
	cmd := exec.Command("tail", "-"+strconv.Itoa(lines), src)
	output, err := cmd.Output()
	if err != nil {
		return err
	}

	return ioutil.WriteFile(dst, output, 0644)
}

func executeMySQLQuery(db *sql.DB, query string, output io.Writer) error {
	// Handle special status commands
	if query == "STATUS;" {
		fmt.Fprintf(output, "Status: OK\n")
		return nil
	}

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Create slice to store values
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Output column names
	fmt.Fprintf(output, "%s\n", strings.Join(columns, "\t"))

	// Output data rows
	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return err
		}

		var result []string
		for _, val := range values {
			if val == nil {
				result = append(result, "NULL")
			} else {
				result = append(result, fmt.Sprintf("%v", val))
			}
		}
		fmt.Fprintf(output, "%s\n", strings.Join(result, "\t"))
	}

	return rows.Err()
}
