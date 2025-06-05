// Copyright © 2018 Hong Bin <hongbin119@gmail.com>
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
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hpcloud/tail"
	"github.com/spf13/cobra"
)

// doctorCmd represents the doctor command
var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "mysql error log anaylze & watch",
	Long:  `Anaylze & watch mysql error log about semaphore crash`,
	Run: func(cmd *cobra.Command, args []string) {

		if watch {
			watchErroLog()
		} else {
			logOutput(anaylzeErrorLog(filename))
		}
	},
}

var (
	filename string
	watch    bool
)

// 颜色常量
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
)

type logInfo struct {
	restartTime, semaphoreTime, writerState        []string
	mysqlVer                                       string
	restarts, semaphores                           int
	waitPoint, writer, lastWriteLock, lastReadLock map[string][]string
	waiterToWriter                                 map[string]string
	latchReaders                                   map[string][]string // latch地址 -> []reader threadId
	threadWaitLatch                                map[string]string   // threadId -> latch地址
	latchType                                      map[string]string   // latchAddr -> S-lock/X-lock/SX-lock
	latchCreated                                   map[string]string   // latchAddr -> 创建位置
	writerMode                                     map[string]string   // writerId -> reserved mode
	threadWaitInfo                                 map[string]string   // threadId -> 等待信息（文件:行号）
	threadWaitSeconds                              map[string]string   // threadId -> 等待时长
	threadLockType                                 map[string]string   // threadId -> 该线程请求的锁类型
}

// 新增：logInfo初始化函数
func newLogInfo() logInfo {
	return logInfo{
		waitPoint:         make(map[string][]string),
		writer:            make(map[string][]string),
		lastWriteLock:     make(map[string][]string),
		lastReadLock:      make(map[string][]string),
		waiterToWriter:    make(map[string]string),
		latchReaders:      make(map[string][]string),
		threadWaitLatch:   make(map[string]string),
		latchType:         make(map[string]string),
		latchCreated:      make(map[string]string),
		writerMode:        make(map[string]string),
		threadWaitInfo:    make(map[string]string),
		threadWaitSeconds: make(map[string]string),
		threadLockType:    make(map[string]string),
	}
}

// 新增：预编译的正则表达式
var (
	versionRegex    = regexp.MustCompile(`Version:\s*'([^']+)'`)
	threadWaitRegex = regexp.MustCompile(`--Thread ([0-9]+) has waited at ([^ ]+) line ([0-9]+) for ([0-9]+) seconds`)
	lockRegex       = regexp.MustCompile(`((?:S|X|SX)-lock)(?: \(([^)]+)\))? on RW-latch at ([^ ]+) created in file ([^ ]+) line ([0-9]+)`)
	readerRegex     = regexp.MustCompile(`thread id ([0-9]+)\)`)
	writerRegex     = regexp.MustCompile(`a writer \(thread id ([0-9]+)\) has reserved it in mode ([a-zA-Z ]+)`)
)

// 新增：通用字符串处理函数
func parseTimeFromLine(line string) string {
	fields := strings.Fields(line)
	if len(fields) > 0 {
		return formatTime(fields[0])
	}
	return ""
}

// 新增：构建DSN字符串，消除重复代码
func buildDSN() string {
	return fmt.Sprintf("%s:%s@(%s:%d)/", dbUser, dbPassWd, dbHost, dbPort)
}

func init() {
	RootCmd.AddCommand(doctorCmd)

	// Analysis after a crash
	doctorCmd.Flags().StringVarP(&filename, "file", "f", "", "MySQL error log file")

	//Observed before the crash
	doctorCmd.Flags().BoolVarP(&watch, "watch", "w", false, "watch MySQL error log. Collect thread info if discovery semaphore wait ")
}

func watchErroLog() {

	dsn := buildDSN()

	db := mysqlConnect(dsn)
	var logfile, datadir string
	db.QueryRow("select @@log_error as logfile;").Scan(&logfile)
	db.QueryRow("select @@datadir as datadir;").Scan(&datadir)

	if filepath.Dir(logfile) == "." {
		logfile = fmt.Sprintf("%s/%s", datadir, logfile)
	}

	t, err := tail.TailFile(logfile, tail.Config{Follow: true, ReOpen: true, Location: &tail.SeekInfo{Offset: 0, Whence: 2}, MustExist: true})
	ifErrWithLog(err, "")

	// SAVE QUERY RESULT
	f, err := os.OpenFile("watch.log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	fmt.Println("collecting info save watch.log")
	ifErrWithPanic(err)
	defer f.Close()

	for line := range t.Lines {
		if strings.Contains(line.Text, "has reserved it in mode") {
			fmt.Println(line.Time.Format(time.RFC3339), line.Text)
			thd := strings.Trim(strings.Split(line.Text, " ")[4], ")")

			query := fmt.Sprintf("select /* mysqldba */ THREAD_OS_ID,PROCESSLIST_ID,PROCESSLIST_USER,PROCESSLIST_HOST,DIGEST_TEXT from performance_schema.threads t join performance_schema.events_statements_current e using(THREAD_ID) where t.THREAD_OS_ID='%s';", thd)

			rows, err := db.Query(query)
			ifErrWithLog(err, "")

			for rows.Next() {
				var clientUser, clientHost, digestText string
				var osid, procid int

				err := rows.Scan(&osid, &procid, &clientUser, &clientHost, &digestText)
				ifErrWithPanic(err)

				result := fmt.Sprintf("%v THREAD_OS_ID:%v PROCESSLIST_ID:%v PROCESSLIST_USER:%v PROCESSLIST_HOST:%v DIGEST_TEXT:%v \n", line.Time.Format(time.RFC3339), osid, procid, clientUser, clientHost, digestText)

				_, err = f.WriteString(result)
				ifErrWithPanic(err)
			}

			err = rows.Err()
			ifErrWithLog(err, "")
			rows.Close()
		}

	}
}

func anaylzeErrorLog(filename string) (lg logInfo) {
	var waitKey, writerKey string
	lg = newLogInfo() // 使用新的初始化函数

	f, err := os.Open(filename)
	ifErrWithLog(err, " No error log file specified")

	scanner := bufio.NewScanner(f)

	var currentThreadId string
	latchToThread := make(map[string]string)
	var currentLatchAddr string // 新增：记录当前正在处理的Latch地址

	for scanner.Scan() {
		line := scanner.Text()

		// 使用预编译的正则表达式
		if strings.Contains(line, "Version:") {
			if match := versionRegex.FindStringSubmatch(line); len(match) > 1 {
				lg.mysqlVer = match[1]
			}
		}

		if strings.Contains(line, "mysqld: ready for connections") {
			lg.restarts++
			lg.restartTime = append(lg.restartTime, parseTimeFromLine(line))
		}

		if strings.Contains(line, "Semaphore wait has lasted > 600 seconds") {
			lg.semaphores++
			lg.semaphoreTime = append(lg.semaphoreTime, parseTimeFromLine(line))
		}

		if strings.HasPrefix(line, "--Thread ") && strings.Contains(line, " has waited at ") {
			fields := strings.Fields(line)
			if len(fields) > 1 {
				currentThreadId = fields[1]
			}
			if len(fields) > 7 {
				waitKey = fmt.Sprintf("%s:%s", fields[5], fields[7])
				lg.waitPoint[waitKey] = append(lg.waitPoint[waitKey], fields[1])
			}

			// 使用预编译的正则表达式
			if match := threadWaitRegex.FindStringSubmatch(line); len(match) > 4 {
				threadId := match[1]
				file := match[2]
				lineNum := match[3]
				seconds := match[4]
				lg.threadWaitInfo[threadId] = fmt.Sprintf("%s:%s", file, lineNum)
				lg.threadWaitSeconds[threadId] = seconds
			}
		}

		// 块内遇到锁信息都归属于currentThreadId
		if strings.Contains(line, "-lock") && strings.Contains(line, "on RW-latch at") && strings.Contains(line, "created in file") {
			if match := lockRegex.FindStringSubmatch(line); len(match) > 4 && currentThreadId != "" {
				lockType := match[1]
				lockMode := match[2] // 如 wait_ex
				latchAddr := match[3]
				createdFile := match[4]
				createdLine := match[5]

				fullLockType := lockType
				if lockMode != "" {
					fullLockType = fmt.Sprintf("%s (%s)", lockType, lockMode)
				}

				createdPos := fmt.Sprintf("%s:%s", createdFile, createdLine)
				lg.threadWaitLatch[currentThreadId] = latchAddr
				latchToThread[latchAddr] = currentThreadId
				lg.threadLockType[currentThreadId] = fullLockType // 以线程ID为key存储该线程请求的锁类型
				lg.latchType[latchAddr] = lockType                // latch基础类型（不含模式）
				lg.latchCreated[latchAddr] = createdPos
				currentLatchAddr = latchAddr // 新增：记录当前Latch地址
			}
		}

		if strings.Contains(line, "number of readers") && strings.Contains(line, "thread id") {
			all := readerRegex.FindAllStringSubmatch(line, -1)
			// 只给当前正在处理的Latch加reader
			if currentLatchAddr != "" {
				for _, m := range all {
					if len(m) > 1 {
						lg.latchReaders[currentLatchAddr] = append(lg.latchReaders[currentLatchAddr], m[1])
					}
				}
			}
		}

		if strings.Contains(line, "a writer (thread id ") && strings.Contains(line, ") has reserved it in mode") {
			if match := writerRegex.FindStringSubmatch(line); len(match) > 2 && currentThreadId != "" {
				writerId := match[1]
				mode := strings.TrimSpace(match[2])
				lg.waiterToWriter[currentThreadId] = writerId
				writerKey = writerId
				lg.writer[writerKey] = append(lg.writer[writerKey], waitKey)
				lg.writerMode[writerId] = mode
			}
		}

		if strings.Contains(line, fmt.Sprintf("OS thread handle %s", writerKey)) {
			lg.writerState = append(lg.writerState, line)
		}

		if strings.Contains(line, "Last time read locked") {
			str := strings.Fields(line)
			if len(str) > 8 {
				lg.lastReadLock[writerKey] = append(lg.lastReadLock[writerKey], fmt.Sprintf("%s:%s", str[6], str[8]))
			}
		}

		if strings.Contains(line, "Last time write locked") {
			str := strings.Split(line, "/")
			if len(str) > 0 {
				lg.lastWriteLock[writerKey] = append(lg.lastWriteLock[writerKey], strings.Replace(str[len(str)-1], " line ", ":", 1))
			}
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	return lg
}

func logOutput(lg logInfo) {
	fmt.Printf("MySQL Version: https://github.com/mysql/mysql-server/blob/mysql-%s\n", lg.mysqlVer)
	fmt.Printf("MySQL Crash(s): %d次 %q\n", lg.semaphores, lg.semaphoreTime)
	fmt.Printf("MySQL Start(s): %d次 %q\n", lg.restarts, lg.restartTime)
	printLockDependency(lg)
}

// 递归输出锁链式阻塞依赖链，最大深度5
func printLockDependency(lg logInfo) {
	maxDepth := 10
	const maxShowWaiters = 5
	// 新增：提取函数来收集和排序latch等待者
	latchWaiters := collectLatchWaiters(lg)

	// 排序 latchAddr
	var latchAddrs []string
	for latchAddr := range latchWaiters {
		latchAddrs = append(latchAddrs, latchAddr)
	}
	sort.Strings(latchAddrs)
	latchIdx := 1 // 新增：锁序号
	for _, latchAddr := range latchAddrs {
		waiters := uniqueSlice(latchWaiters[latchAddr])
		showWaiters := waiters
		if len(waiters) > maxShowWaiters {
			showWaiters = append(waiters[:maxShowWaiters], "...")
		}
		ltype := lg.latchType[latchAddr]
		lcreated := lg.latchCreated[latchAddr]
		fmt.Printf("🔒%s[%d]%s Latch %s%s%s (%s%s%s, created %s): %s%d%s threads waiting: [",
			ColorYellow, latchIdx, ColorReset,
			ColorYellow, latchAddr, ColorReset,
			ColorGreen, ltype, ColorReset, lcreated,
			ColorYellow, len(waiters), ColorReset)
		for i, tid := range showWaiters {
			fmt.Printf("%s%s%s", ColorBlue, tid, ColorReset)
			if i != len(showWaiters)-1 {
				fmt.Print(" ")
			}
		}
		fmt.Print("]\n")

		if len(waiters) > 0 {
			visitedThreads := make(map[string]bool)
			latchVisited := make(map[string]bool) // 每个latch独立的visited状态
			printThreadChain(lg, waiters[0], 0, maxDepth, visitedThreads, latchVisited)
		}
		fmt.Print("\n")
		latchIdx++ // 新增：递增序号
	}
}

// 递归打印单个线程的依赖链，包括writer和reader依赖，latchAddr去重
func printThreadChain(lg logInfo, threadId string, depth, maxDepth int, visitedThreads map[string]bool, latchVisited map[string]bool) {
	if depth > maxDepth {
		fmt.Printf("%s%s (max depth reached)\n", strings.Repeat("  ", depth), threadId)
		return
	}

	visitedThreads[threadId] = true
	indent := strings.Repeat("   ", depth)
	writerId, hasWriter := lg.waiterToWriter[threadId]
	latchAddr, hasLatch := lg.threadWaitLatch[threadId]
	ltype := ""
	if hasLatch {
		ltype = lg.latchType[latchAddr]
	}

	if hasWriter {
		mode := lg.writerMode[writerId]
		waitInfo := lg.threadWaitInfo[writerId]
		waitSeconds := lg.threadWaitSeconds[writerId]
		if visitedThreads[writerId] {
			fmt.Printf("%s  🔁 (cycle detected) writer thread [%s%s%s]\n", indent, ColorRed, writerId, ColorReset)
			return
		}

		// 显示 writer 线程请求的锁信息
		writerLatchAddr := lg.threadWaitLatch[writerId]
		writerLatchType := lg.threadLockType[writerId] // 改为直接从线程ID获取该线程请求的锁类型
		writerLatchCreated := lg.latchCreated[writerLatchAddr]

		fmt.Printf("%s  ⛔ -> Blocked by writer thread [%s%s%s] (reserved mode: %s)\n", indent, ColorRed, writerId, ColorReset, mode)
		if waitInfo != "" && waitSeconds != "" {
			fmt.Printf("%s      -> Writer thread waited at %s for %s seconds\n", indent, waitInfo, waitSeconds)
		}
		if writerLatchAddr != "" && writerLatchType != "" {
			fmt.Printf("%s      -> Requesting %s%s%s on RW-latch at %s%s%s (created %s)\n", indent, ColorGreen, writerLatchType, ColorReset, ColorYellow, writerLatchAddr, ColorReset, writerLatchCreated)
		}

		if locks, ok := lg.lastReadLock[writerId]; ok {
			summary := summarizeLocks(locks)
			fmt.Printf("%s      -> Last read locked at %s\n", indent, summary)
		}

		if locks, ok := lg.lastWriteLock[writerId]; ok {
			summary := summarizeLocks(locks)
			fmt.Printf("%s      -> Last write locked at %s\n", indent, summary)
		}
		printThreadChain(lg, writerId, depth+1, maxDepth, visitedThreads, latchVisited)
		if hasLatch2, latchAddr2 := false, ""; writerId != "" {
			latchAddr2, hasLatch2 = lg.threadWaitLatch[writerId]
			if hasLatch2 && ltype == "S-lock" { // 只有S-lock waiter才递归reader
				printReaderDependency(lg, latchAddr2, indent+"     ", depth+2, maxDepth, visitedThreads, latchVisited)
			}
		}
	} else if hasLatch && ltype == "S-lock" {
		printReaderDependency(lg, latchAddr, indent+"      ", depth+1, maxDepth, visitedThreads, latchVisited)
	} else if hasLatch {
		// 对于没有明确writer信息但有latch的情况，显示基础信息
		if depth == 0 {
			// 对于第一层线程，如果没有找到writer映射但有last lock信息，显示可用信息
			fmt.Printf("%s  -> No writer mapping found for thread %s\n", indent, threadId)
			// 尝试通过latch地址查找last lock信息
			if locks, ok := lg.lastWriteLock[threadId]; ok && len(locks) > 0 {
				summary := summarizeLocks(locks)
				fmt.Printf("%s       -> Last write locked at %s\n", indent, summary)
			}
			if locks, ok := lg.lastReadLock[threadId]; ok && len(locks) > 0 {
				summary := summarizeLocks(locks)
				fmt.Printf("%s       -> Last read locked at %s\n", indent, summary)
			}
		}
	}
}

// 新增：统计锁点出现次数并格式化输出
func summarizeLocks(locks []string) string {
	counter := make(map[string]int)
	for _, l := range locks {
		counter[l]++
	}
	// 排序锁点
	var keys []string
	for k := range counter {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var result []string
	for _, k := range keys {
		v := counter[k]
		if v > 1 {
			result = append(result, fmt.Sprintf("%s (%d)", k, v))
		} else {
			result = append(result, k)
		}
	}
	return "[" + strings.Join(result, ", ") + "]"
}

// 新增：收集和排序latch等待者
func collectLatchWaiters(lg logInfo) map[string][]string {
	latchWaiters := make(map[string][]string)

	// 先收集所有waitPoint的keys并排序，确保遍历顺序一致
	var waitKeys []string
	for key := range lg.waitPoint {
		waitKeys = append(waitKeys, key)
	}
	sort.Strings(waitKeys)

	// 按排序后的顺序处理，确保结果一致
	for _, key := range waitKeys {
		threads := lg.waitPoint[key]
		for _, threadId := range uniqueSlice(threads) {
			if latchAddr, ok := lg.threadWaitLatch[threadId]; ok {
				latchWaiters[latchAddr] = append(latchWaiters[latchAddr], threadId)
			}
		}
	}

	// 对每个latch的waiters进行排序
	for latchAddr := range latchWaiters {
		sort.Strings(latchWaiters[latchAddr])
	}

	return latchWaiters
}

// 新增：打印reader依赖关系的辅助函数
func printReaderDependency(lg logInfo, latchAddr, indentPrefix string, depth, maxDepth int, visitedThreads map[string]bool, latchVisited map[string]bool) {
	if !latchVisited[latchAddr] {
		latchVisited[latchAddr] = true
		readers := uniqueSlice(lg.latchReaders[latchAddr])
		for _, readerId := range readers {
			fmt.Printf("%s📖 -> Blocked by reader thread %s%s%s (holding S-lock on latch %s%s%s)\n", indentPrefix, ColorBlue, readerId, ColorReset, ColorYellow, latchAddr, ColorReset)
			if visitedThreads[readerId] {
				fmt.Printf("%s🔁 Thread %s (cycle detected)\n", indentPrefix+"  ", readerId)
				continue
			}
			printThreadChain(lg, readerId, depth, maxDepth, visitedThreads, latchVisited)
		}
	}
}

// 新增：去重辅助
func uniqueSlice(slice []string) []string {
	m := make(map[string]struct{})
	var result []string
	for _, v := range slice {
		if _, ok := m[v]; !ok {
			m[v] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}

func formatTime(s string) string {
	layoutIn := "2006-01-02T15:04:05.000000+08:00"
	layoutOut := "2006-01-02 15:04:05"
	t, err := time.Parse(layoutIn, s)
	if err != nil {
		fmt.Println(err)
	}
	return t.Format(layoutOut)
}
