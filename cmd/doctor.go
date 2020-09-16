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
	"bytes"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/hpcloud/tail"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
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

type logInfo struct {
	restartTime, semaphoreTime, writerState        []string
	mysqlVer                                       string
	restarts, semaphores                           int
	waitPoint, writer, lastWriteLock, lastReadLock map[string][]string
}

func init() {
	RootCmd.AddCommand(doctorCmd)

	// Analysis after a crash
	doctorCmd.Flags().StringVarP(&filename, "file", "f", "", "MySQL error log file")

	//Observed before the crash
	doctorCmd.Flags().BoolVarP(&watch, "watch", "w", false, "watch MySQL error log. Collect thread info if discovery semaphore wait ")
}
func watchErroLog() {

	dsn := fmt.Sprintf("%s:%s@(%s:%d)/", dbUser, dbPassWd, dbHost, dbPort)

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
			defer rows.Close()

			for rows.Next() {
				var clientUser, clientHost, digestText string
				var osid, procid int

				err := rows.Scan(&osid, &procid, &clientUser, &clientHost, &digestText)

				result := fmt.Sprintf("%v THREAD_OS_ID:%v PROCESSLIST_ID:%v PROCESSLIST_USER:%v PROCESSLIST_HOST:%v DIGEST_TEXT:%v \n", line.Time.Format(time.RFC3339), osid, procid, clientUser, clientHost, digestText)

				_, err = f.WriteString(result)
				ifErrWithPanic(err)
			}

			err = rows.Err()
			ifErrWithLog(err, "")
		}

	}
}
func anaylzeErrorLog(filename string) (lg logInfo) {
	var waitKey, writerKey string
	lg = logInfo{}
	lg.waitPoint = make(map[string][]string)
	lg.writer = make(map[string][]string)
	lg.lastWriteLock = make(map[string][]string)
	lg.lastReadLock = make(map[string][]string)

	f, err := os.Open(filename)
	ifErrWithLog(err, " No error log file specified")

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "Version:") {
			lg.mysqlVer = strings.Fields(scanner.Text())[1]
		}
		if strings.Contains(scanner.Text(), "ready for connections") {
			lg.restarts++
			t := formatTime(strings.Fields(scanner.Text())[0])
			lg.restartTime = append(lg.restartTime, t)
		}

		if strings.Contains(scanner.Text(), "Semaphore wait has lasted > 600 seconds") {
			lg.semaphores++
			t := formatTime(strings.Fields(scanner.Text())[0])
			lg.semaphoreTime = append(lg.semaphoreTime, t)
		}

		// --Thread 140202719565568 has waited at ha_innodb.cc line 14791 for 244.00 seconds the semaphore:
		if strings.Contains(scanner.Text(), "has waited at") {

			str := strings.Fields(scanner.Text())
			// str[5]: ha_innodb.cc  str[7]: 14791 str[1]: 140202719565568
			waitKey = fmt.Sprintf("%s:%s", str[5], str[7])
			lg.waitPoint[waitKey] = append(lg.waitPoint[waitKey], str[1])
		}

		//a writer (thread id 140617682765568) has reserved it in mode  exclusive
		if strings.Contains(scanner.Text(), "has reserved it in mode") {
			writerKey = strings.Trim(strings.Fields(scanner.Text())[4], ")")
			lg.writer[writerKey] = append(lg.writer[writerKey], waitKey)
		}

		if strings.Contains(scanner.Text(), fmt.Sprintf("OS thread handle %s", writerKey)) {

			lg.writerState = append(lg.writerState, scanner.Text())

		}

		//Last time read locked in file row0purge.cc line 861
		if strings.Contains(scanner.Text(), "Last time read locked") {
			str := strings.Fields(scanner.Text())
			lg.lastReadLock[writerKey] = append(lg.lastReadLock[writerKey], fmt.Sprintf("%s:%s", str[6], str[8]))
		}

		//Last time write locked in file /usr/local/mysql-install/storage/innobase/dict/dict0stats.cc line 2366
		if strings.Contains(scanner.Text(), "Last time write locked") {
			str := strings.Split(scanner.Text(), "/")
			lg.lastWriteLock[writerKey] = append(lg.lastWriteLock[writerKey], strings.Replace(str[len(str)-1], " line ", ":", 1))
		}

	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	return lg
}

func logOutput(lg logInfo) {
	fmt.Printf("MySQL Server Version: %s\n", lg.mysqlVer)
	fmt.Print("\n********** MySQL service start count **********\n")
	fmt.Printf("MySQL Semaphore crash -> %v times %q\n", lg.semaphores, lg.semaphoreTime)
	fmt.Printf("  MySQL Service start -> %v times %q\n", lg.restarts, lg.restartTime)

	fmt.Print("\n********** Which thread waited lock **********")
	for k, v := range lg.waitPoint {
		fmt.Printf("\n%20s -> %3v  %v\n", k, len(v), unique(v))
	}

	fmt.Print("\n********** Which writer threads block at **********")
	for k, v := range lg.writer {
		fmt.Printf("\n%20s -> %3v  %v\n", k, len(v), unique(v))
	}

	fmt.Print("\n********** These writer threads trx state **********\n")
	for _, v := range lg.writerState {
		fmt.Println(v)
	}

	fmt.Print("\n********** These writer threads at last time reads locked **********")
	for k, v := range lg.lastReadLock {
		fmt.Printf("\n%20s -> %3v  %v\n", k, len(v), unique(v))
	}

	fmt.Print("\n********** These writer threads at last time write locked **********")
	for k, v := range lg.lastWriteLock {
		fmt.Printf("\n%20s -> %3v  %v\n", k, len(v), unique(v))
	}
}

func sortMapByValue(m map[string]uint64) string {
	type kv struct {
		Key   string
		Value uint64
	}
	b := new(bytes.Buffer)
	var ss []kv
	for k, v := range m {
		ss = append(ss, kv{k, v})
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value > ss[j].Value
	})

	for _, kv := range ss {
		fmt.Fprintf(b, "%s:(%v) ", kv.Key, kv.Value)
	}
	return b.String()
}

func unique(intSlice []string) string {
	counter := make(map[string]uint64)
	for _, row := range intSlice {
		counter[row]++
	}
	return sortMapByValue(counter)
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
