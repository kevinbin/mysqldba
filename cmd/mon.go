// Copyright © 2017 Hong Bin <hongbin@actionsky.com>
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
	"database/sql"

	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/logrusorgru/aurora"
	"github.com/shirou/gopsutil/load"
	"github.com/spf13/cobra"
	"math"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// monCmd represents the mon command
var monCmd = &cobra.Command{
	Use:   "monitor",
	Short: "A MySQL monitor like iostat",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		monitor()
	},
}

var (
	count   = 0
	myStat  = make(map[string]int64)
	myStat2 = make(map[string]int64)
	slave   = make(map[string]interface{})
)

var (
	interval int64

	qps, tps, threadRunning, threadConnected, threadCreated    int64
	comSelete, comInsert, comUpdate, comDelete                 int64
	innodbBpData, innodbBpFree, innodbBpDirty, innodbBpFlush   int64
	innodbReads, innodbInsert, innodbUpdate, innodbDelete      int64
	innodbBpReadRequest, innodbBpRead, innodbBpWaitFree, hit   int64
	innodbOSLogFsync, innodbOSLogWrite                         int64
	innodbLogWait, innodbLogWriteRequest, innodbLogWrite       int64
	innodbRowLockTime, innodbRowLockWait, innodbRowLockAvgWait int64
	send, received                                             int64
	gtidN                                                      int
	innodbState, netCol, thdCol, ibBufferCol, ibRowCol         bool
	redoCol, qpsCol, ibBufferHit, comCol, redoIoCol, saveCsv   bool
	slaveCol, gtidCol, cpuLoad, rowLockCol                     bool
)

type Oput struct {
	lineCSV []int64

	lineOne  string
	lineTwo  string
	lineEnd  string
	lineData string
}

func init() {
	RootCmd.AddCommand(monCmd)

	monCmd.Flags().Int64VarP(&interval, "interval", "i", 1, "interval")
	monCmd.Flags().BoolVar(&innodbState, "skip_innodb_status", false, "skip collect show engine innodb status output")
	monCmd.Flags().BoolVar(&netCol, "skip_net", false, "skip output mysql network through")
	monCmd.Flags().BoolVar(&ibBufferCol, "skip_ib_buffer", false, "skip output innodb buffer page status")
	monCmd.Flags().BoolVar(&ibRowCol, "skip_ib_row", false, "skip output innodb row operate")
	monCmd.Flags().BoolVar(&thdCol, "skip_thread", false, "skip output thread status")
	monCmd.Flags().BoolVar(&redoIoCol, "skip_redo_io", false, "skip output innodb redo log io operate")
	monCmd.Flags().BoolVar(&redoCol, "skip_redo", false, "skip output innodb redo log fsync operate")
	monCmd.Flags().BoolVar(&qpsCol, "skip_qps", false, "skip output query per second")
	monCmd.Flags().BoolVar(&ibBufferHit, "skip_ib_hit", false, "skip output innodb buffer hit operate")
	monCmd.Flags().BoolVar(&comCol, "skip_com", false, "skip output mysql I/D/U/S operate")
	monCmd.Flags().BoolVar(&slaveCol, "skip_slave", false, "skip output slave status")
	monCmd.Flags().BoolVar(&gtidCol, "skip_gtid", false, "skip output slave Retrieved & Executed gtid diff ")
	monCmd.Flags().BoolVar(&cpuLoad, "skip_load", false, "skip output system load")
	monCmd.Flags().BoolVar(&saveCsv, "csv", false, "save output to csv file ")
	monCmd.Flags().BoolVar(&rowLockCol, "row_lock", false, "output innodb row lock load")

}

func ife(condition bool, trueVal, falseVal interface{}) interface{} {
	if condition {
		return trueVal
	}
	return falseVal
}
func abs(i int64) int64 {

	return int64(math.Abs(float64(i)))
}
func showEngineInnodb(db *sql.DB) {

	if !innodbState {
		// show engine innodb status
		rows, err := db.Query(innodbStatusSQL)
		ifErrWithLog(err)
		defer rows.Close()

		var typeCol, nameCol, statusCol string

		if rows.Next() {
			err := rows.Scan(&typeCol, &nameCol, &statusCol)
			ifErrWithLog(err)
		}
		// save innodb status resulte
		fileName := "innodbstatus" // this file save show engine innodb status output
		f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		ifErrWithPanic(err)

		_, err = f.WriteString(statusCol)
		ifErrWithPanic(err)
	}
}

func showGlobalStatus(db *sql.DB) {
	rows, err := db.Query(globalStatusSQL)
	ifErrWithLog(err)
	defer rows.Close()

	for rows.Next() {
		var n string
		var v int64
		err := rows.Scan(&n, &v)
		ifErrWithLog(err)
		myStat[n] = v
	}
	err = rows.Err()
	ifErrWithLog(err)

	tps = (myStat["Com_commit"] + myStat["Com_rollback"] - myStat2["Com_commit"] - myStat2["Com_rollback"]) / interval
	qps = (myStat["Queries"] - myStat2["Queries"]) / interval
	comSelete = (myStat["Com_select"] - myStat2["Com_select"]) / interval
	comInsert = (myStat["Com_insert"] - myStat2["Com_insert"]) / interval
	comUpdate = (myStat["Com_update"] - myStat2["Com_update"]) / interval
	comDelete = (myStat["Com_delete"] - myStat2["Com_delete"]) / interval

	threadRunning = myStat["Threads_running"]
	threadConnected = myStat["Threads_connected"]
	threadCreated = (myStat["Threads_created"] - myStat2["Threads_created"]) / interval

	innodbBpData = myStat["Innodb_buffer_pool_pages_data"]
	innodbBpFree = myStat["Innodb_buffer_pool_pages_free"]
	innodbBpDirty = myStat["Innodb_buffer_pool_pages_dirty"]
	innodbBpFlush = (myStat["Innodb_buffer_pool_pages_flushed"] - myStat2["Innodb_buffer_pool_pages_flushed"]) / interval

	innodbReads = (myStat["Innodb_rows_read"] - myStat2["Innodb_rows_read"]) / interval
	innodbInsert = (myStat["Innodb_rows_inserted"] - myStat2["Innodb_rows_inserted"]) / interval
	innodbUpdate = (myStat["Innodb_rows_updated"] - myStat2["Innodb_rows_updated"]) / interval
	innodbDelete = (myStat["Innodb_rows_deleted"] - myStat2["Innodb_rows_deleted"]) / interval

	innodbBpReadRequest = (myStat["Innodb_buffer_pool_read_requests"] - myStat2["Innodb_buffer_pool_read_requests"]) / interval
	innodbBpRead = (myStat["Innodb_buffer_pool_reads"] - myStat2["Innodb_buffer_pool_reads"]) / interval
	innodbBpWaitFree = myStat["Innodb_buffer_pool_wait_free"]

	if innodbBpReadRequest > 0 {
		hit = 100 - (myStat["Innodb_buffer_pool_reads"]/myStat["Innodb_buffer_pool_read_requests"])*100
	}

	innodbOSLogFsync = (myStat["Innodb_os_log_fsyncs"] - myStat2["Innodb_os_log_fsyncs"]) / interval
	innodbOSLogWrite = (myStat["Innodb_os_log_written"] - myStat2["Innodb_os_log_written"]) / interval

	innodbLogWait = myStat["Innodb_log_waits"]
	innodbLogWriteRequest = (myStat["Innodb_log_write_requests"] - myStat2["Innodb_log_write_requests"]) / interval
	innodbLogWrite = (myStat["Innodb_log_writes"] - myStat2["Innodb_log_writes"]) / interval

	innodbRowLockTime = (myStat["Innodb_row_lock_time"] - myStat2["Innodb_row_lock_time"]) / 1000 // seconds
	innodbRowLockWait = myStat["Innodb_row_lock_waits"] - myStat2["Innodb_row_lock_waits"]
	if innodbRowLockWait == 0 {
		innodbRowLockAvgWait = 0
	} else {
		innodbRowLockAvgWait = innodbRowLockTime / innodbRowLockWait
	}

	send = (myStat["Bytes_sent"] - myStat2["Bytes_sent"]) / interval
	received = (myStat["Bytes_received"] - myStat2["Bytes_received"]) / interval

	for k, v := range myStat {
		myStat2[k] = v
	}
}

func showSlaveStatus(db *sql.DB) {
	rows, err := db.Query(slaveStatusSQL)
	ifErrWithLog(err)
	defer rows.Close()
	if rows.Next() {

		columns, _ := rows.Columns()
		values := make([]interface{}, len(columns))
		for i := range values {
			var v sql.RawBytes
			values[i] = &v
		}

		err = rows.Scan(values...)
		ifErrWithPanic(err)

		for i, name := range columns {
			bp := values[i].(*sql.RawBytes)
			vs := string(*bp)
			vi, err := strconv.ParseInt(vs, 10, 64)
			if err != nil {
				slave[name] = vs
			} else {
				slave[name] = vi
			}
		}
	}
	err = rows.Err()
	ifErrWithLog(err)
}

func strToInt(s string) int {
	v, err := strconv.Atoi(s)
	ifErrWithPanic(err)
	return v
}

func gtidSub(s string) int {
	var gtidDiff int
	if s != "" {
		gtidAll := strings.Split(s, ",")
		for _, v := range gtidAll {
			gtidNum := strings.Split(strings.Split(v, ":")[1], "-")
			gtidDiff += strToInt(gtidNum[1]) - strToInt(gtidNum[0])
		}

	}
	return gtidDiff
}

func rename(f string) {
	if _, err := os.Stat(f); err == nil {
		n := f + "_" + time.Now().Format("20060102150405")
		os.Rename(f, n)
		fmt.Printf("Output file: %s \n", n)
	}
}

func monitor() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("Exiting..")
		rename("innodbstatus")
		rename("monitor")
		os.Exit(1)
	}()
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/", dbUser, dbPassWd, dbHost, dbPort)

	db := mysqlConnect(dsn)

	for {
		showSlaveStatus(db)

		showEngineInnodb(db)
		showGlobalStatus(db)

		// skip first line
		if count < 1 {
			count++
			continue
		}
		o := Oput{lineOne: "+-------+", lineTwo: "--Time--|", lineEnd: "--------+"}

		t := time.Now()
		o.lineData += fmt.Sprintf("%8v|", aurora.Blue(t.Format("15:04:05")))
		o.lineCSV = append(o.lineCSV, int64(t.Unix()))
		if !cpuLoad {

			o.lineOne += "---Sys Load---+"
			o.lineTwo += "  1m   5m  15m|"
			o.lineEnd += "--------------+"
			load, _ := load.Avg()
			o.lineData += fmt.Sprintf("%4v %4v %4v|", load.Load1, load.Load5, load.Load15)
			o.lineCSV = append(o.lineCSV, int64(load.Load1), int64(load.Load5), int64(load.Load15))

		}
		if !qpsCol {
			o.lineOne += "----Through----+"
			o.lineTwo += "  QPS  |  TPS  |"
			o.lineEnd += "-------+-------+"
			o.lineData += fmt.Sprintf("%7v|%7v|", qps, tps)
			o.lineCSV = append(o.lineCSV, qps, tps)

		}

		if !thdCol {
			o.lineOne += "----Thread----+"
			o.lineTwo += " run  conn new|"
			o.lineEnd += "--------------+"
			o.lineData += fmt.Sprintf("%4v %5v %3v|", threadRunning, threadConnected, threadCreated)
			o.lineCSV = append(o.lineCSV, threadRunning, threadConnected, threadCreated)

		}
		if !comCol {
			o.lineOne += "---------Com_Query---------+"
			o.lineTwo += "select insert update delete|"
			o.lineEnd += "---------------------------+"
			o.lineData += fmt.Sprintf("%7v %6v %6v %6v|", comSelete, comInsert, comUpdate, comDelete)
			o.lineCSV = append(o.lineCSV, comSelete, comInsert, comUpdate, comDelete)

		}
		if !ibBufferCol {
			o.lineOne += "--Innodb Buffer State--+"
			o.lineTwo += " data  free dirty flush|"
			o.lineEnd += "-----------------------+"
			o.lineData += fmt.Sprintf("%5s %5v %5s %5v|", numHumen(innodbBpData), innodbBpFree, numHumen(innodbBpDirty), innodbBpFlush)
			o.lineCSV = append(o.lineCSV, innodbBpData, innodbBpFree, innodbBpDirty, innodbBpFlush)

		}
		if !ibRowCol {
			o.lineOne += "------Innodb Row State-----+"
			o.lineTwo += "select insert update delete|"
			o.lineEnd += "---------------------------+"
			o.lineData += fmt.Sprintf("%6s %6s %6s %6s|", numHumen(innodbReads), numHumen(innodbInsert), numHumen(innodbUpdate), numHumen(innodbDelete))
			o.lineCSV = append(o.lineCSV, innodbReads, innodbInsert, innodbUpdate, innodbDelete)

		}

		if !ibBufferHit {
			o.lineOne += "--Innodb BP Request--+"
			o.lineTwo += "logic physic wait hit|"
			o.lineEnd += "---------------------+"
			o.lineData += fmt.Sprintf("%5s %5s %5v %3v|", numHumen(innodbBpReadRequest), numHumen(innodbBpRead), innodbBpWaitFree, ife(hit > 99, aurora.Green(hit), aurora.Red(hit)))
			o.lineCSV = append(o.lineCSV, innodbBpReadRequest, innodbBpRead, innodbBpWaitFree, hit)

		}

		if !redoCol {
			o.lineOne += "--Redo Log--+"
			o.lineTwo += "fsync writen|"
			o.lineEnd += "------------+"
			o.lineData += fmt.Sprintf("%4v %7v|", innodbOSLogFsync, innodbOSLogWrite)
			o.lineCSV = append(o.lineCSV, innodbOSLogFsync, innodbOSLogWrite)

		}

		if rowLockCol {
			o.lineOne += "--Inno Row Lock--+"
			o.lineTwo += " waits  time  t/w|"
			o.lineEnd += "-----------------+"
			o.lineData += fmt.Sprintf("%5v %5v %4v|", innodbRowLockWait, innodbRowLockTime, innodbRowLockAvgWait)
			o.lineCSV = append(o.lineCSV, innodbRowLockWait, innodbRowLockTime, innodbRowLockAvgWait)

		}

		if !redoIoCol {
			o.lineOne += "---RedoLog IO---+"
			o.lineTwo += "wait  logic  phy|"
			o.lineEnd += "----------------+"
			o.lineData += fmt.Sprintf("%4v %5v %5v|", ife(innodbLogWait > 1, aurora.Red(innodbLogWait), aurora.Green(innodbLogWait)), innodbLogWriteRequest, innodbLogWrite)
			o.lineCSV = append(o.lineCSV, innodbLogWait, innodbLogWriteRequest, innodbLogWrite)

		}
		if !slaveCol && slave["Slave_IO_Running"] != nil {

			o.lineOne += "---Slave Status---+"
			o.lineTwo += " io sql delay errn|"
			o.lineEnd += "------------------+"

			o.lineData += fmt.Sprintf("%3v %3v %5v %4v|", ife(slave["Slave_IO_Running"] == "Yes", aurora.Green(slave["Slave_IO_Running"]), aurora.Red(slave["Slave_IO_Running"])), ife(slave["Slave_SQL_Running"] == "Yes", aurora.Green(slave["Slave_SQL_Running"]), aurora.Red(slave["Slave_SQL_Running"])), slave["Seconds_Behind_Master"], slave["Last_Errno"])

		}

		if !slaveCol && !gtidCol && slave["Retrieved_Gtid_Set"] != nil {
			gtidSQL := fmt.Sprintf("select gtid_subtract('%s', '%s') as gtid_diff", slave["Retrieved_Gtid_Set"], slave["Executed_Gtid_Set"])
			gtidN = gtidSub(mysqlSimpleQuery(gtidSQL, db))

			o.lineOne += "-GTID-+"
			o.lineTwo += "  num |"
			o.lineEnd += "------+"

			o.lineData += fmt.Sprintf("%6v|", ife(gtidN <= 100, aurora.Green(gtidN), aurora.Red(gtidN)))

		}

		if !netCol {
			o.lineOne += "----Net----+"
			o.lineTwo += " send  recv|"
			o.lineEnd += "-----------+"
			o.lineData += fmt.Sprintf("%5s", byteHumen(send))
			o.lineData += fmt.Sprintf(" ")
			o.lineData += fmt.Sprintf("%5s", byteHumen(received))
			o.lineData += fmt.Sprintf("|")

			o.lineCSV = append(o.lineCSV, send, received)

		}

		if count == 1 || count > scroll {
			fmt.Println(aurora.Cyan(o.lineOne))
			fmt.Println(aurora.Cyan(o.lineTwo))
			fmt.Println(aurora.Cyan(o.lineEnd))
			count = 1
		}

		fmt.Println(o.lineData)
		if saveCsv {
			csv := []string{}
			r := regexp.MustCompile(`\b(\s+)`)
			n := strings.Replace(strings.Replace(o.lineTwo, "|", "", -1), "--", " ", -1)
			n = r.ReplaceAllString(n, ",")

			for i := range o.lineCSV {
				csv = append(csv, strconv.FormatInt(o.lineCSV[i], 10))
			}

			fileName := "monitor" // this file save monitor output
			f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
			ifErrWithPanic(err)

			_, err = f.WriteString(strings.Join(csv, ",") + "\n")
			ifErrWithPanic(err)

		}

		count++
		time.Sleep(time.Second * time.Duration(interval))
	}
}
