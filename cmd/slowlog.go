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
	"database/sql"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	// "strings"
	"syscall"
	"time"
)

// slowlogCmd represents the slowlog command
var slowlogCmd = &cobra.Command{
	Use:   "slowlog",
	Short: "Capture MySQL slow log ",
	Long: `Custom query time threshold, capture MySQL slow log	
For example: Set query time threshold 0.1s, capture slow log for 60 seconds.
$ mysqldba slowlog -u user -p pass -d /path -l 60 -t 0.1`,
	Run: func(cmd *cobra.Command, args []string) {
		run()
	},
}

func run() {
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/", dbUser, dbPassWd, dbHost, dbPort)
	db := mysqlConnect(dsn)
	res := getLogConfig(db)
	getSlowLog(db, res)

}

var (
	dirPath   string
	duration  int
	queryTime float32
)

func init() {
	RootCmd.AddCommand(slowlogCmd)

	slowlogCmd.Flags().IntVarP(&duration, "long", "l", 60, "How long is capture")
	slowlogCmd.Flags().Float32VarP(&queryTime, "time", "t", 0, "Long query time threshold")
	slowlogCmd.Flags().StringVarP(&dirPath, "dir", "d", ".", "Specify save directory")
}

func getLogConfig(db *sql.DB) map[string]string {
	var r = make(map[string]string)
	rows, err := db.Query(globalVariableSQL)
	ifErrWithLog(err, "")
	defer rows.Close()

	for rows.Next() {
		var n, v string
		err := rows.Scan(&n, &v)
		ifErrWithLog(err, "")
		r[n] = v
	}
	err = rows.Err()
	ifErrWithLog(err, "")
	return r
}

func restoreOption(db *sql.DB, r map[string]string) {

	sql, err := strconv.ParseFloat(r["long_query_time"], 32)
	ifErrWithLog(err, "")

	_, err = db.Exec("SET GLOBAL LONG_QUERY_TIME=?", sql)
	ifErrWithLog(err, "")

	db.Exec("SET GLOBAL SLOW_QUERY_LOG=?", r["slow_query_log"])
	db.Exec("SET GLOBAL LOG_OUTPUT=?", r["log_output"])
}

func getSlowLog(db *sql.DB, r map[string]string) {

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("Configuration restored. Exiting..")
		restoreOption(db, r)

		os.Exit(1)
	}()

	fmt.Printf("Original configuration \n slow_query_log: %v\n long_query_time: %v\n log_output: %v\n", r["slow_query_log"], r["long_query_time"], r["log_output"])

	_, err := db.Exec("SET GLOBAL SLOW_QUERY_LOG=ON")
	ifErrWithLog(err, "")
	db.Exec("SET GLOBAL LOG_OUTPUT=FILE")
	db.Exec("FLUSH SLOW LOGS")
	db.Exec("SET GLOBAL LONG_QUERY_TIME=?", queryTime)
	var slowLogFile string
	if path.Dir(r["slow_query_log_file"]) == "." {
		slowLogFile = fmt.Sprintf("%s%s", r["datadir"], r["slow_query_log_file"])
	} else {
		slowLogFile = r["slow_query_log_file"]
	}

	for i := 1; i <= duration; i++ {
		fmt.Fprintf(os.Stdout, "Query is recording to %v %vs.\r", slowLogFile, i)
		time.Sleep(time.Second)
	}

	restoreOption(db, r)

	tarFile := fmt.Sprintf("%s/%s_%vs_%s.tar.gz", dirPath, filepath.Base(r["slow_query_log_file"]), queryTime, time.Now().Format("20060102150405"))
	err = tarIt(slowLogFile, tarFile)
	ifErrWithLog(err, "")
	fmt.Printf("\nConfiguration restored. Archived to %v\n", tarFile)

}
