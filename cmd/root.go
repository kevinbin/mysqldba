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
	"archive/tar"
	"compress/gzip"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
)

const (
	globalStatusSQL = `show global status where Variable_name in ('Uptime','Com_commit','Com_rollback','Queries','Com_select','Com_insert','Com_update','Com_delete',
	       'Threads_running','Threads_connected','Threads_created','Innodb_buffer_pool_pages_data','Innodb_buffer_pool_pages_free',
	       'Innodb_buffer_pool_pages_dirty', 'Innodb_buffer_pool_pages_flushed','Innodb_rows_read','Innodb_rows_inserted','Innodb_rows_updated',
	       'Innodb_rows_deleted','Innodb_buffer_pool_read_requests','Innodb_buffer_pool_reads','Innodb_os_log_fsyncs','Innodb_os_log_written',
		   'Innodb_log_waits','Innodb_log_write_requests','Innodb_log_writes','Bytes_sent','Bytes_received')`

	innodbStatusSQL = "SHOW ENGINE INNODB STATUS"
	// online: all node is ok, error: some node is not online
	mgrMemberStateSQL = "SELECT IF(COUNT(*) > 0, 'ERROR','ONLINE') as MGR FROM performance_schema.replication_group_members WHERE MEMBER_STATE != 'ONLINE'"
	mgrFlowQueueSQL   = "select right(member_id,5) as member, COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE as app, COUNT_TRANSACTIONS_IN_QUEUE as cert from performance_schema.replication_group_member_stats"
	scroll            = 40
	version           = "3.0"
	globalVariableSQL = "show global variables"
	slaveStatusSQL    = "show slave status"
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "mysqldba",
	Short: "Welcome to the MySQL DBA Toolbox.",
	Long:  "Welcome to the MySQL DBA Toolbox. \nAuthor: HongBin <hongbin119@gmail.com> \nVersion: " + version,
}

var (
	dbUser, dbPassWd, dbHost string
	dbPort                   int
)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {

	RootCmd.PersistentFlags().StringVarP(&dbUser, "user", "u", "root", "mysql login user")
	RootCmd.PersistentFlags().StringVarP(&dbPassWd, "password", "p", "root", "mysql login password")
	RootCmd.PersistentFlags().StringVarP(&dbHost, "host", "H", "localhost", "mysql host ip")
	RootCmd.PersistentFlags().IntVarP(&dbPort, "port", "P", 3306, "mysql server port")

}

func byteHumen(i int64) aurora.Value {

	switch {

	case i >= 1024 && i < 1048576:
		return aurora.Green(strconv.FormatInt(i/1024, 10) + "KB")

	case i >= 1048576 && i < 1073741824:
		return aurora.Yellow(strconv.FormatInt(i/1048576, 10) + "MB")

	case i >= 1073741824:
		return aurora.Red(strconv.FormatInt(i/1073741824, 10) + "GB")

	default:
		return aurora.Green(strconv.FormatInt(i, 10) + "B")

	}

}

func numHumen(i int64) aurora.Value {

	switch {

	case i >= 1000 && i < 10000:
		return aurora.Green(strconv.FormatInt(i/1000, 10) + "K")
	case i >= 10000:
		return aurora.Red(strconv.FormatInt(i/1000, 10) + "K")
	default:
		return aurora.Green(strconv.FormatInt(i, 10))

	}

}

func ifErrWithPanic(err error) {
	if err != nil {
		panic(err)
	}
}

func ifErrWithLog(err error, msg ...string) {
	if err != nil {
		log.Fatal(err, msg)
	}
}

func mysqlConnect(dsn string) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	err = db.Ping()
	ifErrWithLog(err)
	return db
}

func mysqlSimpleQuery(q string, db *sql.DB) string {
	rows, err := db.Query(q)
	ifErrWithLog(err)
	defer rows.Close()
	var r string

	if rows.Next() {
		err := rows.Scan(&r)
		ifErrWithLog(err)
	}
	err = rows.Err()
	ifErrWithLog(err)
	return r
}

func tarIt(src string, des string) error {
	fi, err := os.Stat(src)
	if err != nil {
		return err
	}

	fw, err := os.Create(des)
	if err != nil {
		return err
	}
	defer fw.Close()

	gzw := gzip.NewWriter(fw)

	defer gzw.Close()

	tw := tar.NewWriter(gzw)
	defer tw.Close()

	// create a new dir/file header
	header, err := tar.FileInfoHeader(fi, fi.Name())
	if err != nil {
		return err
	}

	// write the header
	if err := tw.WriteHeader(header); err != nil {
		return err
	}

	// open files for taring
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	// copy file data into tar writer
	if _, err := io.Copy(tw, f); err != nil {
		return err
	}

	return nil
}
