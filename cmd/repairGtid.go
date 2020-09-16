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
	"fmt"
	"github.com/spf13/cobra"
	"strings"
)

var (
	source, dest string
)

// repairGtidCmd represents the repairGtid command
var repairGtidCmd = &cobra.Command{
	Use:   "repairGtid",
	Short: "Compare master(source) and slave(destination) GTID to find inconsistent",
	Long: `Compare master and slave GTID to find inconsistent and generate fix SQL. 
For example:
Is slave's GTID more than master? Compare master(source) instance and slave(destination) instance.  
$ mysqldba repairGtid -s 'msandbox:msandbox@(127.0.0.1:22695)/' -d 'msandbox:msandbox@(127.0.0.1:22696)/'`,
	Run: func(cmd *cobra.Command, args []string) {
		// Connect master
		db1 := mysqlConnect(source)
		gtidExecuted := "SELECT @@GLOBAL.GTID_EXECUTED"

		masterGtid := mysqlSimpleQuery(gtidExecuted, db1)

		// Connect slave
		db2 := mysqlConnect(dest)

		slaveGtid := mysqlSimpleQuery(gtidExecuted, db2)
		gtidLoss := fmt.Sprintf("SELECT GTID_SUBTRACT('%s', '%s')", slaveGtid, masterGtid)
		g := mysqlSimpleQuery(gtidLoss, db2)

		if g != "" {
			fmt.Printf("Master loss these gtid:\n%s \n\n", g)
			fmt.Printf("Master can injecting empty transactions to repair the consistency of GTID (Warning: Please check the contents of GTID event before execution).\n")
			genEmptyTrx(g)
		} else {
			fmt.Printf("It's OK! There is no inconsistency GTID\n")
		}

	},
}

func init() {
	RootCmd.AddCommand(repairGtidCmd)

	repairGtidCmd.PersistentFlags().StringVarP(&source, "source", "s", "root:root@(localhost:3306)/", "connect mysql master server ")
	repairGtidCmd.PersistentFlags().StringVarP(&dest, "dest", "d", "root:root@(localhost:3306)/", "connect mysql slave server")

}

func genEmptyTrx(s string) {
	gtidAll := strings.Split(s, ",")
	for _, v := range gtidAll {
		uuid := strings.Split(v, ":")[0]

		if strings.Contains(strings.Split(v, ":")[1], "-") {
			gtidNum := strings.Split(strings.Split(v, ":")[1], "-")
			for i := strToInt(gtidNum[0]); i <= strToInt(gtidNum[1]); i++ {

				fmt.Printf("SET GTID_NEXT='%v:%v';BEGIN;COMMIT;\n", strings.TrimLeft(uuid, "\n"), i)
			}
		} else {
			fmt.Printf("SET GTID_NEXT='%s';BEGIN;COMMIT;\n", strings.TrimLeft(v, "\n"))
		}
	}
	fmt.Printf("SET GTID_NEXT=AUTOMATIC;")
}
