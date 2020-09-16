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
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

// rbr2sbrCmd represents the rbr2sbr command
var rbr2sbrCmd = &cobra.Command{
	Use:   "rbr2sbr",
	Short: "Convert a rbr binary log to sbr format",
	Long: `Convert a rbr binary log to sbr format
For example:
$ mysqlbinlog -v --base64-output=DECODE-ROWS mysql-bin.000006 | mysqldba rbr2sbr | pt-query-digest --type=binlog --order-by Query_time:cnt --group-by fingerprint
	`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("rbr2sbr called")
		rbrToSbr()
	},
}

func init() {
	RootCmd.AddCommand(rbr2sbrCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// rbr2sbrCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// rbr2sbrCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	// rbr2sbrCmd.DisableFlagParsing = true
}

func rbrToSbr() {
	var v string
	var rbrStatement bool
	rbrStatement = false
	rbuf := bufio.NewReader(os.Stdin)
	for {
		line, err := rbuf.ReadString('\n')
		ifErrWithPanic(err)
		if strings.HasPrefix(line, "#") && strings.Contains(line, "end_log_pos") {
			if strings.Contains(line, "Query") {
				v = strings.Split(line, "Query")[1]
			}
			token := []string{"Update_rows:", "Write_rows:", "Delete_rows:", "Rows_query:", "Table_map:"}
			for _, rbrToken := range token {
				if strings.Contains(line, rbrToken) {
					line = strings.Split(line, rbrToken)[0] + "Query" + v
				}
			}
		}
		if strings.HasPrefix(line, "### ") {
			rbrStatement = true
			line = strings.TrimSpace(strings.Split(line, "# ")[1]) + "\n"
		} else if rbrStatement {
			fmt.Println("/*!*/;")
			rbrStatement = false
		}
		fmt.Print(line)
	}
}
