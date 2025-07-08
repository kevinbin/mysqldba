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
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
)

// BinlogStats 统计结构体
type BinlogStats struct {
	TotalCount  int
	InsertCount int
	UpdateCount int
	DeleteCount int
	QueryType   string
	QueryCount  int
	TableName   string
	Timestamp   string
}

// binlogCmd represents the binlog command
var binlogCmd = &cobra.Command{
	Use:   "binlog",
	Short: "Analyze MySQL binlog file and count DML operations",
	Long: `Analyze MySQL binlog file and count INSERT, UPDATE, DELETE operations in each transaction.

Examples:
  mysqldba binlog /path/to/mysql-bin.000001
  mysqldba binlog -s /path/to/mysql-bin.000001`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		binlogFile := args[0]
		if showSummary {
			analyzeBinlogSummary(binlogFile)
		} else {
			analyzeBinlogDetailed(binlogFile)
		}
	},
}

var (
	showSummary bool
)

func init() {
	RootCmd.AddCommand(binlogCmd)
	binlogCmd.Flags().BoolVarP(&showSummary, "summary", "s", false, "Show summary only")
}

// analyzeBinlogDetailed analyze binlog in detailed mode (default)
func analyzeBinlogDetailed(binlogFile string) {
	// Check if file exists
	if _, err := os.Stat(binlogFile); os.IsNotExist(err) {
		fmt.Printf("Error: File %s does not exist\n", binlogFile)
		return
	}

	// Execute mysqlbinlog command
	cmd := exec.Command("mysqlbinlog", "-vv", "--base64-output=DECODE-ROWS", binlogFile)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Printf("Error: Unable to create pipe: %v\n", err)
		return
	}

	if err := cmd.Start(); err != nil {
		fmt.Printf("Error: Unable to start mysqlbinlog command: %v\n", err)
		return
	}

	// Compile regular expressions
	tableMapRegex := regexp.MustCompile(`.*Table_map:.*mapped to number`)
	tableNameRegex := regexp.MustCompile(`Table_map: \x60([^\x60]+)\x60\.\x60([^\x60]+)\x60 mapped to number`)
	insertRegex := regexp.MustCompile(`### INSERT INTO .*`)
	updateRegex := regexp.MustCompile(`### UPDATE .*`)
	deleteRegex := regexp.MustCompile(`### DELETE FROM .*`)
	atRegex := regexp.MustCompile(`^# at `)
	commitRegex := regexp.MustCompile(`^COMMIT`)

	// State variables
	stats := &BinlogStats{}
	flag := false

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()

		if tableMapRegex.MatchString(line) {
			// Parse table mapping line
			parts := strings.Fields(line)
			timestamp := ""
			if len(parts) >= 2 {
				// Remove # symbol from timestamp
				timestamp = strings.TrimPrefix(parts[0], "#") + " " + parts[1]
			}

			// Extract table name using regex
			tableName := ""
			matches := tableNameRegex.FindStringSubmatch(line)
			if len(matches) >= 3 {
				tableName = matches[1] + "." + matches[2] // database.table
			}

			fmt.Printf("Timestamp: %s Table: %s",
				aurora.Cyan(timestamp),
				aurora.Yellow(tableName))
			flag = true
		} else if insertRegex.MatchString(line) {
			stats.TotalCount++
			stats.InsertCount++
			stats.QueryType = "INSERT"
			stats.QueryCount++
		} else if updateRegex.MatchString(line) {
			stats.TotalCount++
			stats.UpdateCount++
			stats.QueryType = "UPDATE"
			stats.QueryCount++
		} else if deleteRegex.MatchString(line) {
			stats.TotalCount++
			stats.DeleteCount++
			stats.QueryType = "DELETE"
			stats.QueryCount++
		} else if atRegex.MatchString(line) && flag && stats.QueryCount > 0 {
			// Output query type and affected rows
			fmt.Printf(" Query Type: %s %s rows\n",
				aurora.Green(stats.QueryType),
				aurora.Magenta(strconv.Itoa(stats.QueryCount)))
			stats.QueryType = ""
			stats.QueryCount = 0
		} else if commitRegex.MatchString(line) {
			// Output transaction summary
			fmt.Printf("[Transaction Total: %s Insert(s): %s Update(s): %s Delete(s): %s]\n",
				aurora.Bold(aurora.Blue(strconv.Itoa(stats.TotalCount))),
				aurora.Green(strconv.Itoa(stats.InsertCount)),
				aurora.Yellow(strconv.Itoa(stats.UpdateCount)),
				aurora.Red(strconv.Itoa(stats.DeleteCount)))
			fmt.Println(strings.Repeat("=", 88))

			// Reset statistics
			stats.TotalCount = 0
			stats.InsertCount = 0
			stats.UpdateCount = 0
			stats.DeleteCount = 0
			stats.QueryType = ""
			stats.QueryCount = 0
			flag = false
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error: Error reading output: %v\n", err)
	}

	if err := cmd.Wait(); err != nil {
		fmt.Printf("Error: mysqlbinlog command failed: %v\n", err)
	}
}

// analyzeBinlogSummary analyze binlog and show summary only
func analyzeBinlogSummary(binlogFile string) {
	// Check if file exists
	if _, err := os.Stat(binlogFile); os.IsNotExist(err) {
		fmt.Printf("Error: File %s does not exist\n", binlogFile)
		return
	}

	// Execute mysqlbinlog command
	cmd := exec.Command("mysqlbinlog", "-vv", "--base64-output=DECODE-ROWS", binlogFile)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Printf("Error: Unable to create pipe: %v\n", err)
		return
	}

	if err := cmd.Start(); err != nil {
		fmt.Printf("Error: Unable to start mysqlbinlog command: %v\n", err)
		return
	}

	// Compile regular expressions
	insertRegex := regexp.MustCompile(`### INSERT INTO .*`)
	updateRegex := regexp.MustCompile(`### UPDATE .*`)
	deleteRegex := regexp.MustCompile(`### DELETE FROM .*`)
	commitRegex := regexp.MustCompile(`^COMMIT`)

	// Global statistics
	var totalInserts, totalUpdates, totalDeletes, totalTransactions int

	// Current transaction statistics
	var currentInserts, currentUpdates, currentDeletes int

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()

		if insertRegex.MatchString(line) {
			currentInserts++
		} else if updateRegex.MatchString(line) {
			currentUpdates++
		} else if deleteRegex.MatchString(line) {
			currentDeletes++
		} else if commitRegex.MatchString(line) {
			// Transaction ends, add to total
			totalInserts += currentInserts
			totalUpdates += currentUpdates
			totalDeletes += currentDeletes
			totalTransactions++

			// Reset current transaction statistics
			currentInserts = 0
			currentUpdates = 0
			currentDeletes = 0
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error: Error reading output: %v\n", err)
		return
	}

	if err := cmd.Wait(); err != nil {
		fmt.Printf("Error: mysqlbinlog command failed: %v\n", err)
		return
	}

	// Output summary results
	fmt.Printf("\n")
	fmt.Printf("==================== BINLOG ANALYSIS SUMMARY ====================\n")
	fmt.Printf("File: %s\n", aurora.Cyan(binlogFile))
	fmt.Printf("Transactions: %s\n", aurora.Bold(aurora.Blue(strconv.Itoa(totalTransactions))))
	fmt.Printf("INSERT Rows: %s\n", aurora.Green(strconv.Itoa(totalInserts)))
	fmt.Printf("UPDATE Rows: %s\n", aurora.Yellow(strconv.Itoa(totalUpdates)))
	fmt.Printf("DELETE Rows: %s\n", aurora.Red(strconv.Itoa(totalDeletes)))
	fmt.Printf("Total DML Rows: %s\n", aurora.Bold(aurora.Magenta(strconv.Itoa(totalInserts+totalUpdates+totalDeletes))))
	fmt.Printf("===============================================================\n")
}
