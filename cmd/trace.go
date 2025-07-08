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
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

// traceCmd represents the trace command
var traceCmd = &cobra.Command{
	Use:   "trace",
	Short: "Trace SQL execution and gather performance information",
	Long: `Trace SQL execution and collect explain plan, optimizer trace, table DDL and index information.
For example:
  mysqldba trace -u user -p pass -H hostname -P port -d database -f query.sql`,
	Run: func(cmd *cobra.Command, args []string) {
		runTrace()
	},
}

func runTrace() {
	if database == "" || sqlFile == "" {
		fmt.Println("Database name and SQL file are required")
		fmt.Println("Use 'mysqldba trace --help' for more information")
		return
	}

	// Read SQL from file
	sqlContent, err := os.ReadFile(sqlFile)
	if err != nil {
		fmt.Printf("Error reading SQL file: %v\n", err)
		return
	}

	sql := strings.TrimSpace(string(sqlContent))
	sql = strings.TrimSuffix(sql, ";")

	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s", dbUser, dbPassWd, dbHost, dbPort, database)
	db := mysqlConnect(dsn)
	defer db.Close()

	traceFilename := fmt.Sprintf("trace_%s.log", time.Now().Format("20060102_150405"))
	fmt.Printf("Starting SQL trace.\n")

	// Create trace file
	traceFile, err := os.Create(traceFilename)
	if err != nil {
		fmt.Printf("Error creating trace file: %v\n", err)
		return
	}
	defer traceFile.Close()

	// Execute explain
	fmt.Println("Collecting explain plan...")
	traceFile.WriteString("-- SQL Statement --\n")
	traceFile.WriteString(sql + ";\n\n")

	// Generate index recommendations if enabled
	if enableIndexRecommendations {
		fmt.Println("Generating index recommendations...")
		traceFile.WriteString("\n-- Index Recommendations --\n")

		// Add a note if dynamic selectivity is enabled
		if dynamicSelectivity {
			traceFile.WriteString("Note: Using dynamic selectivity calculation (may take longer).\n\n")
			fmt.Println("Note: Dynamic selectivity calculation enabled. This may take longer...")
		}

		recommendations, err := RecommendIndexes(db, sql, dynamicSelectivity)
		if err != nil {
			fmt.Printf("Error generating index recommendations: %v\n", err)
			traceFile.WriteString(fmt.Sprintf("Error generating index recommendations: %v\n", err))
		} else {
			// Write recommendations to trace file
			for table, recs := range recommendations {
				traceFile.WriteString(fmt.Sprintf("Table [%s] Index Suggestions:\n", table))
				for _, rec := range recs {
					traceFile.WriteString(fmt.Sprintf("  %s\n", rec.IndexSQL))
					traceFile.WriteString(fmt.Sprintf("  Selectivity Info: %s\n\n", rec.SelectivityInfo))
				}
			}
		}
	}

	traceFile.WriteString("-- Explain Plan --\n")
	explainRows, err := db.Query("EXPLAIN " + sql)
	if err != nil {
		fmt.Printf("Error executing EXPLAIN: %v\n", err)
		traceFile.WriteString(fmt.Sprintf("Error: %v\n", err))
	} else {
		writeFormattedTable(explainRows, traceFile)
	}

	// Execute explain analyze
	fmt.Println("Collecting EXPLAIN ANALYZE results...")
	traceFile.WriteString("\n-- Explain Analyze --\n")
	analyzeRows, err := db.Query("EXPLAIN ANALYZE " + sql)
	if err != nil {
		fmt.Printf("Error executing EXPLAIN ANALYZE: %v\n", err)
		traceFile.WriteString(fmt.Sprintf("Error: %v\n", err))
	} else {
		writeQueryResultToFile(analyzeRows, traceFile)
	}

	// Extract table names from SQL
	tables := extractTableNames(sql)

	// Get table DDL and indexes
	for _, table := range tables {
		fmt.Printf("Collecting table definition for %s...\n", table)
		traceFile.WriteString(fmt.Sprintf("\n-- Table Definition for %s --\n", table))

		// Get CREATE TABLE statement
		createTableRows, err := db.Query("SHOW CREATE TABLE " + table)
		if err != nil {
			fmt.Printf("Error getting CREATE TABLE for %s: %v\n", table, err)
			traceFile.WriteString(fmt.Sprintf("Error: %v\n", err))
		} else {
			writeQueryResultToFile(createTableRows, traceFile)
		}

		// Get indexes
		traceFile.WriteString(fmt.Sprintf("\n-- Indexes for %s --\n", table))
		indexRows, err := db.Query("SHOW INDEX FROM " + table)
		if err != nil {
			fmt.Printf("Error getting indexes for %s: %v\n", table, err)
			traceFile.WriteString(fmt.Sprintf("Error: %v\n", err))
		} else {
			writeFormattedTable(indexRows, traceFile)
		}

		unusedIndexes, err := db.Query(fmt.Sprintf("SELECT INDEX_NAME unused_index_name FROM SYS.SCHEMA_UNUSED_INDEXES WHERE OBJECT_NAME = '%s'", table))

		if err != nil {
			fmt.Printf("Error getting unused indexes for %s: %v\n", table, err)
			traceFile.WriteString(fmt.Sprintf("Error: %v\n", err))
		} else {
			writeQueryResultToFile(unusedIndexes, traceFile)
		}
	}

	// Collect optimizer trace
	fmt.Println("Collecting optimizer trace...")

	_, err = db.Exec("SET optimizer_trace_max_mem_size=100*1024*1024")
	if err != nil {
		fmt.Printf("Error setting optimizer_trace_max_mem_size: %v\n", err)
	}

	_, err = db.Exec("SET optimizer_trace='enabled=on'")
	if err != nil {
		fmt.Printf("Error enabling optimizer_trace: %v\n", err)
	}

	// Enable performance schema instrumentation
	_, err = db.Exec("CALL sys.ps_setup_save(10)")
	if err != nil {
		fmt.Printf("Error calling ps_setup_save: %v\n", err)
	}

	_, err = db.Exec("CALL sys.ps_setup_enable_instrument('stage%')")
	if err != nil {
		fmt.Printf("Error enabling stage instruments: %v\n", err)
	}

	_, err = db.Exec("CALL sys.ps_setup_enable_consumer('events_stages_%')")
	if err != nil {
		fmt.Printf("Error enabling events_stages consumer: %v\n", err)
	}

	_, err = db.Exec("CALL sys.ps_setup_enable_consumer('events_statement_%')")
	if err != nil {
		fmt.Printf("Error enabling events_statement consumer: %v\n", err)
	}

	// Execute the SQL to generate optimizer trace
	_, err = db.Exec(sql)
	if err != nil {
		fmt.Printf("Error executing SQL: %v\n", err)
		traceFile.WriteString(fmt.Sprintf("Error executing SQL: %v\n", err))
	}

	// Get optimizer trace
	traceFile.WriteString("\n-- Optimizer Trace --\n")
	traceRows, err := db.Query("SELECT trace FROM information_schema.optimizer_trace")
	if err != nil {
		fmt.Printf("Error getting optimizer trace: %v\n", err)
		traceFile.WriteString(fmt.Sprintf("Error: %v\n", err))
	} else {
		writeQueryResultToFile(traceRows, traceFile)
	}

	// Get stage execution information
	traceFile.WriteString("\n-- Execution Stages --\n")
	stageRows, err := db.Query(`
		SELECT 
			left(DIGEST,12) query_digest,
			sys.format_time(statement.timer_wait) as exec_time, 
			sys.format_time(stage.timer_wait) AS duration, 
			stage.event_name AS event_name
		FROM 
			performance_schema.events_stages_history_long as stage 
		INNER JOIN 
			performance_schema.events_statements_history_long AS statement USING(THREAD_ID)
		INNER JOIN 
			performance_schema.threads th USING(THREAD_ID) 
		WHERE 
			stage.event_id > statement.event_id 
			AND stage.end_event_id <= statement.end_event_id 
			AND statement.EVENT_NAME='statement/sql/select' 
			AND statement.LOCK_TIME > 0 
			AND th.PROCESSLIST_ID=connection_id() 
		ORDER BY 
			stage.event_id
	`)
	if err != nil {
		fmt.Printf("Error getting execution stages: %v\n", err)
		traceFile.WriteString(fmt.Sprintf("Error: %v\n", err))
	} else {
		writeQueryResultToFile(stageRows, traceFile)
	}

	// Get statement execution details
	traceFile.WriteString("\n-- Statement Execution Details --\n")
	detailRows, err := db.Query(`
		SELECT 
			left(DIGEST,12) DIGEST,
			sys.format_time(TIMER_WAIT) TIMER_WAIT,
			sys.format_time(LOCK_TIME) LOCK_TIME,
			ROWS_AFFECTED,
			ROWS_SENT,
			ROWS_EXAMINED,
			CREATED_TMP_DISK_TABLES,
			CREATED_TMP_TABLES,
			SELECT_SCAN,
			SELECT_FULL_JOIN,
			SORT_MERGE_PASSES 
		FROM 
			performance_schema.events_statements_history_long eh
		INNER JOIN 
			performance_schema.threads th USING(THREAD_ID) 
		WHERE 
			th.PROCESSLIST_ID=connection_id() 
			AND eh.EVENT_NAME ='statement/sql/select' 
			AND eh.LOCK_TIME > 0
	`)
	if err != nil {
		fmt.Printf("Error getting statement details: %v\n", err)
		traceFile.WriteString(fmt.Sprintf("Error: %v\n", err))
	} else {
		writeFormattedTable(detailRows, traceFile)
	}

	// Get current lock conflicts
	fmt.Println("Collecting lock conflicts information...")
	traceFile.WriteString("\n-- Current Lock Conflicts --\n")
	lockRows, err := db.Query(`
		SELECT 
			t2.thread_id AS waiting_thread,
			t2.processlist_info AS waiting_query,
			trx1.trx_rows_modified AS waiting_rows_modified,
			TIMESTAMPDIFF(SECOND, trx1.trx_started, NOW()) AS waiting_age,
			TIMESTAMPDIFF(SECOND, trx1.trx_wait_started, NOW()) AS waiting_wait_secs,
			t2.processlist_user AS waiting_user,
			t2.processlist_host AS waiting_host,
			t2.processlist_db AS waiting_db,
			t1.thread_id AS blocking_thread,
			t1.processlist_info AS blocking_query,
			trx2.trx_rows_modified AS blocking_rows_modified,
			TIMESTAMPDIFF(SECOND, trx2.trx_started, NOW()) AS blocking_age,
			TIMESTAMPDIFF(SECOND, trx2.trx_wait_started, NOW()) AS blocking_wait_secs,
			t1.processlist_user AS blocking_user,
			t1.processlist_host AS blocking_host,
			t1.processlist_db AS blocking_db,
			CONCAT(t1.processlist_command, IF(t1.processlist_command = 'Sleep', CONCAT(' ', t1.processlist_time), '')) AS blocking_status,
			CONCAT(blockings.lock_mode, ' ', blockings.object_schema, '.', blockings.object_name) AS lock_info
		FROM performance_schema.data_lock_waits w
		JOIN performance_schema.data_locks blockings ON blockings.ENGINE_LOCK_ID = w.BLOCKING_ENGINE_LOCK_ID
		JOIN performance_schema.data_locks waitings ON waitings.ENGINE_LOCK_ID = w.REQUESTING_ENGINE_LOCK_ID
		JOIN information_schema.innodb_trx trx1 ON trx1.trx_id = waitings.engine_transaction_id
		JOIN information_schema.innodb_trx trx2 ON trx2.trx_id = blockings.engine_transaction_id
		JOIN performance_schema.threads t1 ON t1.thread_id = blockings.thread_id
		JOIN performance_schema.threads t2 ON t2.thread_id = waitings.thread_id
	`)
	if err != nil {
		fmt.Printf("Error getting lock conflicts: %v\n", err)
		traceFile.WriteString(fmt.Sprintf("Error: %v\n", err))
	} else {
		writeFormattedTable(lockRows, traceFile)
	}

	// Restore performance schema settings
	_, err = db.Exec("CALL sys.ps_setup_reload_saved()")
	if err != nil {
		fmt.Printf("Error restoring performance schema settings: %v\n", err)
	}

	fmt.Printf("SQL trace completed. Results saved to %s\n", traceFilename)
}

// Extract table names from SQL query
func extractTableNames(sql string) []string {
	// Remove comments
	re := regexp.MustCompile(`/\*.*?\*/`)
	sql = re.ReplaceAllString(sql, "")

	// Remove newlines and extra spaces
	sql = strings.Join(strings.Fields(sql), " ")

	// Find tables after FROM and JOIN keywords
	re = regexp.MustCompile(`(?i)(FROM|JOIN)\s+([a-zA-Z0-9_\.]+)`)
	matches := re.FindAllStringSubmatch(sql, -1)

	tableMap := make(map[string]bool)
	for _, match := range matches {
		table := match[2]
		// Handle table.column format
		parts := strings.Split(table, ".")
		if len(parts) > 1 {
			table = parts[0]
		}
		tableMap[table] = true
	}

	var tables []string
	for table := range tableMap {
		tables = append(tables, table)
	}

	return tables
}

// Write query results to file
func writeQueryResultToFile(rows *sql.Rows, file *os.File) {
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		file.WriteString(fmt.Sprintf("Error getting columns: %v\n", err))
		return
	}

	// Prepare result containers
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for i := 0; i < count; i++ {
		valuePtrs[i] = &values[i]
	}

	// Write column headers
	file.WriteString(strings.Join(columns, "\t") + "\n")
	file.WriteString(strings.Repeat("-", 80) + "\n")

	// Write rows
	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			file.WriteString(fmt.Sprintf("Error scanning row: %v\n", err))
			continue
		}

		// Convert values to strings
		var rowStrings []string
		for _, val := range values {
			var v string
			if val == nil {
				v = "NULL"
			} else {
				switch typedVal := val.(type) {
				case []byte:
					v = string(typedVal)
				default:
					v = fmt.Sprintf("%v", typedVal)
				}
			}
			rowStrings = append(rowStrings, v)
		}

		// Write row
		file.WriteString(strings.Join(rowStrings, "\t") + "\n")
	}
}

// Write formatted table with aligned columns for better readability
func writeFormattedTable(rows *sql.Rows, file *os.File) {
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		file.WriteString(fmt.Sprintf("Error getting columns: %v\n", err))
		return
	}

	// Prepare result containers
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for i := 0; i < count; i++ {
		valuePtrs[i] = &values[i]
	}

	// First pass: collect all rows and determine column widths
	var allRows [][]string
	colWidths := make([]int, count)

	// Initialize column widths with header lengths
	for i, col := range columns {
		colWidths[i] = len(col)
	}

	// Process all rows to get values and update column widths
	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			file.WriteString(fmt.Sprintf("Error scanning row: %v\n", err))
			continue
		}

		// Convert values to strings
		rowStrings := make([]string, count)
		for i, val := range values {
			var v string
			if val == nil {
				v = "NULL"
			} else {
				switch typedVal := val.(type) {
				case []byte:
					v = string(typedVal)
				default:
					v = fmt.Sprintf("%v", typedVal)
				}
			}
			rowStrings[i] = v

			// Update column width if necessary
			if len(v) > colWidths[i] {
				colWidths[i] = len(v)
			}
		}
		allRows = append(allRows, rowStrings)
	}

	// Write column headers with proper formatting
	for i, col := range columns {
		if i > 0 {
			file.WriteString(" | ")
		}
		format := fmt.Sprintf("%%-%ds", colWidths[i])
		file.WriteString(fmt.Sprintf(format, col))
	}
	file.WriteString("\n")

	// Write separator line
	separatorLine := ""
	for i, width := range colWidths {
		if i > 0 {
			separatorLine += "-+-"
		}
		separatorLine += strings.Repeat("-", width)
	}
	file.WriteString(separatorLine + "\n")

	// Write rows with proper formatting
	for _, rowStrings := range allRows {
		for i, val := range rowStrings {
			if i > 0 {
				file.WriteString(" | ")
			}
			format := fmt.Sprintf("%%-%ds", colWidths[i])
			file.WriteString(fmt.Sprintf(format, val))
		}
		file.WriteString("\n")
	}
}

var (
	database                   string
	sqlFile                    string
	enableIndexRecommendations bool
	dynamicSelectivity         bool
)

func init() {
	RootCmd.AddCommand(traceCmd)

	traceCmd.Flags().StringVarP(&database, "database", "d", "", "Database name (required)")
	traceCmd.Flags().StringVarP(&sqlFile, "file", "f", "", "SQL file containing the query to trace (required)")
	traceCmd.Flags().BoolVarP(&enableIndexRecommendations, "recommend-indexes", "r", false, "Enable index recommendations")
	traceCmd.Flags().BoolVarP(&dynamicSelectivity, "dynamic-selectivity", "s", false, "Use dynamic selectivity calculation for index recommendations")

	traceCmd.MarkFlagRequired("database")
	traceCmd.MarkFlagRequired("file")
}
