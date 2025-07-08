// Copyright © 2023 Hong Bin <hongbin119@gmail.com>
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
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

const defaultSelectivity = 0.5
const maxIndexLen = 4


// Column represents a column with its selectivity and condition information
type Column struct {
	Name        string
	Selectivity float64
	CondType    string
	Source      string
}

// TableColumnInfo holds column information for a specific table
type TableColumnInfo struct {
	TableName      string
	WhereColumns   []string
	JoinColumns    []string
	GroupByColumns []string
	OrderByColumns []string
	SelectColumns  []string
}

// SQLParseResult contains the complete parsed SQL information
type SQLParseResult struct {
	Tables       map[string]string           // alias -> real table name
	TableColumns map[string]*TableColumnInfo // table name -> column info
}

// IndexRecommendation holds the recommended index information
type IndexRecommendation struct {
	IndexSQL        string
	SelectivityInfo string
}

// getSelectivity calculates the selectivity of a column in a table, preferring MySQL statistics, fallback to full scan
func getSelectivity(db *sql.DB, table, column string) (float64, error) {
	schema, pureTable := parseSchemaTable(table)
	// 1. Prefer MySQL statistics
	selectivity, err := getSelectivityByStats(db, schema, pureTable, column)
	if err == nil && selectivity > 0 && selectivity <= 1 {
		return selectivity, nil
	}
	// 2. Fallback to full table scan
	return getSelectivityByScan(db, pureTable, column)
}

// getSelectivityByStats estimates selectivity using information_schema statistics
func getSelectivityByStats(db *sql.DB, schema, table, column string) (float64, error) {
	var cardinality, tableRows int64
	// Get index cardinality
	query := `
		SELECT CARDINALITY
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
		ORDER BY SEQ_IN_INDEX LIMIT 1`
	err := db.QueryRow(query, schema, table, column).Scan(&cardinality)
	if err != nil || cardinality == 0 {
		return 0, fmt.Errorf("get cardinality error: %v", err)
	}
	// Get table row count
	query2 := `
		SELECT TABLE_ROWS
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`
	err = db.QueryRow(query2, schema, table).Scan(&tableRows)
	if err != nil || tableRows == 0 {
		return 0, fmt.Errorf("get table rows error: %v", err)
	}
	return float64(cardinality) / float64(tableRows), nil
}

// getSelectivityByScan fallback: full table scan to calculate selectivity
func getSelectivityByScan(db *sql.DB, table, column string) (float64, error) {
	// 排除NULL值和空值的影响
	query := fmt.Sprintf("SELECT COUNT(DISTINCT NULLIF(NULLIF(`%s`, ''), NULL)) AS ndv, COUNT(`%s`) AS total FROM `%s`", column, column, table)
	var ndv, total int64
	err := db.QueryRow(query).Scan(&ndv, &total)
	if err != nil || total == 0 {
		return defaultSelectivity, fmt.Errorf("scan error: %v", err)
	}
	return float64(ndv) / float64(total), nil
}

// parseSchemaTable splits schema and table name if present (e.g., db.table)
func parseSchemaTable(table string) (schema, pureTable string) {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	// If no schema, use current database (empty string)
	return "", table
}

// ParseSQL parses SQL statement and extracts table and column information
func ParseSQL(query string) (*SQLParseResult, error) {
	p := parser.New()
	node, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %v", err)
	}

	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("unsupported statement type %T (only SELECT is supported)", node)
	}

	result := &SQLParseResult{
		Tables:       make(map[string]string),
		TableColumns: make(map[string]*TableColumnInfo),
	}

	// 提取表信息
	if sel.From != nil && sel.From.TableRefs != nil {
		extractTablesFromASTTiDB(sel.From.TableRefs, result.Tables)
	}

	// 初始化 TableColumnInfo
	for alias, tableName := range result.Tables {
		if _, exists := result.TableColumns[tableName]; !exists {
			result.TableColumns[tableName] = &TableColumnInfo{
				TableName:      tableName,
				WhereColumns:   []string{},
				JoinColumns:    []string{},
				GroupByColumns: []string{},
				OrderByColumns: []string{},
				SelectColumns:  []string{},
			}
		}
		if alias != tableName {
			result.TableColumns[alias] = result.TableColumns[tableName]
		}
	}

	// WHERE
	if sel.Where != nil {
		whereColumns := extractColumnsFromExprTiDB(sel.Where)
		distributeColumnsToTables(whereColumns, result.Tables, result.TableColumns, "WHERE")
	}

	// JOIN
	var joinColumns []string
	if sel.From != nil && sel.From.TableRefs != nil {
		joinColumns = extractJoinColumnsFromASTTiDB(sel.From.TableRefs)
	}
	distributeColumnsToTables(joinColumns, result.Tables, result.TableColumns, "JOIN")

	// ORDER BY
	var orderColumns []string
	if sel.OrderBy != nil {
		for _, ob := range sel.OrderBy.Items {
			cols := extractColumnsFromExprTiDB(ob.Expr)
			orderColumns = append(orderColumns, cols...)
		}
	}
	distributeColumnsToTables(orderColumns, result.Tables, result.TableColumns, "ORDER BY")

	// GROUP BY
	var groupColumns []string
	if sel.GroupBy != nil {
		for _, gb := range sel.GroupBy.Items {
			cols := extractColumnsFromExprTiDB(gb.Expr)
			groupColumns = append(groupColumns, cols...)
		}
	}
	distributeColumnsToTables(groupColumns, result.Tables, result.TableColumns, "GROUP BY")

	// SELECT 字段
	if sel.Fields != nil {
		for _, field := range sel.Fields.Fields {
			cols := extractColumnsFromExprTiDB(field.Expr)
			distributeColumnsToTables(cols, result.Tables, result.TableColumns, "SELECT")
		}
	}

	return result, nil
}

// TiDB AST 相关辅助函数

// 提取表信息
func extractTablesFromASTTiDB(node ast.ResultSetNode, tables map[string]string) {
	switch n := node.(type) {
	case *ast.TableSource:
		if tblName, ok := n.Source.(*ast.TableName); ok {
			table := tblName.Name.O
			alias := n.AsName.O
			if alias == "" {
				alias = table
			}
			tables[alias] = table
		}
	case *ast.Join:
		extractTablesFromASTTiDB(n.Left, tables)
		extractTablesFromASTTiDB(n.Right, tables)
	case *ast.SelectStmt:
		if n.From != nil && n.From.TableRefs != nil {
			extractTablesFromASTTiDB(n.From.TableRefs, tables)
		}
	}
}

// 提取 JOIN 列
func extractJoinColumnsFromASTTiDB(node ast.ResultSetNode) []string {
	var columns []string
	switch n := node.(type) {
	case *ast.Join:
		if n.On != nil && n.On.Expr != nil {
			cols := extractColumnsFromExprTiDB(n.On.Expr)
			columns = append(columns, cols...)
		}
		columns = append(columns, extractJoinColumnsFromASTTiDB(n.Left)...)
		columns = append(columns, extractJoinColumnsFromASTTiDB(n.Right)...)
	case *ast.TableSource:
		// do nothing
	}
	return columns
}

// 提取表达式中的列名
// 用 Visitor 方式遍历 AST

type columnExtractor struct {
	columns []string
}

func (v *columnExtractor) Enter(in ast.Node) (ast.Node, bool) {
	if col, ok := in.(*ast.ColumnNameExpr); ok {
		if col.Name.Table.O != "" {
			v.columns = append(v.columns, col.Name.Table.O+"."+col.Name.Name.O)
		} else {
			v.columns = append(v.columns, col.Name.Name.O)
		}
	}
	return in, false
}

func (v *columnExtractor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func extractColumnsFromExprTiDB(expr ast.Node) []string {
	v := &columnExtractor{}
	if expr != nil {
		expr.Accept(v)
	}
	return v.columns
}

// distributeColumnsToTables assigns columns to their respective tables based on clause type
func distributeColumnsToTables(columns []string, tables map[string]string, tableColumns map[string]*TableColumnInfo, clauseType string) {
	for _, col := range columns {
		tableName, columnName := parseTableColumn(col, tables)
		if tableName == "" || columnName == "" {
			continue
		}

		// Find the actual table info (handle both direct table names and aliases)
		var tableInfo *TableColumnInfo
		if info, exists := tableColumns[tableName]; exists {
			tableInfo = info
		} else {
			// Try to find by alias
			for alias, realTable := range tables {
				if alias == tableName || realTable == tableName {
					if info, exists := tableColumns[realTable]; exists {
						tableInfo = info
						break
					}
				}
			}
		}

		if tableInfo == nil {
			continue
		}

		// Add column to appropriate slice based on clause type
		switch clauseType {
		case "WHERE":
			if !contains(tableInfo.WhereColumns, columnName) {
				tableInfo.WhereColumns = append(tableInfo.WhereColumns, columnName)
			}
		case "JOIN":
			if !contains(tableInfo.JoinColumns, columnName) {
				tableInfo.JoinColumns = append(tableInfo.JoinColumns, columnName)
			}
		case "GROUP BY":
			if !contains(tableInfo.GroupByColumns, columnName) {
				tableInfo.GroupByColumns = append(tableInfo.GroupByColumns, columnName)
			}
		case "ORDER BY":
			if !contains(tableInfo.OrderByColumns, columnName) {
				tableInfo.OrderByColumns = append(tableInfo.OrderByColumns, columnName)
			}
		case "SELECT":
			if !contains(tableInfo.SelectColumns, columnName) {
				tableInfo.SelectColumns = append(tableInfo.SelectColumns, columnName)
			}
		}
	}
}

// contains checks if a string slice contains a specific string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// 获取表的主键和唯一索引字段
func getTableUniqueColumns(db *sql.DB, table string) (pkCols, uniqueCols map[string]bool, allIndexes [][]string, err error) {
	pkCols = make(map[string]bool)
	uniqueCols = make(map[string]bool)
	allIndexes = [][]string{}
	rows, err := db.Query("SHOW INDEX FROM " + table)
	if err != nil {
		return
	}
	defer rows.Close()

	// 字段顺序: Table, Non_unique, Key_name, Seq_in_index, Column_name, ...
	var (
		tableName, keyName, columnName string
		nonUnique, seqInIndex          int
		dummy                          interface{}
	)
	indexMap := make(map[string][]string)
	pkName := "PRIMARY"
	for rows.Next() {
		// 只取前6列
		err = rows.Scan(&tableName, &nonUnique, &keyName, &seqInIndex, &columnName, &dummy, &dummy, &dummy, &dummy, &dummy, &dummy, &dummy, &dummy, &dummy, &dummy)
		if err != nil {
			// 兼容不同MySQL版本字段数
			err = rows.Scan(&tableName, &nonUnique, &keyName, &seqInIndex, &columnName)
			if err != nil {
				return
			}
		}
		indexMap[keyName] = append(indexMap[keyName], columnName)
		if keyName == pkName {
			pkCols[columnName] = true
		}
		if nonUnique == 0 && keyName != pkName {
			uniqueCols[columnName] = true
		}
	}
	for _, cols := range indexMap {
		allIndexes = append(allIndexes, cols)
	}
	return
}

// 判断推荐索引是否已被现有索引包含
func isIndexCovered(recommendCols []string, allIndexes [][]string) bool {
	for _, idx := range allIndexes {
		if len(idx) < len(recommendCols) {
			continue
		}
		match := true
		for i, col := range recommendCols {
			if idx[i] != col {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func RecommendIndexes(db *sql.DB, sql string, dynamicSelectivity bool) (map[string][]IndexRecommendation, error) {
	parseResult, err := ParseSQL(sql)
	if err != nil {
		return nil, err
	}

	recommendations := make(map[string][]IndexRecommendation)

	for tableName, tableInfo := range parseResult.TableColumns {
		if tableName != tableInfo.TableName {
			continue
		}

		// 获取主键、唯一索引、所有索引
		pkCols, uniqueCols, allIndexes, _ := getTableUniqueColumns(db, tableName)

		// 合并所有相关字段，按优先级排序并去重
		priorityCols := []string{}
		colSet := make(map[string]bool)
		addCol := func(col string) {
			if col != "" && !colSet[col] {
				priorityCols = append(priorityCols, col)
				colSet[col] = true
			}
		}
		// 1. WHERE/JOIN/主查询过滤
		for _, col := range tableInfo.WhereColumns {
			addCol(col)
		}
		for _, col := range tableInfo.JoinColumns {
			addCol(col)
		}
		// 2. GROUP BY
		for _, col := range tableInfo.GroupByColumns {
			addCol(col)
		}
		// 3. ORDER BY
		for _, col := range tableInfo.OrderByColumns {
			addCol(col)
		}
		// 4. SELECT/子查询
		for _, col := range tableInfo.SelectColumns {
			addCol(col)
		}

		// 限制最大索引长度
		if len(priorityCols) > maxIndexLen {
			priorityCols = priorityCols[:maxIndexLen]
		}

		// 推荐主索引（过滤掉主键/唯一键的单列索引）
		if len(priorityCols) > 0 {
			// 单列索引且为主键/唯一键则跳过
			if len(priorityCols) == 1 && (pkCols[priorityCols[0]] || uniqueCols[priorityCols[0]]) {
				// skip
			} else if !isIndexCovered(priorityCols, allIndexes) {
				// 生成selectivity信息
				var selectivityDetails []string
				for _, col := range priorityCols {
					selectivity := defaultSelectivity
					cond := ""
					if contains(tableInfo.WhereColumns, col) {
						cond = "WHERE"
					} else if contains(tableInfo.JoinColumns, col) {
						cond = "JOIN"
					} else if contains(tableInfo.GroupByColumns, col) {
						cond = "GROUP BY"
					} else if contains(tableInfo.OrderByColumns, col) {
						cond = "ORDER BY"
					} else if contains(tableInfo.SelectColumns, col) {
						cond = "SELECT"
					}
					if dynamicSelectivity {
						var err error
						selectivity, err = getSelectivity(db, tableName, col)
						if err != nil {
							selectivity = defaultSelectivity
						}
					}
					selectivityDetails = append(selectivityDetails, fmt.Sprintf("%s:[selectivity=%.2f, condition=%s]", col, selectivity, cond))
				}
				indexName := fmt.Sprintf("idx_%s_%s", tableName, strings.Join(priorityCols, "_"))
				indexSQL := fmt.Sprintf("ALTER TABLE %s ADD INDEX %s (%s);", tableName, indexName, strings.Join(priorityCols, ", "))
				recommendations[tableName] = append(recommendations[tableName], IndexRecommendation{
					IndexSQL:        indexSQL,
					SelectivityInfo: strings.Join(selectivityDetails, ", "),
				})
			}
		}

		// 推荐覆盖索引（主索引列+SELECT列，去重，限制长度）
		coverCols := append([]string{}, priorityCols...)
		coverSet := make(map[string]bool)
		for _, col := range coverCols {
			coverSet[col] = true
		}
		for _, col := range tableInfo.SelectColumns {
			if !coverSet[col] {
				coverCols = append(coverCols, col)
				coverSet[col] = true
			}
		}
		if len(coverCols) > maxIndexLen {
			coverCols = coverCols[:maxIndexLen]
		}
		if len(coverCols) > len(priorityCols) && !isIndexCovered(coverCols, allIndexes) {
			var coverDetails []string
			for _, col := range coverCols {
				cond := ""
				if contains(tableInfo.WhereColumns, col) {
					cond = "WHERE"
				} else if contains(tableInfo.JoinColumns, col) {
					cond = "JOIN"
				} else if contains(tableInfo.GroupByColumns, col) {
					cond = "GROUP BY"
				} else if contains(tableInfo.OrderByColumns, col) {
					cond = "ORDER BY"
				} else if contains(tableInfo.SelectColumns, col) {
					cond = "SELECT"
				}
				coverDetails = append(coverDetails, fmt.Sprintf("%s:[condition=%s]", col, cond))
			}
			indexName := fmt.Sprintf("idx_%s_cover_%s", tableName, strings.Join(coverCols, "_"))
			indexSQL := fmt.Sprintf("ALTER TABLE %s ADD INDEX %s (%s);", tableName, indexName, strings.Join(coverCols, ", "))
			recommendations[tableName] = append(recommendations[tableName], IndexRecommendation{
				IndexSQL:        indexSQL,
				SelectivityInfo: strings.Join(coverDetails, ", "),
			})
		}
	}
	return recommendations, nil
}

// parseTableColumn splits a column reference into table and column parts
func parseTableColumn(fullCol string, tables map[string]string) (string, string) {
	parts := strings.Split(fullCol, ".")

	if len(parts) == 1 {
		// No table specified, can't determine which table it belongs to
		return "", parts[0]
	} else if len(parts) == 2 {
		tableName := tables[parts[0]]
		if tableName == "" {
			tableName = parts[0]
		}
		return tableName, parts[1]
	}

	return "", ""
}
