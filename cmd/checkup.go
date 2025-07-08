package cmd

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	// "time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/spf13/cobra"
)

// CheckItem represents a single checkup item.
type CheckItem struct {
	Title  string
	Anchor string
	SQL    string
}

// 巡检建议结构体
type Advice struct {
	Level      string // 高/中/低
	Content    string
	RelatedSQL string
}

// 账号权限结构体
type AccountPrivilege struct {
	User           string
	Host           string
	Grants         []string
	HighRisk       bool
	IsWildcardHost bool
	LastUsed       string
}

// ================== 分模块SQL定义 ==================

// 常用SQL常量
const sqlWildcardHostAccounts = "SELECT user AS USER, host AS HOST FROM mysql.user WHERE host='%';"
const sqlWildcardHostAccountsCount = "SELECT COUNT(*) FROM mysql.user WHERE host = '%';"
const sqlNoPasswordAccount = "SELECT user AS USER, host AS HOST, authentication_string AS AUTHENTICATION_STRING FROM mysql.user WHERE (authentication_string = '' OR authentication_string IS NULL);"
const sqlNoPasswordAccountCount = "SELECT COUNT(*) FROM mysql.user WHERE (authentication_string = '' OR authentication_string IS NULL);"
const sqlAnonymousAccount = "SELECT user AS USER, host AS HOST FROM mysql.user WHERE user = '';"
const sqlAnonymousAccountCount = "SELECT COUNT(*) FROM mysql.user WHERE user = '';"
const sqlHighRiskAccounts = "SELECT user AS USER, host AS HOST, Super_priv AS SUPER_PRIV, Grant_priv AS GRANT_PRIV, File_priv AS FILE_PRIV, Process_priv AS PROCESS_PRIV, Shutdown_priv AS SHUTDOWN_PRIV, Reload_priv AS RELOAD_PRIV FROM mysql.user WHERE Super_priv='Y' OR Grant_priv='Y' OR File_priv='Y' OR Process_priv='Y' OR Shutdown_priv='Y' OR Reload_priv='Y';"
const sqlHighRiskAccountsCount = "SELECT COUNT(*) FROM mysql.user WHERE Super_priv='Y' OR Grant_priv='Y' OR File_priv='Y' OR Process_priv='Y' OR Shutdown_priv='Y' OR Reload_priv='Y';"

// 指标与参数
var perfRatiosItem = CheckItem{
	Title:  "关键指标",
	Anchor: "perf_ratios",
	SQL:    "",
}
var paramItemsWithPerf = append([]CheckItem{perfRatiosItem}, paramItems...)

var paramItems = []CheckItem{
	{
		Title:  "关键参数",
		Anchor: "important_vars",
		SQL: `
SELECT VARIABLE_NAME, VARIABLE_VALUE
FROM performance_schema.global_variables
WHERE VARIABLE_NAME IN (
    'validate_password_plugin','skip_grant_tables','datadir','have_ssl','secure_file_priv','version','TIME_ZONE','transaction_isolation',
    'innodb_lock_wait_timeout','max_connections','max_user_connections','slow_query_log',
    'long_query_time','log_queries_not_using_indexes','log_throttle_queries_not_using_indexes',
    'lower_case_table_names','innodb_buffer_pool_size','innodb_flush_log_at_trx_commit',
    'read_only', 'log_slave_updates','innodb_io_capacity','query_cache_type','query_cache_size','max_connect_errors',
    'innodb_thread_concurrency','binlog_format','sync_binlog','join_buffer_size','thread_stack','sort_buffer_size',
    'sort_buffer_size','join_buffer_size','read_buffer_size','read_rnd_buffer_size','thread_stack','binlog_cache_size','tmp_table_size','max_allowed_packet','net_buffer_length','net_read_timeout','net_write_timeout','max_heap_table_size','tmpdir'
);`,
	},
	{
		Title:  "关键状态",
		Anchor: "important_stats",
		SQL: `
SELECT VARIABLE_NAME, VARIABLE_VALUE
FROM performance_schema.global_status
WHERE VARIABLE_NAME IN (
		'uptime',''
);`,
	},
}

// 数据与索引
var tableItems = []CheckItem{
	{
		Title:  "数据库对象统计",
		Anchor: "all_db_objects",
		SQL: `
SELECT SCHEMA_NAME, TYPE, COUNT FROM (
    SELECT table_schema AS SCHEMA_NAME, 'TABLE' AS TYPE, COUNT(*) AS COUNT FROM information_schema.TABLES WHERE table_type='BASE TABLE' GROUP BY table_schema
    UNION ALL
    SELECT event_schema AS SCHEMA_NAME, 'EVENTS' AS TYPE, COUNT(*) AS COUNT FROM information_schema.EVENTS GROUP BY event_schema
    UNION ALL
    SELECT trigger_schema AS SCHEMA_NAME, 'TRIGGERS' AS TYPE, COUNT(*) AS COUNT FROM information_schema.TRIGGERS GROUP BY trigger_schema
    UNION ALL
    SELECT routine_schema AS SCHEMA_NAME, 'PROCEDURE' AS TYPE, COUNT(*) AS COUNT FROM information_schema.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' GROUP BY routine_schema
    UNION ALL
    SELECT routine_schema AS SCHEMA_NAME, 'FUNCTION' AS TYPE, COUNT(*) AS COUNT FROM information_schema.ROUTINES WHERE ROUTINE_TYPE = 'FUNCTION' GROUP BY routine_schema
    UNION ALL
    SELECT TABLE_SCHEMA AS SCHEMA_NAME, 'VIEWS' AS TYPE, COUNT(*) AS COUNT FROM information_schema.VIEWS GROUP BY table_schema
) t
ORDER BY SCHEMA_NAME, TYPE;
        `,
	},
	{
		Title:  "所有数据库文件大小",
		Anchor: "db_file_size",
		SQL: `
SELECT a.SCHEMA_NAME, a.DEFAULT_CHARACTER_SET_NAME, a.DEFAULT_COLLATION_NAME,
       COUNT(b.TABLE_NAME) AS TABLE_COUNT,
       SUM(table_rows) AS TABLE_ROWS,
       TRUNCATE(SUM(data_length)/1024/1024, 2) AS DATA_SIZE_MB,
       TRUNCATE(SUM(index_length)/1024/1024, 2) AS INDEX_SIZE_MB,
       TRUNCATE(SUM(data_length+index_length)/1024/1024, 2) AS ALL_SIZE_MB,
       TRUNCATE(SUM(max_data_length)/1024/1024, 2) AS MAX_SIZE_MB,
       TRUNCATE(SUM(data_free)/1024/1024, 2) AS FREE_SIZE_MB
FROM INFORMATION_SCHEMA.SCHEMATA a
LEFT JOIN information_schema.tables b ON a.SCHEMA_NAME=b.TABLE_SCHEMA
GROUP BY a.SCHEMA_NAME, a.DEFAULT_CHARACTER_SET_NAME, a.DEFAULT_COLLATION_NAME
ORDER BY SUM(data_length) DESC, SUM(index_length) DESC;`,
	},
	{
		Title:  "无主键或唯一键的表（前100）",
		Anchor: "no_pk",
		SQL: `
SELECT table_schema AS TABLE_SCHEMA, table_name AS TABLE_NAME
FROM information_schema.tables
WHERE table_type='BASE TABLE'
AND (table_schema, table_name) NOT IN (
    SELECT table_schema, table_name
    FROM information_schema.table_constraints
    WHERE constraint_type IN ('PRIMARY KEY','UNIQUE')
    AND table_schema NOT IN ('mysql', 'information_schema', 'sys', 'performance_schema')
)
AND table_schema NOT IN ('mysql', 'information_schema', 'sys', 'performance_schema')
LIMIT 100;`,
	},
	{
		Title:  "未使用过的索引",
		Anchor: "sql_unused_indexes",
		SQL:    `SELECT OBJECT_SCHEMA AS OBJECT_SCHEMA, OBJECT_NAME AS OBJECT_NAME, INDEX_NAME AS INDEX_NAME FROM sys.schema_unused_indexes WHERE object_schema NOT IN ('mysql', 'information_schema', 'sys', 'performance_schema');`,
	},
	{
		Title:  "自增ID使用情况（前20）",
		Anchor: "auto_increment",
		SQL: `SELECT table_schema AS TABLE_SCHEMA, table_name AS TABLE_NAME, column_type AS COLUMN_TYPE,
                        (
                            CASE
                                DATA_TYPE
                                WHEN 'tinyint' THEN 255
                                WHEN 'smallint' THEN 65535
                                WHEN 'mediumint' THEN 16777215
                                WHEN 'int' THEN 4294967295
                                WHEN 'bigint' THEN 18446744073709551615
                            END >> IF(LOCATE('unsigned', COLUMN_TYPE) > 0, 0, 1)
                        ) AS max_value,
                        AUTO_INCREMENT as current_value,
                        round(AUTO_INCREMENT / (
                            CASE
                                DATA_TYPE
                                WHEN 'tinyint' THEN 255
                                WHEN 'smallint' THEN 65535
                                WHEN 'mediumint' THEN 16777215
                                WHEN 'int' THEN 4294967295
                                WHEN 'bigint' THEN 18446744073709551615
                            END >> IF(LOCATE('unsigned', COLUMN_TYPE) > 0, 0, 1)
                        ),2) * 100 AS auto_increment_ratio
                    FROM
                        INFORMATION_SCHEMA.COLUMNS
                        INNER JOIN INFORMATION_SCHEMA.TABLES USING (TABLE_SCHEMA, TABLE_NAME)
                    WHERE
                        TABLE_SCHEMA NOT IN ('mysql','INFORMATION_SCHEMA','performance_schema')
                        AND EXTRA = 'auto_increment'order by auto_increment_ratio desc LIMIT 20;`,
	},
	{
		Title:  "启动后未被访问的表",
		Anchor: "silent_table",
		SQL: `SELECT t.table_schema AS TABLE_SCHEMA, t.table_name AS TABLE_NAME, t.table_rows AS TABLE_ROWS, tio.count_read AS COUNT_READ, tio.count_write AS COUNT_WRITE
FROM information_schema.tables AS t
JOIN performance_schema.table_io_waits_summary_by_table AS tio
  ON tio.object_schema = t.table_schema AND tio.object_name = t.table_name
WHERE t.table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
  AND tio.count_write = 0
ORDER BY t.table_schema, t.table_name;`,
	},
	{
		Title:  "表碎片率（前20）",
		Anchor: "table_fragmentation",
		SQL: `
SELECT
  table_schema AS TABLE_SCHEMA,
  table_name AS TABLE_NAME,
  ROUND(data_free/(data_length+index_length+1)*100,2) AS FRAGMENTATION_PCT,
  data_length, index_length, data_free
FROM information_schema.tables
WHERE table_type='BASE TABLE'
  AND table_schema NOT IN ('mysql','information_schema','performance_schema','sys')
  AND (data_length+index_length) > 0
  AND data_free > 0
ORDER BY FRAGMENTATION_PCT DESC
LIMIT 20;
`,
	},
	{
		Title:  "字段数过多的表（前20）",
		Anchor: "too_many_columns",
		SQL:    `SELECT table_schema, table_name, COUNT(*) AS column_count FROM information_schema.columns WHERE table_schema NOT IN ('mysql','information_schema','performance_schema','sys') GROUP BY table_schema, table_name HAVING column_count > 50 ORDER BY column_count DESC LIMIT 20;`,
	},
	{
		Title:  "行长度过大的表（前20）",
		Anchor: "row_length_too_large",
		SQL:    `SELECT table_schema, table_name, AVG_ROW_LENGTH FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('mysql','information_schema','performance_schema','sys') AND AVG_ROW_LENGTH > 65535 ORDER BY AVG_ROW_LENGTH DESC LIMIT 20;`,
	},
	{
		Title:  "TEXT/BLOB字段过多的表（前20）",
		Anchor: "too_many_text_blob",
		SQL:    `SELECT table_schema, table_name, COUNT(*) AS text_blob_count FROM information_schema.columns WHERE table_schema NOT IN ('mysql','information_schema','performance_schema','sys') AND DATA_TYPE IN ('text','blob','mediumtext','longtext','mediumblob','longblob','tinytext','tinyblob') GROUP BY table_schema, table_name HAVING text_blob_count > 3 ORDER BY text_blob_count DESC LIMIT 20;`,
	},
	{
		Title:  "长时间未提交事务（超过60秒）",
		Anchor: "long_running_trx",
		SQL:    `SELECT TRX_STARTED, now() - trx_started as TIME_ELAPSED, TRX_ROWS_LOCKED, TRX_ROWS_MODIFIED, concat(proc.user, '@', proc.host) as ACCOUNT, proc.db as SCHEMA_NAME, concat('kill ',proc.id,';') as KILL_QUERY FROM information_schema.innodb_trx as trx INNER JOIN information_schema.processlist as proc ON trx.trx_mysql_thread_id=proc.id WHERE trx.trx_state='RUNNING' AND proc.command='Sleep' AND proc.time > 60;`,
	},
	// Deprecated Table Engine (e.g., MyISAM, MEMORY, CSV, ARCHIVE)
	{
		Title:  "已废弃表引擎",
		Anchor: "deprecated_table_engine",
		SQL:    `SELECT table_schema, table_name, engine FROM information_schema.tables WHERE engine IN ('MyISAM','MEMORY','CSV','ARCHIVE') AND table_schema NOT IN ('mysql','information_schema','performance_schema','sys');`,
	},
	// Mixed Table Collation (schemas with more than one collation)
	{
		Title:  "表排序规则混用",
		Anchor: "mixed_table_collation",
		SQL:    `SELECT table_schema, COUNT(DISTINCT table_collation) AS collation_count FROM information_schema.tables WHERE table_schema NOT IN ('mysql','information_schema','performance_schema','sys') GROUP BY table_schema HAVING collation_count > 1;`,
	},
	// Mixed Table Character Set (schemas with more than one charset)
	{
		Title:  "表字符集混用",
		Anchor: "mixed_table_charset",
		SQL:    `SELECT table_schema, COUNT(DISTINCT SUBSTRING_INDEX(table_collation, '_', 1)) AS charset_count FROM information_schema.tables WHERE table_schema NOT IN ('mysql','information_schema','performance_schema','sys') GROUP BY table_schema HAVING charset_count > 1;`,
	},
	// Table Foreign Key Existence
	{
		Title:  "存在外键的表",
		Anchor: "table_foreign_key",
		SQL:    `SELECT table_schema, table_name, constraint_name FROM information_schema.table_constraints WHERE constraint_type = 'FOREIGN KEY' AND table_schema NOT IN ('mysql','information_schema','performance_schema','sys');`,
	},
	// Duplicated Index Check
	{
		Title:  "重复索引",
		Anchor: "duplicated_index",
		SQL: `SELECT
  t1.table_schema,
  t1.table_name,
  t1.index_name AS redundant_index,
  GROUP_CONCAT(t1.column_name ORDER BY t1.seq_in_index) AS redundant_cols,
  GROUP_CONCAT(t1.seq_in_index ORDER BY t1.seq_in_index) AS redundant_col_pos
FROM information_schema.statistics t1
JOIN information_schema.statistics t2
  ON t1.table_schema = t2.table_schema
 AND t1.table_name = t2.table_name
 AND t1.index_name != t2.index_name
 AND t1.seq_in_index = t2.seq_in_index
GROUP BY t1.table_schema, t1.table_name, t1.index_name, t2.index_name
HAVING
  LEFT(GROUP_CONCAT(t2.column_name ORDER BY t2.seq_in_index), LENGTH(redundant_cols)) = redundant_cols
  AND LENGTH(GROUP_CONCAT(t2.column_name ORDER BY t2.seq_in_index)) >= LENGTH(redundant_cols)
ORDER BY t1.table_schema, t1.table_name;`,
	},
}

// SQL与性能
var sqlItems = []CheckItem{
	{
		Title:  "全表扫描SQL（近1月）",
		Anchor: "full_table_scan_sql",
		SQL:    `SELECT digest AS DIGEST, exec_count AS EXEC_COUNT, rows_examined AS ROWS_EXAMINED, query AS QUERY FROM sys.statements_with_full_table_scans WHERE db NOT IN ("performance_schema","mysql","information_schema") AND rows_examined > 100000 AND LAST_SEEN > (NOW() - INTERVAL 1 MONTH) ORDER BY exec_count DESC;`,
	},
	{
		Title:  "报错或警告SQL（近1月）",
		Anchor: "error_warning_sql",
		SQL:    `SELECT digest AS DIGEST, exec_count AS EXEC_COUNT, errors AS ERRORS, warnings AS WARNINGS, query AS QUERY FROM sys.statements_with_errors_or_warnings WHERE LAST_SEEN > (NOW() - INTERVAL 1 MONTH) ORDER BY exec_count DESC;`,
	},
	{
		Title:  "无结果返回的SQL（近1月）",
		Anchor: "noresult_sql",
		SQL: `SELECT digest AS DIGEST, LEFT(DIGEST_TEXT,100) AS QUERY, count_star AS EXEC_COUNT FROM performance_schema.events_statements_summary_by_digest
WHERE
    (
        TRIM(DIGEST_TEXT) LIKE 'SELECT%'
        OR TRIM(DIGEST_TEXT) LIKE 'CREATE%TABLE%SELECT%'
        OR TRIM(DIGEST_TEXT) LIKE 'DELETE%'
        OR TRIM(DIGEST_TEXT) LIKE 'UPDATE%'
        OR TRIM(DIGEST_TEXT) LIKE 'REPLACE%'
    )
    AND SUM_ROWS_SENT = 0
    AND SUM_ROWS_AFFECTED = 0
    AND COUNT_STAR > 20
    AND LAST_SEEN > (NOW() - INTERVAL 1 MONTH)
ORDER BY SUM_ROWS_EXAMINED DESC;`,
	},
	{
		Title:  "当前会话信息",
		Anchor: "processlist",
		SQL:    "SELECT * FROM information_schema.PROCESSLIST;",
	},
	{
		Title:  "锁冲突/等待信息",
		Anchor: "innodb_lock_waits",
		SQL: `SELECT
    r.trx_id waiting_trx_id,
    r.trx_mysql_thread_id waiting_thread,
    r.trx_query waiting_query,
    b.trx_id blocking_trx_id,
    b.trx_mysql_thread_id blocking_thread,
    b.trx_query blocking_query
FROM
    performance_schema.data_lock_waits w
    JOIN performance_schema.data_locks l ON w.REQUESTING_ENGINE_LOCK_ID = l.ENGINE_LOCK_ID
    JOIN information_schema.innodb_trx r ON w.REQUESTING_ENGINE_TRANSACTION_ID = r.trx_id
    JOIN information_schema.innodb_trx b ON w.BLOCKING_ENGINE_TRANSACTION_ID = b.trx_id;`,
	},
	{
		Title:  "慢查询统计",
		Anchor: "slow_query_stats",
		SQL:    `SHOW GLOBAL STATUS LIKE 'Slow_queries';`,
	},
	{
		Title:  "活跃连接来源统计",
		Anchor: "active_conn_stats",
		SQL:    `SELECT host, COUNT(*) AS CONNECTIONS FROM information_schema.processlist GROUP BY host ORDER BY CONNECTIONS DESC;`,
	},
}

// 主从状态
var replicaItems = []CheckItem{
	{
		Title:  "主库状态",
		Anchor: "master_info_status",
		SQL:    `SHOW MASTER STATUS;`,
	},
	{
		Title:  "从库状态",
		Anchor: "slave_info_status",
		SQL:    `SHOW SLAVE STATUS;`,
	},
}

// 账号与权限
var accountItems = []CheckItem{
	{
		Title:  "空密码账号",
		Anchor: "no_password_account",
		SQL:    sqlNoPasswordAccount,
	},
	{
		Title:  "高危权限账号",
		Anchor: "high_risk_accounts",
		SQL:    sqlHighRiskAccounts,
	},
	{
		Title:  "host为%账号",
		Anchor: "wildcard_host_accounts",
		SQL:    sqlWildcardHostAccounts,
	},
	{
		Title:  "启动后未使用过的账号",
		Anchor: "silent_account",
		SQL: `SELECT m_u.user AS USER, m_u.host AS HOST, m_u.password_last_changed AS PASSWD_LAST_CHANGED
FROM mysql.user m_u LEFT JOIN performance_schema.accounts ps_a ON m_u.user = ps_a.user AND ps_a.host = m_u.host
LEFT JOIN information_schema.views is_v
    ON is_v.DEFINER = CONCAT(m_u.User, '@', m_u.Host) AND is_v.security_type = 'DEFINER'
LEFT JOIN information_schema.routines is_r
    ON is_r.DEFINER = CONCAT(m_u.User, '@', m_u.Host) AND is_r.security_type = 'DEFINER'
LEFT JOIN information_schema.events is_e ON is_e.definer = CONCAT(m_u.user, '@', m_u.host)
LEFT JOIN information_schema.triggers is_t ON is_t.definer = CONCAT(m_u.user, '@', m_u.host)
WHERE
ps_a.user IS NULL AND is_v.definer IS NULL AND is_r.definer IS NULL AND is_e.definer IS NULL AND is_t.definer IS NULL AND m_u.account_locked = 'N'
ORDER BY m_u.user, m_u.host;`,
	},
	{
		Title:  "匿名账号",
		Anchor: "anonymous_account",
		SQL:    sqlAnonymousAccount,
	},
}

// 分区表健康（数据与索引模块）
var partitionItem = CheckItem{
	Title:  "分区表统计",
	Anchor: "partitioned_tables",
	SQL:    `SELECT table_schema, table_name, partition_name, table_rows FROM information_schema.partitions WHERE partition_name IS NOT NULL ORDER BY table_schema, table_name;`,
}

const htmlHeader = `
<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>MySQL 巡检报告</title>
<style type="text/css">
body { font: 12px Consolas, monospace; background: #fff; color: #222; }
table.report-table { font: 12px Consolas; color: #222; background: #FFFFCC; border-collapse: collapse; margin: 10px 0; }
th { background: #0066cc; color: #fff; padding: 5px; border: 1px solid #ccc; white-space: nowrap; font-size: 14px; font-weight: bold;}
td { padding: 4px; border: 1px solid #ccc; }
tr:nth-child(odd) { background: #fff; }
tr:hover { background: #ffe; }
h2 { color: #336699; border-bottom: 1px solid #ccc; margin-top: 30px; }
a { color: #336699; text-decoration: none; }
/* 建议区块高亮 */
.advice-high { color: #d32f2f; font-weight: bold; }
.advice-mid { color: #f57c00; }
.advice-low { color: #388e3c; }
.advice-block { background: #fff3e0; border-left: 5px solid #f57c00; padding: 10px; margin: 10px 0; }
/* 目录 */
.dir-list { list-style-type: decimal; margin-left: 20px; }
.dir-list li { margin: 4px 0; transition: background 0.2s; }
.dir-list li:hover { background: #e3f2fd; }
.dir-sublist { list-style-type: disc; margin-left: 20px; }
/* 表格异常值高亮 */
.bad-value { color: #fff; background: #d32f2f; font-weight: bold; padding: 2px 6px; border-radius: 3px; }
</style>
</head>
<body>
<a name="top"></a>
<h1>MySQL数据库巡检报告</h1>
<div style="font-size:13px;color:#888;margin-bottom:10px;">生成时间: <span id="report-time"></span></div>
<hr>
<script type="text/javascript">
// Set report time to current time in local format
document.addEventListener("DOMContentLoaded", function() {
    var now = new Date();
    var y = now.getFullYear();
    var m = ("0" + (now.getMonth() + 1)).slice(-2);
    var d = ("0" + now.getDate()).slice(-2);
    var h = ("0" + now.getHours()).slice(-2);
    var min = ("0" + now.getMinutes()).slice(-2);
    var s = ("0" + now.getSeconds()).slice(-2);
    var timeStr = y + "-" + m + "-" + d + " " + h + ":" + min + ":" + s;
    document.getElementById("report-time").textContent = timeStr;
});
</script>
`

const htmlFooter = `
</body>
</html>
`

func renderTable(rows *sql.Rows) (string, error) {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	var sb strings.Builder
	sb.WriteString("<table class=\"report-table\">\n<tr>")
	for _, col := range columns {
		sb.WriteString("<th>")
		sb.WriteString(htmlEscape(col))
		sb.WriteString("</th>")
	}
	sb.WriteString("</tr>\n")
	for rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return "", err
		}
		sb.WriteString("<tr>")
		for _, val := range values {
			var v string
			switch t := val.(type) {
			case nil:
				v = ""
			case []byte:
				v = string(t)
			default:
				v = fmt.Sprintf("%v", t)
			}
			// 简单高亮异常值（如包含"bad"关键字）
			if strings.Contains(strings.ToLower(v), "bad") {
				sb.WriteString("<td class=\"bad-value\">")
				sb.WriteString(htmlEscape(v))
				sb.WriteString("</td>")
			} else {
				sb.WriteString("<td>")
				sb.WriteString(htmlEscape(v))
				sb.WriteString("</td>")
			}
		}
		sb.WriteString("</tr>\n")
	}
	sb.WriteString("</table>\n")
	return sb.String(), nil
}

func htmlEscape(s string) string {
	replacer := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&#39;",
	)
	return replacer.Replace(s)
}

// 新增：性能关键比率模块
func fetchStatusAndVars(db *sql.DB) (map[string]string, map[string]string) {
	status := make(map[string]string)
	vars := make(map[string]string)

	rows, err := db.Query("SHOW GLOBAL STATUS")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var k, v string
			rows.Scan(&k, &v)
			status[k] = v
		}
	}

	rows2, err2 := db.Query("SHOW GLOBAL VARIABLES")
	if err2 == nil {
		defer rows2.Close()
		for rows2.Next() {
			var k, v string
			rows2.Scan(&k, &v)
			vars[k] = v
		}
	}

	return status, vars
}

func highlightIfBad(val float64, threshold float64, isLowerBetter bool) string {
	if (isLowerBetter && val > threshold) || (!isLowerBetter && val < threshold) {
		return fmt.Sprintf(`<span class=\"bad-value\">%.2f%%</span>`, val)
	}
	return fmt.Sprintf("%.2f%%", val)
}

func renderPerfRatios(db *sql.DB) string {
	status, vars := fetchStatusAndVars(db)
	getF := func(m map[string]string, k string) float64 {
		v, _ := strconv.ParseFloat(m[k], 64)
		return v
	}
	sb := strings.Builder{}
	sb.WriteString(`<table class="report-table"><tr><th>指标</th><th>数值</th><th>说明</th><th>计算公式</th></tr>`)

	// QPS (Queries Per Second)
	uptime := getF(status, "Uptime")
	queries := getF(status, "Queries")
	qps := 0.0
	if uptime > 0 {
		qps = queries / uptime
	}
	qpsDesc := "每秒查询数，衡量数据库整体负载"
	sb.WriteString(fmt.Sprintf("<tr><td>QPS</td><td>%.2f</td><td>%s</td><td>Queries/Uptime</td></tr>", qps, qpsDesc))

	// TPS (Transactions Per Second)
	comCommit := getF(status, "Com_commit")
	comRollback := getF(status, "Com_rollback")
	tps := 0.0
	if uptime > 0 {
		tps = (comCommit + comRollback) / uptime
	}
	tpsDesc := "每秒事务数，反映事务处理能力"
	sb.WriteString(fmt.Sprintf("<tr><td>TPS</td><td>%.2f</td><td>%s</td><td>(Com_commit+Com_rollback)/Uptime</td></tr>", tps, tpsDesc))

	// 慢查询比例
	slow := getF(status, "Slow_queries")
	questions := getF(status, "Questions")
	slowRatio := 0.0
	if questions > 0 {
		slowRatio = slow / questions * 100
	}
	slowDesc := "慢查询占比，小于1%为佳，过高说明SQL性能瓶颈，建议优化慢SQL"
	sb.WriteString(fmt.Sprintf("<tr><td>慢查询比例</td><td>%s</td><td>%s</td><td>Slow_queries/Questions</td></tr>", highlightIfBad(slowRatio, 1, true), slowDesc))

	// 表缓存命中率
	openTables := getF(status, "Open_tables")
	openedTables := getF(status, "Opened_tables")
	tableCacheHit := 100.0
	if openedTables > 0 {
		tableCacheHit = openTables / openedTables * 100
	}
	tableCacheDesc := "表缓存命中率，大于95%为佳，低说明table_open_cache过小"
	sb.WriteString(fmt.Sprintf("<tr><td>表缓存命中率</td><td>%s</td><td>%s</td><td>Open_tables/Opened_tables</td></tr>", highlightIfBad(tableCacheHit, 90, false), tableCacheDesc))

	// 线程缓存命中率
	threadsCreated := getF(status, "Threads_created")
	connections := getF(status, "Connections")
	threadCacheHit := 100.0
	if connections > 0 {
		threadCacheHit = 100 - threadsCreated/connections*100
	}
	threadCacheDesc := "线程缓存命中率，大于99%为佳，低说明thread_cache_size过小"
	sb.WriteString(fmt.Sprintf("<tr><td>线程缓存命中率</td><td>%s</td><td>%s</td><td>100-Threads_created/Connections</td></tr>", highlightIfBad(threadCacheHit, 50, false), threadCacheDesc))

	// 连接失败率
	abortedConnects := getF(status, "Aborted_connects")
	connFailRatio := 0.0
	if connections > 0 {
		connFailRatio = abortedConnects / connections * 100
	}
	connFailDesc := "连接失败率，小于1%为佳，过高需排查网络/账号等问题"
	sb.WriteString(fmt.Sprintf("<tr><td>连接失败率</td><td>%s</td><td>%s</td><td>Aborted_connects/Connections</td></tr>", highlightIfBad(connFailRatio, 1, true), connFailDesc))

	// 表定义缓存命中率
	tableDefHits := getF(status, "Table_definition_cache_hits")
	tableDefOpened := getF(status, "Opened_table_definitions")
	tableDefCacheHit := 100.0
	if tableDefHits+tableDefOpened > 0 {
		tableDefCacheHit = tableDefHits / (tableDefHits + tableDefOpened) * 100
	}
	tableDefDesc := "表定义缓存命中率，大于90%为佳，低说明table_definition_cache过小"
	sb.WriteString(fmt.Sprintf("<tr><td>表定义缓存命中率</td><td>%s</td><td>%s</td><td>Table_definition_cache_hits/(Table_definition_cache_hits+Opened_table_definitions)</td></tr>", highlightIfBad(tableDefCacheHit, 90, false), tableDefDesc))

	// 打开文件利用率
	openFiles := getF(status, "Open_files")
	openFilesLimit := getF(vars, "open_files_limit")
	openFilesUtil := 0.0
	if openFilesLimit > 0 {
		openFilesUtil = openFiles / openFilesLimit * 100
	}
	openFilesDesc := "打开文件利用率，小于85%为佳，过高需增大open_files_limit"
	sb.WriteString(fmt.Sprintf("<tr><td>打开文件利用率</td><td>%s</td><td>%s</td><td>Open_files/open_files_limit</td></tr>", highlightIfBad(openFilesUtil, 85, true), openFilesDesc))

	// QCache 碎片率（仅在 Qcache_total_blocks 存在时显示）
	qcacheTotalBlocks := getF(status, "Qcache_total_blocks")
	qcacheFreeBlocks := getF(status, "Qcache_free_blocks")
	if qcacheTotalBlocks > 0 {
		qcacheFrag := qcacheFreeBlocks / qcacheTotalBlocks * 100
		qcacheDesc := "QCache碎片率，低于20%为佳，过高影响性能，建议关闭query_cache或定期重启"
		sb.WriteString(fmt.Sprintf("<tr><td>QCache碎片率</td><td>%s</td><td>%s</td><td>Qcache_free_blocks/Qcache_total_blocks</td></tr>", highlightIfBad(qcacheFrag, 20, true), qcacheDesc))
	}

	// InnoDB buffer pool 命中率
	innodbReads := getF(status, "Innodb_buffer_pool_reads")
	innodbReadReq := getF(status, "Innodb_buffer_pool_read_requests")
	innodbHit := 100.0
	if innodbReadReq > 0 {
		innodbHit = 100 - innodbReads/innodbReadReq*100
	}
	innodbHitDesc := "InnoDB缓冲池命中率，大于99%为佳，低说明innodb_buffer_pool_size过小"
	sb.WriteString(fmt.Sprintf("<tr><td>InnoDB buffer pool命中率</td><td>%s</td><td>%s</td><td>1-Innodb_buffer_pool_reads/Innodb_buffer_pool_read_requests</td></tr>", highlightIfBad(innodbHit, 99, false), innodbHitDesc))

	// 二进制日志缓存命中率
	binlogUse := getF(status, "Binlog_cache_use")
	binlogDiskUse := getF(status, "Binlog_cache_disk_use")
	binlogHit := 100.0
	if binlogUse > 0 {
		binlogHit = 100 - binlogDiskUse/binlogUse*100
	}
	binlogDesc := "Binlog缓存命中率，大于99%为佳，低说明binlog_cache_size过小"
	sb.WriteString(fmt.Sprintf("<tr><td>Binlog缓存命中率</td><td>%s</td><td>%s</td><td>1-Binlog_cache_disk_use/Binlog_cache_use</td></tr>", highlightIfBad(binlogHit, 99, false), binlogDesc))

	// 表锁命中率
	tableLocksImmediate := getF(status, "Table_locks_immediate")
	tableLocksWaited := getF(status, "Table_locks_waited")
	lockHit := 100.0
	if tableLocksImmediate+tableLocksWaited > 0 {
		lockHit = tableLocksImmediate / (tableLocksImmediate + tableLocksWaited) * 100
	}
	lockHitDesc := "表锁命中率，大于95%为佳，低说明并发冲突严重"
	sb.WriteString(fmt.Sprintf("<tr><td>表锁命中率</td><td>%s</td><td>%s</td><td>Table_locks_immediate/(Table_locks_immediate+Table_locks_waited)</td></tr>", highlightIfBad(lockHit, 95, false), lockHitDesc))

	// 临时表磁盘占比
	tmpDisk := getF(status, "Created_tmp_disk_tables")
	tmpMem := getF(status, "Created_tmp_tables")
	tmpRatio := 0.0
	if tmpDisk+tmpMem > 0 {
		tmpRatio = tmpDisk / (tmpDisk + tmpMem) * 100
	}
	tmpDesc := "磁盘临时表占比，小于25%为佳，过高需增大tmp_table_size/max_heap_table_size"
	sb.WriteString(fmt.Sprintf("<tr><td>磁盘临时表占比</td><td>%s</td><td>%s</td><td>Created_tmp_disk_tables/(Created_tmp_tables+Created_tmp_disk_tables)</td></tr>", highlightIfBad(tmpRatio, 25, true), tmpDesc))

	// 排序磁盘占比
	sortMerge := getF(status, "Sort_merge_passes")
	sortScan := getF(status, "Sort_scan")
	sortRange := getF(status, "Sort_range")
	sortTotal := sortScan + sortRange
	sortRatio := 0.0
	if sortTotal > 0 {
		sortRatio = sortMerge / sortTotal * 100
	}
	sortDesc := "磁盘排序占比，小于10%为佳，过高需增大sort_buffer_size"
	sb.WriteString(fmt.Sprintf("<tr><td>磁盘排序占比</td><td>%s</td><td>%s</td><td>Sort_merge_passes/(Sort_scan+Sort_range)</td></tr>", highlightIfBad(sortRatio, 10, true), sortDesc))

	// 连接使用率
	maxUsed := getF(status, "Max_used_connections")
	maxConn := getF(vars, "max_connections")
	connRatio := 0.0
	if maxConn > 0 {
		connRatio = maxUsed / maxConn * 100
	}
	connDesc := "最大连接使用率，小于80%为佳，过高需关注连接数配置"
	sb.WriteString(fmt.Sprintf("<tr><td>最大连接使用率</td><td>%s</td><td>%s</td><td>Max_used_connections/max_connections</td></tr>", highlightIfBad(connRatio, 80, true), connDesc))

	// InnoDB Dirty Pages Ratio
	innodbDirty := getF(status, "Innodb_buffer_pool_pages_dirty")
	innodbTotal := getF(status, "Innodb_buffer_pool_pages_total")
	innodbDirtyRatio := 0.0
	if innodbTotal > 0 {
		innodbDirtyRatio = innodbDirty / innodbTotal * 100
	}
	innodbDirtyDesc := "InnoDB脏页比例，小于75%为佳，过高需关注刷盘配置"
	sb.WriteString(fmt.Sprintf("<tr><td>InnoDB Dirty Pages Ratio</td><td>%s</td><td>%s</td><td>Innodb_buffer_pool_pages_dirty/Innodb_buffer_pool_pages_total</td></tr>", highlightIfBad(innodbDirtyRatio, 75, true), innodbDirtyDesc))

	// per_thread_buffers sum (MB)
	perThreadBuffers := getF(vars, "sort_buffer_size") + getF(vars, "join_buffer_size") + getF(vars, "read_buffer_size") + getF(vars, "read_rnd_buffer_size") + getF(vars, "thread_stack") + getF(vars, "binlog_cache_size")
	perThreadBuffersMB := perThreadBuffers / 1024.0 / 1024.0
	perThreadBuffersDesc := "每线程内存消耗总和，影响并发时内存占用"
	sb.WriteString(fmt.Sprintf("<tr><td>per_thread_buffers</td><td>%.2f MB</td><td>%s</td><td>sort_buffer_size + join_buffer_size + read_buffer_size + read_rnd_buffer_size + thread_stack + binlog_cache_size</td></tr>", perThreadBuffersMB, perThreadBuffersDesc))

	maxConnections := getF(vars, "max_connections")
	maxUsedConnections := getF(status, "Max_used_connections")
	totalPerThreadBuffers := perThreadBuffers * maxConnections
	maxTotalPerThreadBuffers := perThreadBuffers * maxUsedConnections
	totalPerThreadBuffersGB := totalPerThreadBuffers / 1024.0 / 1024.0 / 1024.0
	maxTotalPerThreadBuffersGB := maxTotalPerThreadBuffers / 1024.0 / 1024.0 / 1024.0
	totalPerThreadBuffersDesc := "所有连接最大可能消耗的线程内存"
	maxTotalPerThreadBuffersDesc := "最大并发下线程内存总消耗估算"
	sb.WriteString(fmt.Sprintf("<tr><td>total_per_thread_buffers</td><td>%.2f GB</td><td>%s</td><td>per_thread_buffers × max_connections</td></tr>", totalPerThreadBuffersGB, totalPerThreadBuffersDesc))
	sb.WriteString(fmt.Sprintf("<tr><td>max_total_per_thread_buffers</td><td>%.2f GB</td><td>%s</td><td>per_thread_buffers × Max_used_connections</td></tr>", maxTotalPerThreadBuffersGB, maxTotalPerThreadBuffersDesc))

	tmpTableSize := getF(vars, "tmp_table_size")
	maxHeapTableSize := getF(vars, "max_heap_table_size")
	maxTmpTableSize := min(tmpTableSize, maxHeapTableSize)
	maxTmpTableSizeGB := maxTmpTableSize / 1024.0 / 1024.0 / 1024.0
	maxTmpTableSizeDesc := "单个内存临时表最大值，影响大查询时内存消耗"
	sb.WriteString(fmt.Sprintf("<tr><td>max_tmp_table_size</td><td>%.2f GB</td><td>%s</td><td>min(tmp_table_size, max_heap_table_size)</td></tr>", maxTmpTableSizeGB, maxTmpTableSizeDesc))

	keyBufferSize := getF(vars, "key_buffer_size")
	innodbBufferPoolSize := getF(vars, "innodb_buffer_pool_size")
	innodbLogBufferSize := getF(vars, "innodb_log_buffer_size")
	queryCacheSize := getF(vars, "query_cache_size")
	version := vars["version"]
	serverBuffers := keyBufferSize + maxTmpTableSize + innodbBufferPoolSize + innodbLogBufferSize
	if !strings.HasPrefix(version, "8.0") {
		serverBuffers += queryCacheSize
	}
	serverBuffersGB := serverBuffers / 1024.0 / 1024.0 / 1024.0
	serverBuffersDesc := "全局缓冲区内存消耗总和"
	sb.WriteString(fmt.Sprintf("<tr><td>server_buffers</td><td>%.2f GB</td><td>%s</td><td>key_buffer_size + max_tmp_table_size + innodb_buffer_pool_size + innodb_log_buffer_size [+ query_cache_size if <8.0]</td></tr>", serverBuffersGB, serverBuffersDesc))

	// max_used_memory
	maxUsedMemory := serverBuffers + maxTotalPerThreadBuffers
	maxUsedMemoryGB := maxUsedMemory / 1024.0 / 1024.0 / 1024.0
	maxUsedMemoryDesc := "历史最大内存消耗估算"
	sb.WriteString(fmt.Sprintf("<tr><td>max_used_memory</td><td>%.2f GB</td><td>%s</td><td>server_buffers + max_total_per_thread_buffers</td></tr>", maxUsedMemoryGB, maxUsedMemoryDesc))

	// total_possible_used_memory
	totalPossibleUsedMemory := serverBuffers + totalPerThreadBuffers
	totalPossibleUsedMemoryGB := totalPossibleUsedMemory / 1024.0 / 1024.0 / 1024.0
	totalPossibleUsedMemoryDesc := "理论最大内存消耗估算"
	sb.WriteString(fmt.Sprintf("<tr><td>total_possible_used_memory</td><td>%.2f GB</td><td>%s</td><td>server_buffers + total_per_thread_buffers</td></tr>", totalPossibleUsedMemoryGB, totalPossibleUsedMemoryDesc))

	// pct_physical_memory
	memInfo, _ := mem.VirtualMemory()
	physicalMemory := float64(memInfo.Total)
	pctPhysicalMemory := 0.0
	if physicalMemory > 0 {
		pctPhysicalMemory = totalPossibleUsedMemory * 100 / physicalMemory
	}
	pctPhysicalMemoryDesc := "理论最大内存消耗占物理内存百分比"
	sb.WriteString(fmt.Sprintf("<tr><td>pct_physical_memory</td><td>%.2f%%</td><td>%s</td><td>total_possible_used_memory / physical_memory</td></tr>", pctPhysicalMemory, pctPhysicalMemoryDesc))

	sb.WriteString("</table><div style=\"text-align:right;\"><a href=\"#top\">回到目录</a></div><hr>")
	return sb.String()
}

// 系统信息渲染
func renderSysInfo() string {
	hostInfo, _ := host.Info()
	memInfo, _ := mem.VirtualMemory()
	swapInfo, _ := mem.SwapMemory()
	cpuInfo, _ := cpu.Info()
	cpuCount, _ := cpu.Counts(true)
	loadAvg, _ := load.Avg()
	diskStat, _ := disk.Usage("/")

	var sb strings.Builder
	sb.WriteString(`<a name="sysinfo"></a><h2>系统信息</h2><table>`) // anchor for directory
	sb.WriteString("<tr><th>项</th><th>值</th></tr>")
	sb.WriteString(fmt.Sprintf("<tr><td>主机名</td><td>%s</td></tr>", htmlEscape(hostInfo.Hostname)))
	sb.WriteString(fmt.Sprintf("<tr><td>操作系统</td><td>%s %s</td></tr>", htmlEscape(hostInfo.Platform), htmlEscape(hostInfo.PlatformVersion)))
	sb.WriteString(fmt.Sprintf("<tr><td>内核</td><td>%s</td></tr>", htmlEscape(hostInfo.KernelVersion)))
	sb.WriteString(fmt.Sprintf("<tr><td>架构</td><td>%s</td></tr>", htmlEscape(hostInfo.KernelArch)))
	sb.WriteString(fmt.Sprintf("<tr><td>开机时长</td><td>%.2f 小时</td></tr>", float64(hostInfo.Uptime)/3600))
	sb.WriteString(fmt.Sprintf("<tr><td>CPU物理核数</td><td>%d</td></tr>", cpuCount))
	if len(cpuInfo) > 0 {
		sb.WriteString(fmt.Sprintf("<tr><td>CPU型号</td><td>%s</td></tr>", htmlEscape(cpuInfo[0].ModelName)))
	}
	sb.WriteString(fmt.Sprintf("<tr><td>CPU负载(1/5/15min)</td><td>%.2f / %.2f / %.2f</td></tr>", loadAvg.Load1, loadAvg.Load5, loadAvg.Load15))
	sb.WriteString(fmt.Sprintf("<tr><td>内存总量</td><td>%.2f GB</td></tr>", float64(memInfo.Total)/1024/1024/1024))
	sb.WriteString(fmt.Sprintf("<tr><td>内存可用</td><td>%.2f GB</td></tr>", float64(memInfo.Available)/1024/1024/1024))
	sb.WriteString(fmt.Sprintf("<tr><td>Swap总量</td><td>%.2f GB</td></tr>", float64(swapInfo.Total)/1024/1024/1024))
	sb.WriteString(fmt.Sprintf("<tr><td>Swap已用</td><td>%.2f GB</td></tr>", float64(swapInfo.Used)/1024/1024/1024))
	sb.WriteString(fmt.Sprintf("<tr><td>根分区总空间</td><td>%.2f GB</td></tr>", float64(diskStat.Total)/1024/1024/1024))
	sb.WriteString(fmt.Sprintf("<tr><td>根分区可用空间</td><td>%.2f GB</td></tr>", float64(diskStat.Free)/1024/1024/1024))
	sb.WriteString("</table><div style=\"text-align:right;\"><a href=\"#top\">回到目录</a></div><hr>")
	return sb.String()
}

// 目录分块渲染，调整顺序
func renderDirectory() string {
	type dirBlock struct {
		Title  string
		Items  []CheckItem
		Anchor string
	}
	blocks := []dirBlock{
		{"系统信息", nil, "sysinfo"},
		{"巡检建议", nil, "advice"},
		{"指标与参数", paramItemsWithPerf, "指标与参数"},
		{"数据与索引", tableItems, "数据与索引"},
		{"SQL与性能", sqlItems, "SQL与性能"},
		{"账号与权限", accountItems, "账号与权限"},
		{"主从状态", replicaItems, "主从状态"},
	}
	var sb strings.Builder
	sb.WriteString("<h2>目录</h2><ol class=\"dir-list\">")
	for _, block := range blocks {
		sb.WriteString(fmt.Sprintf(`<li><a href="#%s">%s</a>`, block.Anchor, block.Title))
		if len(block.Items) > 0 {
			sb.WriteString(`<ul class="dir-sublist">`)
			for _, item := range block.Items {
				sb.WriteString(fmt.Sprintf(`<li><a href="#%s">%s</a></li>`, item.Anchor, item.Title))
			}
			sb.WriteString("</ul>")
		}
		sb.WriteString("</li>")
	}
	sb.WriteString("</ol><hr>")
	return sb.String()
}

// 内容分块渲染
func renderModule(title string, items []CheckItem, db *sql.DB) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`<a name="%s"></a><h2>%s</h2>`, title, title))
	for _, item := range items {
		sb.WriteString(fmt.Sprintf(`<a name="%s"></a>`, item.Anchor))
		sb.WriteString(fmt.Sprintf("<h3>%s</h3>", item.Title))
		if item.Anchor == "perf_ratios" {
			sb.WriteString(renderPerfRatios(db))
			continue
		}
		rows, err := db.Query(item.SQL)
		if err != nil {
			sb.WriteString(fmt.Sprintf(`<div style="color:red;">SQL执行失败: %s<br>错误: %s</div>`, htmlEscape(item.SQL), htmlEscape(err.Error())))
			continue
		}
		if item.Anchor == "slave_info_status" {
			tableHtml, err := renderSlaveStatusTable(rows)
			if err != nil {
				sb.WriteString(fmt.Sprintf(`<div style="color:red;">结果渲染失败: %s</div>`, htmlEscape(err.Error())))
				continue
			}
			sb.WriteString(tableHtml)
			continue
		}
		tableHtml, err := renderTable(rows)
		if err != nil {
			sb.WriteString(fmt.Sprintf(`<div style="color:red;">结果渲染失败: %s</div>`, htmlEscape(err.Error())))
			continue
		}
		sb.WriteString(tableHtml)
	}
	sb.WriteString(`<div style="text-align:right;"><a href="#top">回到目录</a></div><hr>`)
	return sb.String()
}

// 新增：只渲染SHOW SLAVE STATUS关键字段
func renderSlaveStatusTable(rows *sql.Rows) (string, error) {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	var sb strings.Builder
	// 只保留关键字段
	keyFields := map[string]bool{
		"Slave_IO_Running":      true,
		"Slave_SQL_Running":     true,
		"Seconds_Behind_Master": true,
		"Last_IO_Error":         true,
		"Last_SQL_Error":        true,
	}
	// 找到关键字段的下标
	var keyIdxs []int
	var keyNames []string
	for i, col := range columns {
		if keyFields[col] {
			keyIdxs = append(keyIdxs, i)
			keyNames = append(keyNames, col)
		}
	}
	sb.WriteString("<table>\n<tr>")
	for _, col := range keyNames {
		sb.WriteString("<th>")
		sb.WriteString(htmlEscape(col))
		sb.WriteString("</th>")
	}
	sb.WriteString("</tr>\n")
	for rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return "", err
		}
		sb.WriteString("<tr>")
		for _, idx := range keyIdxs {
			var v string
			switch t := values[idx].(type) {
			case nil:
				v = ""
			case []byte:
				v = string(t)
			default:
				v = fmt.Sprintf("%v", t)
			}
			sb.WriteString("<td>")
			sb.WriteString(htmlEscape(v))
			sb.WriteString("</td>")
		}
		sb.WriteString("</tr>\n")
	}
	sb.WriteString("</table>\n")
	return sb.String(), nil
}

// 通用：查询SQL返回的第一个int计数值，出错返回0
func queryCount(db *sql.DB, sql string) int {
	var count int
	row := db.QueryRow(sql)
	if err := row.Scan(&count); err != nil {
		return 0
	}
	return count
}

// 建议分析
func analyzeAdvices(db *sql.DB) []Advice {
	var advices []Advice

	// 1. 存在 host为%账号 -> 中级
	hostWildcardCount := queryCount(db, sqlWildcardHostAccountsCount)
	if hostWildcardCount > 0 {
		advices = append(advices, Advice{"中", "存在host为%的账号，建议收敛账号host范围", "wildcard_host_accounts"})
	}

	// 2. 存在无主键的表 -> 中级
	noPKCount := queryCount(db, `SELECT COUNT(*) FROM information_schema.tables WHERE table_type='BASE TABLE' AND (table_schema, table_name) NOT IN (SELECT table_schema, table_name FROM information_schema.table_constraints WHERE constraint_type IN ('PRIMARY KEY','UNIQUE') AND table_schema NOT IN ('mysql', 'information_schema', 'sys', 'performance_schema')) AND table_schema NOT IN ('mysql', 'information_schema', 'sys', 'performance_schema');`)
	if noPKCount > 0 {
		advices = append(advices, Advice{"中", "存在无主键的表，建议为表添加主键或唯一键", "no_pk"})
	}

	// 3. 存在空密码账号 -> 高级
	emptyPwdCount := queryCount(db, sqlNoPasswordAccountCount)
	if emptyPwdCount > 0 {
		advices = append(advices, Advice{"高", "存在空密码账号，建议立即设置强密码", "no_password_account"})
	}

	// 3.1 存在匿名账号 -> 高级
	anonymousCount := queryCount(db, sqlAnonymousAccountCount)
	if anonymousCount > 0 {
		advices = append(advices, Advice{"高", "存在匿名账号（user为空），建议删除匿名账号", "anonymous_account"})
	}

	// 4. 高危账号大于1个 -> 高级
	highRiskCount := queryCount(db, sqlHighRiskAccountsCount)
	if highRiskCount > 1 {
		advices = append(advices, Advice{"高", "高危权限账号数量大于1个，建议收敛高危账号", "high_risk_accounts"})
	}

	// 5. 表碎片率高
	fragCount := queryCount(db, `SELECT COUNT(*) FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('mysql','information_schema','performance_schema','sys') AND (data_length+index_length) > 0 AND data_free/(data_length+index_length+1) > 0.3;`)
	if fragCount > 0 {
		advices = append(advices, Advice{"中", "存在碎片率较高的表，建议定期OPTIMIZE TABLE", "table_fragmentation"})
	}

	// 6. 死锁/锁等待
	deadlockCount := queryCount(db, `SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_state='ROLLING BACK';`)
	if deadlockCount > 0 {
		advices = append(advices, Advice{"高", "存在死锁事务，建议优化相关SQL和表结构", "innodb_deadlocks"})
	}
	lockWaitCount := queryCount(db, `SELECT COUNT(*) FROM performance_schema.data_lock_waits;`)
	if lockWaitCount > 0 {
		advices = append(advices, Advice{"中", "存在锁等待，建议优化并发SQL", "innodb_lock_waits"})
	}

	// 7. 慢查询
	// SHOW GLOBAL STATUS LIKE 'Slow_queries' 返回两列
	var slowQueryCount int
	row := db.QueryRow(`SHOW GLOBAL STATUS LIKE 'Slow_queries';`)
	var name string
	if err := row.Scan(&name, &slowQueryCount); err == nil {
		if slowQueryCount > 1000 {
			advices = append(advices, Advice{"中", "慢查询数量较多，建议优化慢SQL", "slow_query_stats"})
		}
	}

	// 9. 活跃连接来源
	connCount := queryCount(db, `SELECT COUNT(*) FROM information_schema.processlist GROUP BY host ORDER BY CONNECTIONS DESC LIMIT 1;`)
	if connCount > 100 {
		advices = append(advices, Advice{"中", "存在单一IP连接数过高，建议排查异常", "active_conn_stats"})
	}

	// 10. 分区表健康
	partCount := queryCount(db, `SELECT COUNT(*) FROM information_schema.partitions WHERE partition_name IS NOT NULL;`)
	if partCount > 50 {
		advices = append(advices, Advice{"中", "分区表数量较多，建议关注分区维护", "partitioned_tables"})
	}

	// 11. 字段数过多
	colCount := queryCount(db, `SELECT COUNT(*) FROM (SELECT table_schema, table_name, COUNT(*) AS column_count FROM information_schema.columns WHERE table_schema NOT IN ('mysql','information_schema','performance_schema','sys') GROUP BY table_schema, table_name HAVING column_count > 50) t;`)
	if colCount > 0 {
		advices = append(advices, Advice{"中", "存在字段数过多的表，建议优化表结构", "too_many_columns"})
	}

	// 12. 行长度过大
	rowLenCount := queryCount(db, `SELECT COUNT(*) FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('mysql','information_schema','performance_schema','sys') AND AVG_ROW_LENGTH > 65535;`)
	if rowLenCount > 0 {
		advices = append(advices, Advice{"中", "存在行长度过大的表，建议优化表结构", "row_length_too_large"})
	}

	// 13. TEXT/BLOB字段过多
	textBlobCount := queryCount(db, `SELECT COUNT(*) FROM (SELECT table_schema, table_name, COUNT(*) AS text_blob_count FROM information_schema.columns WHERE table_schema NOT IN ('mysql','information_schema','performance_schema','sys') AND DATA_TYPE IN ('text','blob','mediumtext','longtext','mediumblob','longblob','tinytext','tinyblob') GROUP BY table_schema, table_name HAVING text_blob_count > 3) t;`)
	if textBlobCount > 0 {
		advices = append(advices, Advice{"中", "存在TEXT/BLOB字段过多的表，建议优化字段类型", "too_many_text_blob"})
	}

	// 14. 长时间未提交事务
	longTrxCount := queryCount(db, `SELECT COUNT(*) FROM information_schema.innodb_trx as trx INNER JOIN information_schema.processlist as proc ON trx.trx_mysql_thread_id=proc.id WHERE trx.trx_state='RUNNING' AND proc.command='Sleep' AND proc.time > 60;`)
	if longTrxCount > 0 {
		advices = append(advices, Advice{"中", "存在长时间未提交的事务，建议及时处理或终止", "long_running_trx"})
	}

	// 检查密码验证插件
	var pluginName, pluginStatus string
	row = db.QueryRow("SELECT PLUGIN_NAME, PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='validate_password';")
	err := row.Scan(&pluginName, &pluginStatus)
	if err != nil || pluginStatus != "ACTIVE" {
		advices = append(advices, Advice{"中", "未启用密码验证插件validate_password，建议启用以提升账号安全性", "validate_password_plugin"})
	}

	return advices
}

// 建议渲染，去掉SQL，仅加相关锚点链接
func renderAdvices(advices []Advice) string {
	var sb strings.Builder
	sb.WriteString(`<a name="advice"></a><h2>巡检建议</h2>`)
	if len(advices) == 0 {
		sb.WriteString(`<div style="color:green;">未发现高风险项</div>`)
	} else {
		sb.WriteString("<div class=\"advice-block\"><ul>")
		for _, a := range advices {
			levelClass := "advice-low"
			if a.Level == "高" {
				levelClass = "advice-high"
			} else if a.Level == "中" {
				levelClass = "advice-mid"
			}
			if a.RelatedSQL != "" {
				sb.WriteString(fmt.Sprintf(`<li><span class="%s">%s</span> %s <a href="#%s">[相关信息]</a></li>`, levelClass, a.Level, htmlEscape(a.Content), a.RelatedSQL))
			} else {
				sb.WriteString(fmt.Sprintf(`<li><span class="%s">%s</span> %s</li>`, levelClass, a.Level, htmlEscape(a.Content)))
			}
		}
		sb.WriteString("</ul></div>")
	}
	sb.WriteString(`<div style="text-align:right;"><a href="#top">回到目录</a></div><hr>`)
	return sb.String()
}

var showOSInfo bool

var CheckupCmd = &cobra.Command{
	Use:   "checkup",
	Short: "MySQL健康巡检，输出HTML报告",
	Run: func(cmd *cobra.Command, args []string) {
		dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s", dbUser, dbPassWd, dbHost, dbPort, database)

		db := mysqlConnect(dsn)
		defer db.Close()

		filename := "mysql_checkup_report.html"
		f, err := os.Create(filename)
		ifErrWithLog(err)
		defer f.Close()

		f.WriteString(htmlHeader)
		f.WriteString(renderDirectory())
		if showOSInfo {
			f.WriteString(renderSysInfo())
		}
		advices := analyzeAdvices(db)
		f.WriteString(renderAdvices(advices))
		f.WriteString(renderModule("指标与参数", paramItemsWithPerf, db))
		f.WriteString(renderModule("数据与索引", tableItems, db))
		f.WriteString(renderModule("SQL与性能", sqlItems, db))
		f.WriteString(renderModule("账号与权限", accountItems, db))
		f.WriteString(renderModule("主从状态", replicaItems, db))
		f.WriteString(htmlFooter)
		fmt.Printf("巡检报告已生成: %s\n", filename)
	},
}

func init() {
	CheckupCmd.Flags().BoolVar(&showOSInfo, "os", false, "是否输出系统信息")
	RootCmd.AddCommand(CheckupCmd)
}
