# MySQL DBA Toolbox

A comprehensive MySQL database administration toolkit that provides deployment, monitoring, diagnosis, and performance analysis tools.

## Install

```bash
go get github.com/kevinbin/mysqldba   
```

## Usage

```
Welcome to the MySQL DBA Toolbox.
Author: HongBin <hongbin119@gmail.com>
Version: 3.0

Usage:
  mysqldba [command]

Available Commands:
  binlog      Analyze MySQL binlog file and count DML operations
  deploy      Automatically deploy MySQL service with intelligent architecture detection
  diagnose    Diagnostic MySQL system
  doctor      Analyze MySQL semaphore
  help        Help about any command
  monitor     A MySQL monitor like iostat
  rbr2sbr     Convert a rbr binary log to sbr format
  repairGtid  Compare master(source) and slave(destination) GTID to find inconsistent
  slowlog     Capture MySQL slow log
  ssh         SSH batch operation tool
  trace       Analyze SQL performance

Flags:
  -h, --help              help for mysqldba
  -H, --host string       mysql host ip (default "localhost")
  -p, --password string   mysql login password (default "root")
  -P, --port int          mysql server port (default 3306)
  -u, --user string       mysql login user (default "root")

Use "mysqldba [command] --help" for more information about a command.
```

## SSH Tool Usage Examples

```bash
# Batch setup SSH passwordless login (read host list from hosts.txt)
mysqldba ssh setup --host-file hosts.txt

# Batch execute commands
mysqldba ssh exec --command "uname -a"

# Batch transfer files
mysqldba ssh copy --local /path/to/local/file --remote /path/to/remote/file

# Batch transfer directories (recursive copy entire directory)
mysqldba ssh copy --local /path/to/local/directory --remote /path/to/remote/directory
```

## Features

1. Database Deployment: `mysqldba deploy`
2. Database Monitoring: `mysqldba mon`
3. Database Diagnosis: `mysqldba diagnose`
4. Binlog Analysis: `mysqldba binlog`
5. Slow Query Analysis: `mysqldba slowlog`
6. SQL Trace Analysis: `mysqldba trace`

### Slow Query Analysis

```bash
$ mysqldba slowlog -u user -p pass -d /path -l 60 -t 0.1
```

### SQL Trace Analysis

Trace SQL execution and collect execution plans, optimizer traces, table structures, and index information for SQL performance analysis.

```bash
$ mysqldba trace -u user -p pass -H hostname -P port -d database -f query.sql
```

Parameter Description:
- `-d, --database`: Database name (required)
- `-f, --file`: File containing SQL queries to trace (required)
- `-u, --user`: MySQL login username (default: root)
- `-p, --password`: MySQL login password (default: root)
- `-H, --host`: MySQL host IP (default: localhost)
- `-P, --port`: MySQL server port (default: 3306)
- `-r, --recommend-indexes`: Enable index recommendation feature
- `-s, --dynamic-selectivity`: Use dynamic selectivity calculation for more accurate index recommendations

Execution results will be saved in trace_{timestamp}.log file.

#### Index Recommendation Feature

By adding the `-r` option, the trace command can analyze SQL statements and provide index optimization recommendations:

```bash
$ mysqldba trace -u user -p pass -H hostname -P port -d database -f query.sql -r
```

For more accurate index recommendations, you can add the `-s` option for dynamic selectivity calculation (increases processing time):

```bash
$ mysqldba trace -u user -p pass -H hostname -P port -d database -f query.sql -r -s
```

The index recommendation feature analyzes:
- Filter conditions in WHERE clauses
- JOIN conditions
- Columns in ORDER BY and GROUP BY clauses
- Existing index information and unused indexes

And provides reasonable index creation recommendations based on analysis results to help optimize SQL performance.

## Disclaimer

> This tool is a personal project and has not been rigorously and extensively tested. 
> The author shall not be held responsible for any loss caused in the process of use, and the user shall bear the responsibility.
