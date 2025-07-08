## Install
```
go get github.com/kevinbin/mydba   
```
## Usage
```
Welcome to the MySQL DBA Toolbox.
Author: HongBin <hongbin119@gmail.com>
Version: 3.0

Usage:
  mydba [command]

Available Commands:
  binlog      Analyze MySQL binlog file and count DML operations
  deploy      自动部署MySQL服务，智能识别架构类型
  diagnose    Diagnostic MySQL system
  doctor      Anaylze MySQL semaphore
  help        Help about any command
  monitor     A MySQL monitor like iostat
  rbr2sbr     Convert a rbr binary log to sbr format
  repairGtid  Compare master(source) and slave(destination) GTID to find inconsistent
  slowlog     Capture MySQL slow log
  ssh         SSH批量操作工具
  trace       Analyze SQL performance

Flags:
  -h, --help              help for mydba
  -H, --host string       mysql host ip (default "localhost")
  -p, --password string   mysql login password (default "root")
  -P, --port int          mysql server port (default 3306)
  -u, --user string       mysql login user (default "root")

Use "mydba [command] --help" for more information about a command.
```

## SSH工具使用示例
```
# 批量设置SSH免密登录（从hosts.txt读取主机列表）
mydba ssh setup --host-file hosts.txt

# 批量执行命令
mydba ssh exec --command "uname -a"

# 批量传输文件
mydba ssh copy --local /path/to/local/file --remote /path/to/remote/file

# 批量传输目录（递归复制整个目录）
mydba ssh copy --local /path/to/local/directory --remote /path/to/remote/directory
```

## 功能

1. 数据库部署: `mydba deploy`
2. 数据库监控: `mydba mon`
3. 数据库诊断: `mydba diagnose`
4. binlog分析: `mydba binlog`
5. 慢查询分析: `mydba slowlog`
6. SQL跟踪分析: `mydba trace`

### 慢查询分析
```
$ mydba slowlog -u user -p pass -d /path -l 60 -t 0.1
```

### SQL跟踪分析
跟踪SQL执行并收集执行计划、优化器跟踪、表结构和索引信息，用于分析SQL性能问题。
```
$ mydba trace -u user -p pass -H hostname -P port -d database -f query.sql
```

参数说明:
- `-d, --database`: 数据库名称 (必需)
- `-f, --file`: 包含要跟踪的SQL查询的文件 (必需)
- `-u, --user`: MySQL登录用户名 (默认: root)
- `-p, --password`: MySQL登录密码 (默认: root)
- `-H, --host`: MySQL主机IP (默认: localhost)
- `-P, --port`: MySQL服务端口 (默认: 3306)
- `-r, --recommend-indexes`: 启用索引建议功能
- `-s, --dynamic-selectivity`: 使用动态选择性计算为索引建议提供更准确的结果

执行结果将保存在trace_{timestamp}.log文件中。

#### 索引建议功能
通过添加 `-r` 选项，trace命令可以分析SQL语句并提供索引优化建议：

```
$ mydba trace -u user -p pass -H hostname -P port -d database -f query.sql -r
```

如果需要更准确的索引建议，可以添加 `-s` 选项进行动态选择性计算（会增加处理时间）：

```
$ mydba trace -u user -p pass -H hostname -P port -d database -f query.sql -r -s
```

索引建议功能会分析：
- WHERE子句中的过滤条件
- JOIN条件
- ORDER BY和GROUP BY子句中的列
- 现有索引信息和未使用的索引

并根据分析结果提供合理的索引创建建议，帮助优化SQL性能。

## Disclaimer
> This tool is a personal project and has not been rigorously and extensively tested. 
> The author shall not be held responsible for any loss caused in the process of use, and the user shall bear the responsibility.
