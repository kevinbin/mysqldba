# MySQL DBA 工具箱

一个综合性的 MySQL 数据库管理工具包，提供部署、监控、诊断和性能分析工具。

## 安装

```bash
go get github.com/kevinbin/mysqldba   
```

## 使用方法

```
欢迎使用 MySQL DBA 工具箱。
作者: HongBin <hongbin119@gmail.com>
版本: 3.0

使用方法:
  mysqldba [命令]

可用命令:
  binlog      分析 MySQL binlog 文件并统计 DML 操作
  deploy      自动部署 MySQL 服务，智能识别架构类型
  diagnose    诊断 MySQL 系统
  doctor      分析 MySQL 信号量
  help        获取任何命令的帮助信息
  monitor     类似 iostat 的 MySQL 监控器
  rbr2sbr     将 rbr 二进制日志转换为 sbr 格式
  repairGtid  比较主库(源)和从库(目标)的 GTID 以发现不一致
  slowlog     捕获 MySQL 慢查询日志
  ssh         SSH 批量操作工具
  trace       分析 SQL 性能

标志:
  -h, --help              显示 mysqldba 的帮助信息
  -H, --host string       mysql 主机 IP (默认 "localhost")
  -p, --password string   mysql 登录密码 (默认 "root")
  -P, --port int          mysql 服务器端口 (默认 3306)
  -u, --user string       mysql 登录用户 (默认 "root")

使用 "mysqldba [命令] --help" 获取关于特定命令的更多信息。
```

## SSH 工具使用示例

```bash
# 批量设置 SSH 免密登录（从 hosts.txt 读取主机列表）
mysqldba ssh setup --host-file hosts.txt

# 批量执行命令
mysqldba ssh exec --command "uname -a"

# 批量传输文件
mysqldba ssh copy --local /path/to/local/file --remote /path/to/remote/file

# 批量传输目录（递归复制整个目录）
mysqldba ssh copy --local /path/to/local/directory --remote /path/to/remote/directory
```

## 功能特性

1. 数据库部署: `mysqldba deploy`
2. 数据库监控: `mysqldba mon`
3. 数据库诊断: `mysqldba diagnose`
4. binlog 分析: `mysqldba binlog`
5. 慢查询分析: `mysqldba slowlog`
6. SQL 跟踪分析: `mysqldba trace`

### 慢查询分析

```bash
$ mysqldba slowlog -u user -p pass -d /path -l 60 -t 0.1
```

### SQL 跟踪分析

跟踪 SQL 执行并收集执行计划、优化器跟踪、表结构和索引信息，用于分析 SQL 性能问题。

```bash
$ mysqldba trace -u user -p pass -H hostname -P port -d database -f query.sql
```

参数说明:
- `-d, --database`: 数据库名称 (必需)
- `-f, --file`: 包含要跟踪的 SQL 查询的文件 (必需)
- `-u, --user`: MySQL 登录用户名 (默认: root)
- `-p, --password`: MySQL 登录密码 (默认: root)
- `-H, --host`: MySQL 主机 IP (默认: localhost)
- `-P, --port`: MySQL 服务端口 (默认: 3306)
- `-r, --recommend-indexes`: 启用索引建议功能
- `-s, --dynamic-selectivity`: 使用动态选择性计算为索引建议提供更准确的结果

执行结果将保存在 trace_{timestamp}.log 文件中。

#### 索引建议功能

通过添加 `-r` 选项，trace 命令可以分析 SQL 语句并提供索引优化建议：

```bash
$ mysqldba trace -u user -p pass -H hostname -P port -d database -f query.sql -r
```

如果需要更准确的索引建议，可以添加 `-s` 选项进行动态选择性计算（会增加处理时间）：

```bash
$ mysqldba trace -u user -p pass -H hostname -P port -d database -f query.sql -r -s
```

索引建议功能会分析：
- WHERE 子句中的过滤条件
- JOIN 条件
- ORDER BY 和 GROUP BY 子句中的列
- 现有索引信息和未使用的索引

并根据分析结果提供合理的索引创建建议，帮助优化 SQL 性能。

## 免责声明

> 此工具为个人项目，未经严格和广泛测试。
> 作者不对使用过程中造成的任何损失承担责任，用户应自行承担使用风险。 