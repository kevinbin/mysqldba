## Install
```
go get github.com/kevinbin/mysqldba   
```
## Usage
```
Welcome to the MySQL DBA Toolbox.

Version: 2.0

Usage:
  mysqldba [command]

Available Commands:
  doctor      mysql error log anaylze & watch
  help        Help about any command
  monitor     A MySQL monitor like iostat
  rbr2sbr     Convert a rbr binary log to sbr format
  repairGtid  Compare master(source) and slave(destination) GTID to find inconsistent
  slowlog     Capture MySQL slow log

Flags:
  -h, --help              help for mysqldba
  -H, --host string       mysql host ip (default "localhost")
  -p, --password string   mysql login password (default "root")
  -P, --port int          mysql server port (default 3306)
  -u, --user string       mysql login user (default "root")

Use "mysqldba [command] --help" for more information about a command.
```




