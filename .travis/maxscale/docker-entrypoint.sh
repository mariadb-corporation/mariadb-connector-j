#!/bin/bash

set -e

config_file="/etc/maxscale.cnf"

# We start config file creation

cat <<EOF > $config_file


[maxscale]
threads=2
log_messages=1
log_trace=1
log_debug=1

[db]
type=server
address=db
port=3306
protocol=MySQLBackend
authenticator_options=skip_authentication=true
router_options=master

[Galera Monitor]
type=monitor
module=mysqlmon
servers=db
user=boby
passwd=hey
monitor_interval=1000

[qla]
type=filter
module=qlafilter
options=/tmp/QueryLog

[fetch]
type=filter
module=regexfilter
match=fetch
replace=select

[hint]
type=filter
module=hintfilter

[Write Connection Router]
type=service
router=readconnroute
servers=db
user=boby
passwd=hey
router_options=master
localhost_match_wildcard_host=1
version_string=10.2.99-MariaDB-maxscale

[Read Connection Router]
type=service
router=readconnroute
servers=db
user=boby
passwd=hey
router_options=synced
localhost_match_wildcard_host=1
version_string=10.2.99-MariaDB-maxscale

[RW Split Router]
type=service
router=readwritesplit
servers=db
user=boby
passwd=hey
max_slave_connections=100%
localhost_match_wildcard_host=1
router_options=disable_sescmd_history=true
version_string=10.2.99-MariaDB-maxscale

[CLI]
type=service
router=cli

[RW Split Listener]
type=listener
service=RW Split Router
protocol=MySQLClient
port=4006
socket=/var/lib/maxscale/rwsplit.sock

[Write Connection Listener]
type=listener
service=Write Connection Router
protocol=MySQLClient
port=4007
socket=/var/lib/maxscale/writeconn.sock

[Read Connection Listener]
type=listener
service=Read Connection Router
protocol=MySQLClient
port=4008
socket=/var/lib/maxscale/readconn.sock

[CLI Listener]
type=listener
service=CLI
protocol=maxscaled
socket=/tmp/maxadmin.sock

EOF

echo 'creating configuration'
exec "$@"
