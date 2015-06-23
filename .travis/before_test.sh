#!/bin/bash

set -x
set -e

export MYSQ_GPG_KEY=5072E1F5

remove_mysql(){
    sudo service mysql stop
    sudo apt-get remove --purge mysql-server mysql-client mysql-common
    sudo apt-get autoremove
    sudo apt-get autoclean
    sudo rm -rf /etc/mysql||true
    sudo rm -rf /var/lib/mysql||true
}

if [ "x$MYSQL_VERSION" == "xtravis" ]
then
    :
elif [ "x$MYSQL_VERSION" != "x" ]
then

    remove_mysql

    sudo tee /etc/apt/sources.list.d/mysql.list << END
deb http://repo.mysql.com/apt/ubuntu/ precise mysql-$MYSQL_VERSION
deb-src http://repo.mysql.com/apt/ubuntu/ precise mysql-$MYSQL_VERSION
END

    sudo apt-key adv --keyserver pool.sks-keyservers.net --recv-keys $MYSQ_GPG_KEY

    sudo apt-get update
    sudo apt-get install mysql-server

    dpkg -l|grep ^ii|grep mysql-server|grep ${MYSQL_VERSION/-dmr/}

else
    remove_mysql

    sudo apt-get install python-software-properties

    sudo apt-key adv --recv-keys --keyserver keyserver.ubuntu.com 0xcbcb082a1bb943db
    sudo add-apt-repository "deb http://ftp.igh.cnrs.fr/pub/mariadb/repo/${MARIA_VERSION}/ubuntu precise main"

    sudo apt-get update

    sudo apt-get install mariadb-server
fi

sudo tee /etc/mysql/conf.d/map.cnf << END
[mysqld]
max_allowed_packet=$MAX_ALLOWED_PACKET
innodb_log_file_size=$INNODB_LOG_FILE_SIZE
END

sudo mysql -u root -e "SET GLOBAL innodb_fast_shutdown = 1"
sudo service mysql stop
sudo rm -f /var/lib/mysql/ib_logfile*
sudo service mysql start

cat /etc/mysql/my.cnf

mysql -u root -e "create database test"