#!/bin/bash

set -x
set -e

export MYSQ_GPG_KEY=5072E1F5

#add JCE
if [ "x$TRAVIS_JDK_VERSION" == "xoraclejdk7" ]
then
    sudo add-apt-repository -y ppa:webupd8team/java
    sudo apt-get update
    sudo apt-get install oracle-java7-unlimited-jce-policy

else if [ "x$TRAVIS_JDK_VERSION" == "xoraclejdk8" ]
    then
        sudo add-apt-repository -y ppa:webupd8team/java
        sudo apt-get update
        sudo apt-get install oracle-java8-unlimited-jce-policy
    fi
fi

remove_mysql(){
    sudo service mysql stop
    sudo apt-get remove --purge mysql-server mysql-client mysql-common
    sudo apt-get autoremove
    sudo apt-get autoclean
    sudo rm -rf /etc/mysql||true
    sudo rm -rf /var/lib/mysql||true
}

if [ -n "$MYSQL_VERSION" ]
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
    sudo add-apt-repository "deb http://nyc2.mirrors.digitalocean.com/mariadb/repo/${MARIA_VERSION}/ubuntu precise main"

    sudo apt-get update

    sudo apt-get install mariadb-server
fi

sudo tee /etc/mysql/conf.d/map.cnf << END
[mysqld]
max_allowed_packet=$MAX_ALLOWED_PACKET
innodb_log_file_size=$INNODB_LOG_FILE_SIZE
END

# Generate SSL files:
sudo .travis/gen-ssl.sh mariadb.example.com /etc/mysql
sudo chown mysql:mysql /etc/mysql/server.crt /etc/mysql/server.key /etc/mysql/ca.crt

# Enable SSL:
sudo tee /etc/mysql/conf.d/ssl.cnf << END
[mysqld]
ssl-ca=/etc/mysql/ca.crt
ssl-cert=/etc/mysql/server.crt
ssl-key=/etc/mysql/server.key
END

sudo mysql -u root -e "SET GLOBAL innodb_fast_shutdown = 1"
sudo mysql -u root -e "update mysql.user set plugin = 'mysql_native_password' where User = 'root' and Host = 'localhost'"

sudo service mysql stop
sudo rm -f /var/lib/mysql/ib_logfile*
sudo service mysql start

#Adding sleep time if mysql DB. If not SSL not totally initialized when launching tests
if [ "x$MYSQL_VERSION" != "x" ]
then
    sleep 20
fi

sudo mysql -uroot -e "create database IF NOT EXISTS test"
