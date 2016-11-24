#!/bin/bash

set -x
set -e

remove_mysql(){
    sudo service mysql stop
    sudo apt-get -qq autoremove --purge mysql-server mysql-client mysql-common
    sudo rm -rf /etc/mysql||true
    sudo rm -rf /var/lib/mysql||true
}
remove_mysql

if [ "$TYPE" == "MAXSCALE" ]
then
    #install maxscale
    wget "https://downloads.mariadb.com/MaxScale/${MAXSCALE_VERSION}/ubuntu/dists/precise/main/binary-amd64/maxscale-2.0.1-2.ubuntu.precise.x86_64.deb"
    sudo dpkg -i maxscale-2.0.1-2.ubuntu.precise.x86_64.deb
    sudo apt-get install -f
    sudo sed -i 's/user=myuser/user=root/g' /etc/maxscale.cnf
    sudo sed -i 's/passwd=mypwd/passwd=/g' /etc/maxscale.cnf
    sudo sed -i 's/Service]/Service]\nenable_root_user=1\nversion_string=10.1.18-MariaDB-maxScale/g' /etc/maxscale.cnf
    sudo sed -i 's|port=4008|port=4008\naddress=localhost|g' /etc/maxscale.cnf
    sudo sed -i 's|port=4006|port=4006\naddress=localhost|g' /etc/maxscale.cnf
fi

if [ -n "$AURORA" ]
then
    # AURORA tests doesn't need an installation
    echo "$MYSQL"
else
    if [ -n "$MYSQL" ]
    then
        sudo tee /etc/apt/sources.list.d/mysql.list << END
deb http://repo.mysql.com/apt/ubuntu/ precise mysql-$MYSQL
deb-src http://repo.mysql.com/apt/ubuntu/ precise mysql-$MYSQL
END
        #normal way, but working only 90% of the time. Temporary force with key in project.
        #sudo apt-key adv --keyserver pgp.mit.edu --recv-keys 5072E1F5
        sudo apt-key add .travis/mysql_pubkey.asc

        sudo apt-get -qq update --force-yes
        sudo apt-get -qq install mysql-server --force-yes

        dpkg -l|grep ^ii|grep mysql-server|grep ${MYSQL/-dmr/}

    else

        sudo apt-get -qq install python-software-properties

        sudo apt-key adv --recv-keys --keyserver hkp://keyserver.ubuntu.com:80 0xcbcb082a1bb943db
        sudo add-apt-repository "deb [arch=amd64,i386] http://nyc2.mirrors.digitalocean.com/mariadb/repo/${MARIA}/ubuntu precise main"

        sudo apt-get -qq update

        sudo apt-get -qq install mariadb-server
    fi

    INNODB_LOG_FILE_SIZE=$(echo $PACKET| cut -d'M' -f 1)0M
    sudo tee /etc/mysql/conf.d/map.cnf << END
[mysqld]
max_allowed_packet=$PACKET
innodb_log_file_size=$INNODB_LOG_FILE_SIZE
character_set_server = utf8mb4
END

    # Generate SSL files:
    sudo .travis/gen-ssl.sh mariadb.example.com /etc/mysql
    sudo chown mysql:mysql /etc/mysql/*.crt /etc/mysql/*.key /etc/mysql/*.p12

    # Enable SSL:
    sudo tee /etc/mysql/conf.d/ssl.cnf << END
[mysqld]
ssl-ca=/etc/mysql/ca.crt
ssl-cert=/etc/mysql/server.crt
ssl-key=/etc/mysql/server.key
END

    sudo mysql -u root -e "SET GLOBAL innodb_fast_shutdown = 1"
    sudo mysql -u root -e "update mysql.user set plugin = 'mysql_native_password' where User = 'root' and Host = 'localhost'"
    sudo mysql -u root -e "create database IF NOT EXISTS testj"

    sudo service mysql stop
    #Adding sleep time for clean shutdown
    if [ "x$MYSQL" != "x" ]
    then
        sleep 2
    fi
    sudo rm -f /var/lib/mysql/ib_logfile*
    sudo service mysql start

    #Adding sleep time if mysql DB. If not SSL not totally initialized when launching tests
    if [ "x$MYSQL" != "x" ]
    then
        sleep 20
    fi

fi

if [ "$TYPE" == "MAXSCALE" ]
then

    #add SSL informations
    sudo sed -i 's|Listener]|Listener]\nssl=enabled\nssl_cert=/etc/mysql/server.crt\nssl_key=/etc/mysql/server.key\nssl_ca_cert=/etc/mysql/ca.crt|g' /etc/maxscale.cnf

    sudo service maxscale start
    tail -n500 /var/log/maxscale/maxscale1.log
    tail -n500 /etc/maxscale.cnf

fi
