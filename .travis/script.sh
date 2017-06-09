#!/bin/bash

set -x
set -e

case "$TYPE" in
 "MAXSCALE" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:4006/testj?user=root&killFetchStmtOnClose=false&enablePacketDebug=true'
   ;;
 "REWRITE" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root&rewriteBatchedStatements=true&enablePacketDebug=true'
   ;;
 "PREPARE" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root&useServerPrepStmts=true&enablePacketDebug=true'
   ;;
 "MULTI" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root&allowMultiQueries=true&enablePacketDebug=true'
   ;;
 "BULK_SERVER" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root&useBatchMultiSend=true&useServerPrepStmts=true&enablePacketDebug=true'
   ;;
 "NO_BULK_CLIENT" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root&useBatchMultiSend=false&enablePacketDebug=true'
   ;;
 "NO_BULK_SERVER" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root&useBatchMultiSend=false&useServerPrepStmts=true&enablePacketDebug=true'
   ;;
 "COMPRESSION" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root&useCompression=true&enablePacketDebug=true'
   ;;
  *)
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root&enablePacketDebug=true'
   ;;
esac;


if [ -n "$PROFILE" ]
then
    export urlString="$urlString&profileSql=true"
    pwd
    rm src/test/resources/logback-test.xml
    mv src/test/resources/logback-test-travis.xml src/test/resources/logback-test.xml
fi

if [ -n "$AURORA" ]
then
    if [ -n "$AURORA_STRING_URL" ]
    then
        urlString=-DdbUrl=$AURORA_STRING_URL
        testSingleHost=true
    else
        testSingleHost=false
    fi
else
    testSingleHost=true
fi

echo "Running coveralls for JDK version: $TRAVIS_JDK_VERSION with profile jdbc42"
mvn clean test $urlString -DtestSingleHost=$testSingleHost $ADDITIONNAL_VARIABLES -DjobId=$TRAVIS_JOB_ID  \
    -DkeystorePath="/etc/mysql/client-keystore.jks" -DkeystorePassword="kspass"  \
    -Dkeystore2Path="/etc/mysql/fullclient-keystore.jks" -Dkeystore2Password="kspass" -DkeyPassword="kspasskey"  \
    -Dkeystore2PathP12="/etc/mysql/fullclient-keystore.p12"

if [ "$TYPE" == "MAXSCALE" ]
then
sudo service maxscale stop
fi


if [ ! -n "$AURORA" ]
then
    sudo service mysql stop
fi
