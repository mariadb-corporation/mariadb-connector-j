#!/bin/bash

set -x
set -e

case "$TYPE" in
 "REWRITE" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root&rewriteBatchedStatements=true'
   ;;
 "MULTI" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root&allowMultiQueries=true'
   ;;
 "BULK_CLIENT" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root&useBatchMultiSend=true&useServerPrepStmts=false'
   ;;
 "NO_BULK_CLIENT" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root&useBatchMultiSend=false&useServerPrepStmts=false'
   ;;
 "NO_BULK_SERVER" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root&useBatchMultiSend=false'
   ;;
 "COMPRESSION" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root&useCompression=true'
   ;;
esac;

if [ -n "$AURORA" ]
then
    testSingleHost=false
else
    testSingleHost=true
fi


mvn clean test $urlString -DtestSingleHost=$testSingleHost $ADDITIONNAL_VARIABLES -DjobId=$TRAVIS_JOB_ID -DkeystorePath="/etc/mysql/client-keystore.p12" -DkeystorePassword="kspass"