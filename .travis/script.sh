#!/bin/bash

set -x
set -e

###################################################################################################################
# test different type of configuration
###################################################################################################################
mysql=( mysql --protocol=tcp -ubob -h127.0.0.1 --port=3305 )
export COMPOSE_FILE=.travis/docker-compose.yml

case "$TYPE" in
 "REWRITE" )
   urlString='jdbc:mariadb://mariadb.example.com:3305/testj?user=bob&rewriteBatchedStatements=true&enablePacketDebug=true'
   ;;
 "PREPARE" )
   urlString='jdbc:mariadb://mariadb.example.com:3305/testj?user=bob&useServerPrepStmts=true&enablePacketDebug=true'
   ;;
 "MULTI" )
   urlString='jdbc:mariadb://mariadb.example.com:3305/testj?user=bob&allowMultiQueries=true&enablePacketDebug=true'
   ;;
 "BULK_SERVER" )
   urlString='jdbc:mariadb://mariadb.example.com:3305/testj?user=bob&useBatchMultiSend=true&useServerPrepStmts=true&enablePacketDebug=true'
   ;;
 "NO_BULK_CLIENT" )
   urlString='jdbc:mariadb://mariadb.example.com:3305/testj?user=bob&useBatchMultiSend=false&enablePacketDebug=true'
   ;;
 "NO_BULK_SERVER" )
   urlString='jdbc:mariadb://mariadb.example.com:3305/testj?user=bob&useBatchMultiSend=false&useServerPrepStmts=true&enablePacketDebug=true'
   ;;
 "COMPRESSION" )
   urlString='jdbc:mariadb://mariadb.example.com:3305/testj?user=bob&useCompression=true&enablePacketDebug=true'
   ;;
  *)
   if [ -n "$MAXSCALE_VERSION" ]
   then
       urlString='jdbc:mariadb://mariadb.example.com:4007/testj?user=bob&killFetchStmtOnClose=false&enablePacketDebug=true'
       mysql=( mysql --protocol=tcp -ubob -h127.0.0.1 --port=4007 )
       export COMPOSE_FILE=.travis/maxscale-compose.yml
   else
       urlString='jdbc:mariadb://mariadb.example.com:3305/testj?user=bob&enablePacketDebug=true'
   fi
   ;;
esac;


if [ -n "$PROFILE" ]
then
    export urlString="$urlString&profileSql=true"
    pwd
    rm src/test/resources/logback-test.xml
    mv src/test/resources/logback-test-travis.xml src/test/resources/logback-test.xml
fi

cmd=( mvn clean test $ADDITIONNAL_VARIABLES -DjobId=${TRAVIS_JOB_ID}  \
    -DkeystorePath="$SSLCERT/client-keystore.jks" \
    -DkeystorePassword="kspass"  \
    -DserverCertificatePath="$SSLCERT/server.crt" \
    -Dkeystore2Path="$SSLCERT/fullclient-keystore.jks" \
    -Dkeystore2Password="kspass" -DkeyPassword="kspasskey"  \
    -Dkeystore2PathP12="$SSLCERT/fullclient-keystore.p12" \
    -DrunLongTest=true )

if [ -n "$AURORA" ]
then
    if [ -n "$AURORA_STRING_URL" ]
    then
        urlString=${AURORA_STRING_URL}
        testSingleHost=true
    else
        testSingleHost=false
    fi
else

    testSingleHost=true

    ###################################################################################################################
    # launch docker server and maxscale
    ###################################################################################################################
    export INNODB_LOG_FILE_SIZE=$(echo ${PACKET}| cut -d'M' -f 1)0M
    docker-compose -f ${COMPOSE_FILE} build
    docker-compose -f ${COMPOSE_FILE} up -d

    ###################################################################################################################
    # launch 3 galera servers
    ###################################################################################################################
    if [ -n "$GALERA" ]
    then
        docker-compose -f .travis/galera-compose.yml up -d
        urlString='jdbc:mariadb://mariadb.example.com:3106/testj?user=bob&enablePacketDebug=true'
        cmd+=( -DdefaultGaleraUrl="jdbc:mariadb:failover://mariadb.example.com:3106,mariadb.example.com:3107,mariadb.example.com:3108/testj?user=bob&enablePacketDebug=true" )

    fi

    ###################################################################################################################
    # wait for docker initialisation
    ###################################################################################################################

    for i in {60..0}; do
        if echo 'SELECT 1' | "${mysql[@]}" &> /dev/null; then
            break
        fi
        echo 'data server still not active'
        sleep 1
    done

    docker-compose -f ${COMPOSE_FILE} logs

    if [ "$i" = 0 ]; then
        echo 'SELECT 1' | "${mysql[@]}"
        echo >&2 'data server init process failed.'
        exit 1
    fi
fi



###################################################################################################################
# run test suite
###################################################################################################################
echo "Running coveralls for JDK version: $TRAVIS_JDK_VERSION"
cmd+=( -DdbUrl="$urlString" )
cmd+=( -DtestSingleHost="$testSingleHost" )
echo ${cmd}

"${cmd[@]}"
if [ -n "$PROFILE" ]
then
    tail -5000 /tmp/debug.log
fi
