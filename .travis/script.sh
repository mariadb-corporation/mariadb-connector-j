#!/bin/bash

set -x
set -e

###################################################################################################################
# test different type of configuration
###################################################################################################################
case "$TYPE" in
 "REWRITE" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3305/testj?user=bob&rewriteBatchedStatements=true&enablePacketDebug=true'
   ;;
 "PREPARE" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3305/testj?user=bob&useServerPrepStmts=true&enablePacketDebug=true'
   ;;
 "MULTI" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3305/testj?user=bob&allowMultiQueries=true&enablePacketDebug=true'
   ;;
 "BULK_SERVER" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3305/testj?user=bob&useBatchMultiSend=true&useServerPrepStmts=true&enablePacketDebug=true'
   ;;
 "NO_BULK_CLIENT" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3305/testj?user=bob&useBatchMultiSend=false&enablePacketDebug=true'
   ;;
 "NO_BULK_SERVER" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3305/testj?user=bob&useBatchMultiSend=false&useServerPrepStmts=true&enablePacketDebug=true'
   ;;
 "COMPRESSION" )
   urlString=-DdbUrl='jdbc:mariadb://localhost:3305/testj?user=bob&useCompression=true&enablePacketDebug=true'
   ;;
  *)
   urlString=-DdbUrl='jdbc:mariadb://localhost:3306/testj?user=root'
   if [ -n "$MAXSCALE_VERSION" ]
   then
       urlString=-DdbUrl='jdbc:mariadb://localhost:4007/testj?user=bob&killFetchStmtOnClose=false&enablePacketDebug=true'
   else
       urlString=-DdbUrl='jdbc:mariadb://localhost:3305/testj?user=bob&enablePacketDebug=true'
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

    ###################################################################################################################
    # launch docker server and maxscale
    ###################################################################################################################
    export INNODB_LOG_FILE_SIZE=$(echo $PACKET| cut -d'M' -f 1)0M
    docker-compose -f .travis/docker-compose.yml build
    docker-compose -f .travis/docker-compose.yml up -d

    ###################################################################################################################
    # for travis, wait for docker initialisation
    ###################################################################################################################
    if [ -n "$TRAVIS" ]
    then
        mysql=( mysql --protocol=tcp -ubob -h127.0.0.1 --port=4007 )

        for i in {30..0}; do
            if echo 'SELECT 1' | "${mysql[@]}" &> /dev/null; then
                break
            fi
            echo 'maxscale still not active'
            sleep 1
        done

        docker-compose -f .travis/docker-compose.yml logs

        if [ "$i" = 0 ]; then
            echo 'SELECT 1' | "${mysql[@]}"
            echo >&2 'Maxscale init process failed.'
            exit 1
        fi
    fi
fi



###################################################################################################################
# run test suite
###################################################################################################################
echo "Running coveralls for JDK version: $TRAVIS_JDK_VERSION"
mvn clean test $urlString -DtestSingleHost=$testSingleHost $ADDITIONNAL_VARIABLES -DjobId=$TRAVIS_JOB_ID  \
    -DkeystorePath="$PROJ_PATH/tmp/client-keystore.jks" \
    -DkeystorePassword="kspass"  \
    -DserverCertificatePath="$PROJ_PATH/tmp/server.crt" \
    -Dkeystore2Path="$PROJ_PATH/tmp/fullclient-keystore.jks" \
    -Dkeystore2Password="kspass" -DkeyPassword="kspasskey"  \
    -Dkeystore2PathP12="$PROJ_PATH/tmp/fullclient-keystore.p12"

if [ -n "$PROFILE" ]
then
    tail -5000 /tmp/debug.log
fi
