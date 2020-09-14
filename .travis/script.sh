#!/bin/bash

set -x
set -e

###################################################################################################################
# test different type of configuration
###################################################################################################################
if [ -n "$SKYSQL" ] ; then

  if [ -z "$SKYSQL_HOST" ] ; then
    echo "No SkySQL configuration found !"
    exit 0
  else
    testSingleHost=true
    urlString="jdbc:mariadb://$SKYSQL_HOST:$SKYSQL_PORT/testj?user=$SKYSQL_USER&password=$SKYSQL_PASSWORD&enablePacketDebug=true&useSsl&serverSslCert=$SKYSQL_SSL_CA"

    cmd=( mvn clean test $ADDITIONNAL_VARIABLES -DjobId=${TRAVIS_JOB_ID} )
  fi

else
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
     urlString='jdbc:mariadb://mariadb.example.com:3305/testj?user=bob&useBatchMultiSend=true&useServerPrepStmts=true&enablePacketDebug=true&useBulkStmts=true'
     ;;
   "NO_BULK_CLIENT" )
     urlString='jdbc:mariadb://mariadb.example.com:3305/testj?user=bob&useBatchMultiSend=true&enablePacketDebug=true'
     ;;
   "NO_BULK_SERVER" )
     urlString='jdbc:mariadb://mariadb.example.com:3305/testj?user=bob&useBatchMultiSend=false&useServerPrepStmts=true&enablePacketDebug=true'
     ;;
   "COMPRESSION" )
     urlString='jdbc:mariadb://mariadb.example.com:3305/testj?user=bob&useCompression=true&enablePacketDebug=true'
     ;;
    *)
     urlString='jdbc:mariadb://mariadb.example.com:3305/testj?user=bob&enablePacketDebug=true'
     ;;
  esac;



  if [ -n "$PROFILE" ] ; then
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
      -DrunLongTest=true \
      -DserverPublicKey="$SSLCERT/public.key"\
      -DsslPort="$SSLPORT")

  if [ -n "$AURORA" ] ; then
      if [ -n "$AURORA_STRING_URL" ] ; then
          urlString=${AURORA_STRING_URL}
          testSingleHost=true
      else
          testSingleHost=false
      fi
  else

      testSingleHost=true

      export INNODB_LOG_FILE_SIZE=$(echo ${PACKET}| cut -d'M' -f 1)0M

      if [ -n "$MAXSCALE_VERSION" ] ; then
          ###################################################################################################################
          # launch Maxscale with one server
          ###################################################################################################################
          mysql=( mysql --protocol=TCP -ubob -h127.0.0.1 --port=4006 test2)
          export COMPOSE_FILE=.travis/maxscale-compose.yml
          urlString='jdbc:mariadb://mariadb.example.com:4006/testj?user=bob&enablePacketDebug=true'
          docker-compose -f ${COMPOSE_FILE} build
          docker-compose -f ${COMPOSE_FILE} up -d
      else
          if [ -n "$GALERA" ] || [ -n "$GALERA3" ]  ;  then
              if [ -n "$GALERA3" ] ; then
                  ###################################################################################################################
                  # launch 3 galera servers
                  ###################################################################################################################
                  mysql=( mysql --protocol=TCP -ubob -hmariadb.example.com --port=3106 test2)
                  export COMPOSE_FILE=.travis/galera-compose.yml

                  urlString='jdbc:mariadb://mariadb.example.com:3106/testj?user=bob&enablePacketDebug=true'
                  cmd+=( -DdefaultGaleraUrl="jdbc:mariadb:sequential://mariadb.example.com:3106,mariadb.example.com:3107,mariadb.example.com:3108/testj?user=bob&enablePacketDebug=true" -DdefaultSequentialUrl="jdbc:mariadb:sequential://mariadb.example.com:3106,mariadb.example.com:3107,mariadb.example.com:3108/testj?user=bob&enablePacketDebug=true" -DdefaultLoadbalanceUrl="jdbc:mariadb:loadbalance://mariadb.example.com:3106,mariadb.example.com:3107,mariadb.example.com:3108/testj?user=bob&enablePacketDebug=true" )
                  docker-compose -f ${COMPOSE_FILE} up -d
                  SLEEP 10
              else
                  mysql=( mysql --protocol=tcp -ubob -hmariadb.example.com --port=3106 test2)

                  urlString='jdbc:mariadb://mariadb.example.com:3106/testj?user=bob&enablePacketDebug=true'
                  docker run \
                          -v $SSLCERT:/etc/sslcert \
                          -v $ENTRYPOINT:/docker-entrypoint-initdb.d \
                          -e MYSQL_INITDB_SKIP_TZINFO=yes \
                          -e MYSQL_ALLOW_EMPTY_PASSWORD=1 \
                          -e MYSQL_DATABASE=testj \
                          -d \
                          -p 3106:3306 \
                          -p 4067:4567 \
                          -p 4068:4568 \
                          --name=node1 \
                          mariadb:10.2 --wsrep-new-cluster --wsrep-cluster-address='gcomm://node1' \
                          --wsrep-on=ON \
                          --max-connections=500 \
                          --wsrep-node-address=node1:4567 \
                          --wsrep-node-name=node1 \
                          --character-set-server=utf8mb4 \
                          --collation-server=utf8mb4_unicode_ci \
                          --bind-address=0.0.0.0 \
                          --binlog-format=ROW \
                          --wsrep-provider=/usr/lib/galera/libgalera_smm.so \
                          --wsrep-cluster-name=my_super_cluster \
                          --ssl-ca=/etc/sslcert/ca.crt \
                          --ssl-cert=/etc/sslcert/server.crt --ssl-key=/etc/sslcert/server.key

              fi
          else

              ###################################################################################################################
              # launch docker server
              ###################################################################################################################
              mysql=( mysql --protocol=TCP -ubob -hmariadb.example.com --port=3305 test2)
              export COMPOSE_FILE=.travis/docker-compose.yml
              docker-compose -f ${COMPOSE_FILE} up -d

          fi
      fi


      ###################################################################################################################
      # wait for docker initialisation
      ###################################################################################################################

      for i in {15..0}; do
          if echo 'SELECT 1' | "${mysql[@]}" ; then
              break
          fi
          echo 'data server still not active'
          sleep 2
      done


      if [ "$i" = 0 ]; then
          if [ -n "COMPOSE_FILE" ] ; then
              docker-compose -f ${COMPOSE_FILE} logs
          fi

          echo 'SELECT 1' | "${mysql[@]}"
          echo >&2 'data server init process failed.'
          exit 1
      fi
  fi


fi


###################################################################################################################
# run test suite
###################################################################################################################
echo "Running coveralls for JDK version: $TRAVIS_JDK_VERSION"
cmd+=( -DdbUrl="$urlString" )
cmd+=( -DtestSingleHost="$testSingleHost" )
echo ${cmd}

if [ -n "$MAXSCALE_VERSION" ] ; then
    docker-compose -f $COMPOSE_FILE exec maxscale tail -n 500 /var/log/maxscale/maxscale.log
fi

"${cmd[@]}"