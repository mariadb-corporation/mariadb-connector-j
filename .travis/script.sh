#!/bin/bash

set -x
set -e

###################################################################################################################
# launch docker server
###################################################################################################################
if [ -z "$SKYSQL" ] && [ -z "$SKYSQL_HA" ]; then

  if [ -n "$MAXSCALE_VERSION" ] ; then
    ###################################################################################################################
    # launch Maxscale with one server
    ###################################################################################################################
    mysql=( mysql --protocol=TCP -ubob -h127.0.0.1 --port=4006 test2)
    export COMPOSE_FILE=.travis/maxscale-compose.yml
    docker-compose -f ${COMPOSE_FILE} build
    docker-compose -f ${COMPOSE_FILE} up -d
  else
    mysql=(mysql --protocol=tcp -ubob -h127.0.0.1 --port=3305)
    export COMPOSE_FILE=.travis/docker-compose.yml
    docker-compose -f ${COMPOSE_FILE} up -d
  fi

  ###################################################################################################################
  # wait for docker initialisation
  ###################################################################################################################
  for i in {60..0}; do
    if echo 'SELECT 1' | "${mysql[@]}" &>/dev/null; then
      break
    fi
    echo 'server still not up'
    sleep 1
  done

  if [ "$i" = 0 ]; then
    if [ -n "COMPOSE_FILE" ]; then
      docker-compose -f ${COMPOSE_FILE} logs
      if [ -n "$MAXSCALE_VERSION" ] ; then
        docker-compose -f $COMPOSE_FILE exec maxscale tail -n 500 /var/log/maxscale/maxscale.log
      fi
    fi

    echo 'SELECT 1' | "${mysql[@]}"
    echo >&2 'data server init process failed.'
    exit 1
  fi

  if [ -n "$BENCH" ]; then
    ###################################################################################################################
    # run bench
    ###################################################################################################################
    mvn clean package -P bench -Dmaven.test.skip
    java -Duser.country=US -Duser.language=en -DTEST_PORT=3305 -DTEST_HOST=mariadb.example.com -DTEST_USERNAME=bob -jar target/benchmarks.jar

  else
    ###################################################################################################################
    # run test suite
    ###################################################################################################################
    export TEST_DB_HOST=mariadb.example.com
    export TEST_DB_HOST=mariadb.example.com
    export TEST_DB_PORT=3305
    export TEST_DB_DATABASE=testj
    export TEST_DB_USER=bob
    export TEST_DB_OTHER=

    if [ -n "COMPRESSION" ] ; then
      export TEST_DB_OTHER=$'&useCompression'
    fi


    if [ -n "$MAXSCALE_VERSION" ] ; then
      export TEST_DB_PORT=4006
    fi

    echo "Running tests for JDK version: $TRAVIS_JDK_VERSION"
    mvn clean test $ADDITIONNAL_VARIABLES -DjobId=${TRAVIS_JOB_ID}

  fi
else
  if [ -n "$SKYSQL" ]; then
    if [ -z "$SKYSQL_HOST" ] ; then
      echo "No SkySQL configuration found !"
      exit 0
    else
      export TEST_DB_USER=$SKYSQL_USER
      export TEST_DB_HOST=$SKYSQL_HOST
      export TEST_DB_PASSWORD=$SKYSQL_PASSWORD
      export TEST_DB_DATABASE=testj
      export TEST_DB_PORT=$SKYSQL_PORT
      export TEST_DB_OTHER=$'sslMode=verify-full&serverSslCert='$SKYSQL_SSL_CA
    fi
  else
    if [ -z "$SKYSQL_HA_HOST" ] ; then
      echo "No SkySQL HA configuration found !"
      exit 0
    else
      export TEST_DB_USER=$SKYSQL_HA_USER
      export TEST_DB_HOST=$SKYSQL_HA_HOST
      export TEST_DB_PASSWORD=$SKYSQL_HA_PASSWORD
      export TEST_DB_DATABASE=testj
      export TEST_DB_PORT=$SKYSQL_HA_PORT
      export TEST_DB_OTHER=$'sslMode=verify-full&serverSslCert='$SKYSQL_HA_SSL_CA
    fi
  fi
  if [ -z "$BENCH" ] ; then
    echo "Running test for JDK version: $TRAVIS_JDK_VERSION"
    mvn clean test $ADDITIONNAL_VARIABLES -DjobId=${TRAVIS_JOB_ID}
  else
    echo "Running benchmarks"
    mvn clean package -P bench -Dmaven.test.skip
    java -Duser.country=US -Duser.language=en -DTEST_PORT=$TEST_DB_PORT -DTEST_HOST=$TEST_DB_HOST -DTEST_USERNAME=bob -jar target/benchmarks.jar
  fi
fi
