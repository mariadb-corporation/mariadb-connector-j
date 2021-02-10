#!/bin/bash

set -x
set -e

###################################################################################################################
# launch docker server
###################################################################################################################

if [ -n "$SKYSQL" ] || [ -n "$SKYSQL_HA" ]; then
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
else

  export PROJ_PATH=`pwd`
  export SSLCERT=$PROJ_PATH/tmp
  export ENTRYPOINT=$PROJ_PATH/.travis/sql
  export ENTRYPOINT_PAM=$PROJ_PATH/.travis/pam

  export COMPOSE_FILE=.travis/docker-compose.yml

  export TEST_DB_HOST=mariadb.example.com
  export TEST_DB_PORT=3305
  export TEST_DB_DATABASE=testj
  export TEST_DB_USER=bob
  export TEST_DB_OTHER=

  if [ -n "$MAXSCALE_VERSION" ] ; then
      # maxscale ports:
      # - non ssl: 4006
      # - ssl: 4009
      export TEST_DB_PORT=4006
      export TEST_DB_SSL_PORT=4009
      export COMPOSE_FILE=.travis/maxscale-compose.yml
      docker-compose -f ${COMPOSE_FILE} build
  fi

  mysql=( mysql --protocol=TCP -u${TEST_DB_USER} -h${TEST_DB_HOST} --port=${TEST_DB_PORT} ${TEST_DB_DATABASE})


  ###################################################################################################################
  # launch docker server and maxscale
  ###################################################################################################################
  docker-compose -f ${COMPOSE_FILE} up -d

  ###################################################################################################################
  # wait for docker initialisation
  ###################################################################################################################

  for i in {15..0}; do
    if echo 'SELECT 1' | "${mysql[@]}" &> /dev/null; then
        break
    fi
    echo 'data server still not active'
    sleep 5
  done

  if [ "$i" = 0 ]; then
    if echo 'SELECT 1' | "${mysql[@]}" ; then
        break
    fi

    docker-compose -f ${COMPOSE_FILE} logs
    if [ -n "$MAXSCALE_VERSION" ] ; then
        docker-compose -f ${COMPOSE_FILE} exec maxscale tail -n 500 /var/log/maxscale/maxscale.log
    fi
    echo >&2 'data server init process failed.'
    exit 1
  fi

  if [[ "$DB" != mysql* ]] ; then
    ###################################################################################################################
    # execute pam
    ###################################################################################################################
    docker-compose -f ${COMPOSE_FILE} exec -u root db bash /pam/pam.sh
    sleep 1
    docker-compose -f ${COMPOSE_FILE} restart db
    sleep 5

    ###################################################################################################################
    # wait for restart
    ###################################################################################################################

    for i in {30..0}; do
      if echo 'SELECT 1' | "${mysql[@]}" &> /dev/null; then
          break
      fi
      echo 'data server restart still not active'
      sleep 2
    done

    if [ "$i" = 0 ]; then
      if echo 'SELECT 1' | "${mysql[@]}" ; then
          break
      fi

      docker-compose -f ${COMPOSE_FILE} logs
      if [ -n "$MAXSCALE_VERSION" ] ; then
          docker-compose -f ${COMPOSE_FILE} exec maxscale tail -n 500 /var/log/maxscale/maxscale.log
      fi
      echo >&2 'data server restart process failed.'
      exit 1
    fi
  fi
fi


if [ -n "$BENCH" ]; then
  ###################################################################################################################
  # run bench
  ###################################################################################################################
  mvn clean package -P bench -Dmaven.test.skip
  java -Duser.country=US -Duser.language=en -DTEST_PORT=$TEST_DB_PORT -DTEST_HOST=$TEST_DB_HOST -DTEST_USERNAME=$TEST_DB_USER -jar target/benchmarks.jar

else
  ###################################################################################################################
  # run test suite
  ###################################################################################################################

  echo "Running tests for JDK version: $TRAVIS_JDK_VERSION"
  mvn clean test $ADDITIONNAL_VARIABLES -DjobId=${TRAVIS_JOB_ID}

fi

