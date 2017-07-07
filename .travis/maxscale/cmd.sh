#!/usr/bin/env bash

#ensure that first database restart has been done database
sleep 5

#################################################################################################
# wait for db availability for 30s
#################################################################################################
mysql=( mysql --protocol=tcp -ubob -hdb --port=3306 )
for i in {30..0}; do
    if echo 'SELECT 1' | "${mysql[@]}" &> /dev/null; then
        break
    fi
    echo 'DB init process in progress...'
    sleep 1
done

if [ "$i" = 0 ]; then
    echo 'SELECT 1' | "${mysql[@]}"
    echo >&2 'DB init process failed.'
    exit 2
fi

/usr/bin/maxscale --nodaemon

cd /var/log/maxscale
ls -lrt
tail -500 /var/log/maxscale/maxscale1.log