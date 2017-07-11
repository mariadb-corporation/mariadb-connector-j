#!/usr/bin/env bash

#ensure that first database restart has been done database
sleep 5

#################################################################################################
# wait for db availability for 30s
#################################################################################################
mysql=( mysql --protocol=tcp -ubob -hdb --port=3306 )
for j in {1..0}; do
    for i in {10..0}; do
        if echo 'use test2' | "${mysql[@]}" &> /dev/null; then
            break
        fi
        echo 'DB init process in progress...'
        sleep 3
    done

    echo 'use test2' | "${mysql[@]}"
    if [ "$i" = 0 ]; then
        echo 'DB init process failed.'
        exit 1
    fi
done

/usr/bin/maxscale --nodaemon

cd /var/log/maxscale
ls -lrt
tail -500 /var/log/maxscale/maxscale1.log