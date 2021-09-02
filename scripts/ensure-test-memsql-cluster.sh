#!/usr/bin/env bash
set -eu

# this script must be run from the top-level of the repo
cd "$(git rev-parse --show-toplevel)"

DEFAULT_IMAGE_NAME="memsql/cluster-in-a-box:centos-7.0.15-619d118712-1.9.5-1.5.0"
IMAGE_NAME="${MEMSQL_IMAGE:-$DEFAULT_IMAGE_NAME}"
CONTAINER_NAME="memsql-integration"

EXISTS=$(docker inspect ${CONTAINER_NAME} >/dev/null 2>&1 && echo 1 || echo 0)

if [[ "${EXISTS}" -eq 1 ]]; then
  EXISTING_IMAGE_NAME=$(docker inspect -f '{{.Config.Image}}' ${CONTAINER_NAME})
  if [[ "${IMAGE_NAME}" != "${EXISTING_IMAGE_NAME}" ]]; then
    echo "Existing container ${CONTAINER_NAME} has image ${EXISTING_IMAGE_NAME} when ${IMAGE_NAME} is expected; recreating container."
    docker rm -f ${CONTAINER_NAME}
    EXISTS=0
  fi
fi

if [[ "${EXISTS}" -eq 0 ]]; then
    docker run -i --init \
        --name ${CONTAINER_NAME} \
        -v ${PWD}/scripts/ssl:/test-ssl \
        -e LICENSE_KEY=${LICENSE_KEY} \
        -p 5506:3306 -p 5507:3307 -p 5508:3308 \
        ${IMAGE_NAME}
fi

docker start ${CONTAINER_NAME}

memsql-wait-start() {
  echo -n "Waiting for MemSQL to start..."
  while true; do
      if mysql -u root -h 127.0.0.1 -P 5506 -e "select 1" >/dev/null 2>/dev/null; then
          break
      fi
      echo -n "."
      sleep 0.2
  done
  echo ". Success!"
}

memsql-wait-start

if [[ "${EXISTS}" -eq 0 ]]; then
    echo
    echo "Creating aggregator node"
    docker exec -it ${CONTAINER_NAME} memsqlctl create-node --yes --no-start --port 3308
    docker exec -it ${CONTAINER_NAME} memsqlctl update-config --yes --all --key minimum_core_count --value 0
    docker exec -it ${CONTAINER_NAME} memsqlctl update-config --yes --all --key minimum_memory_mb --value 0
    docker exec -it ${CONTAINER_NAME} memsqlctl start-node --yes --all
    docker exec -it ${CONTAINER_NAME} memsqlctl add-aggregator --yes --host 127.0.0.1 --port 3308
fi

echo
echo "Setting up SSL"
docker exec -it ${CONTAINER_NAME} memsqlctl update-config --yes --all --key ssl_ca --value /test-ssl/test-ca-cert.pem
docker exec -it ${CONTAINER_NAME} memsqlctl update-config --yes --all --key ssl_cert --value /test-ssl/test-memsql-cert.pem
docker exec -it ${CONTAINER_NAME} memsqlctl update-config --yes --all --key ssl_key --value /test-ssl/test-memsql-key.pem
echo "Restarting cluster"
docker exec -it ${CONTAINER_NAME} memsqlctl restart-node --yes --all
memsql-wait-start
echo "Setting up root-ssl user"
mysql -u root -h 127.0.0.1 -P 5506 -e 'grant all privileges on *.* to "root-ssl"@"%" require ssl with grant option'
mysql -u root -h 127.0.0.1 -P 5507 -e 'grant all privileges on *.* to "root-ssl"@"%" require ssl with grant option'
mysql -u root -h 127.0.0.1 -P 5508 -e 'grant all privileges on *.* to "root-ssl"@"%" require ssl with grant option'
echo "Done!"

echo
echo "Ensuring child nodes are connected using container IP"
CONTAINER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${CONTAINER_NAME})
CURRENT_LEAF_IP=$(mysql -u root -h 127.0.0.1 -P 5506 --batch -N -e 'select host from information_schema.leaves')
if [[ ${CONTAINER_IP} != "${CURRENT_LEAF_IP}" ]]; then
    # remove leaf with current ip
    mysql -u root -h 127.0.0.1 -P 5506 --batch -N -e "remove leaf '${CURRENT_LEAF_IP}':3307"
    # add leaf with correct ip
    mysql -u root -h 127.0.0.1 -P 5506 --batch -N -e "add leaf root@'${CONTAINER_IP}':3307"
fi
CURRENT_AGG_IP=$(mysql -u root -h 127.0.0.1 -P 5506 --batch -N -e 'select host from information_schema.aggregators where master_aggregator=0')
if [[ ${CONTAINER_IP} != "${CURRENT_AGG_IP}" ]]; then
    # remove aggregator with current ip
    mysql -u root -h 127.0.0.1 -P 5506 --batch -N -e "remove aggregator '${CURRENT_AGG_IP}':3308"
    # add aggregator with correct ip
    mysql -u root -h 127.0.0.1 -P 5506 --batch -N -e "add aggregator root@'${CONTAINER_IP}':3308"
fi
echo "Done!"
