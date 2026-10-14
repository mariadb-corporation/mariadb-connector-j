#!/usr/bin/env bash
# Throwaway AWS resources to run the AWS-IAM plugin tests (AwsIamRdsTest) against a real RDS
# instance: a db.t4g.micro instance with IAM authentication, a database account authenticated by
# IAM, the rds-db:connect permission on the caller, and an IAM role the caller may assume (STS test).
#
#   rds-test.sh up [accessKeys.csv]   create everything (about 10 minutes), write target/rds-test.env
#   rds-test.sh test                  run the plugin tests against it
#   rds-test.sh down                  delete everything, so that nothing is billed afterwards
#
# accessKeys.csv is the file downloaded from the IAM console when creating an access key; without
# it the AWS CLI configuration (~/.aws, environment) is used. The caller needs AmazonRDSFullAccess,
# ec2:DescribeSecurityGroups, ec2:AuthorizeSecurityGroupIngress, ec2:RevokeSecurityGroupIngress, and
# IAM rights on its own inline policies and on the test role (iam:PutUserPolicy, iam:DeleteUserPolicy,
# iam:CreateRole, iam:GetRole, iam:PutRolePolicy, iam:DeleteRolePolicy, iam:DeleteRole): on a test
# account, the IAMFullAccess policy. Needs the aws CLI, the mariadb client and curl.
set -euo pipefail

DIR=$(cd "$(dirname "$0")" && pwd)
ENV_FILE="$DIR/../target/rds-test.env"
ID=${RDS_ID:-conj-iam-test}
ROLE=${RDS_ROLE:-conj-iam-test-role}
REGION=${AWS_REGION:-eu-west-1}
ENGINE=${RDS_ENGINE:-mariadb}            # or mysql
IAM_DB_USER=iam_user
POLICY_NAME=conj-iam-test-connect

# the access key: from the csv when given, else from a previous run, else the CLI configuration
if [ "${1:-}" = "up" ] && [ -n "${2:-}" ]; then
  KEY_LINE=$(tail -n 1 "$2" | tr -d '\r')
  export AWS_ACCESS_KEY_ID=${KEY_LINE%%,*}
  export AWS_SECRET_ACCESS_KEY=${KEY_LINE#*,}
elif [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  . "$ENV_FILE"
fi
export AWS_DEFAULT_REGION=$REGION

case "${1:-}" in
  up)
    CALLER=$(aws sts get-caller-identity --query Arn --output text)
    ACCOUNT=${CALLER#arn:aws:iam::}; ACCOUNT=${ACCOUNT%%:*}
    USER_NAME=${CALLER##*/}
    echo "caller: $CALLER"

    ADMIN_PWD="Adm1n-$RANDOM$RANDOM"
    if aws rds describe-db-instances --db-instance-identifier "$ID" >/dev/null 2>&1; then
      # resuming on an existing instance: its admin password is unknown, reset it
      echo "$ID already exists, resetting its admin password..."
      aws rds wait db-instance-available --db-instance-identifier "$ID"
      aws rds modify-db-instance --db-instance-identifier "$ID" \
        --master-user-password "$ADMIN_PWD" --apply-immediately >/dev/null
      while [ "$(aws rds describe-db-instances --db-instance-identifier "$ID" \
          --query 'DBInstances[0].PendingModifiedValues.MasterUserPassword' --output text)" != "None" ]; do
        sleep 5
      done
      aws rds wait db-instance-available --db-instance-identifier "$ID"
    else
      aws rds create-db-instance \
        --db-instance-identifier "$ID" --db-instance-class db.t4g.micro \
        --engine "$ENGINE" --allocated-storage 20 --storage-type gp3 \
        --master-username admin --master-user-password "$ADMIN_PWD" \
        --enable-iam-database-authentication --publicly-accessible \
        --backup-retention-period 0 --no-multi-az --no-deletion-protection >/dev/null
      echo "creating $ID (about 10 minutes)..."
      aws rds wait db-instance-available --db-instance-identifier "$ID"
    fi

    read -r HOST SG RES_ID < <(aws rds describe-db-instances --db-instance-identifier "$ID" \
      --output text --query 'DBInstances[0].[Endpoint.Address,VpcSecurityGroups[0].VpcSecurityGroupId,DbiResourceId]')

    # open 3306 to this machine only
    MYIP=$(curl -s https://checkip.amazonaws.com)
    if aws ec2 describe-security-groups --group-ids "$SG" --output text \
        --query 'SecurityGroups[0].IpPermissions[?ToPort==`3306`].IpRanges[].CidrIp' | grep -q "$MYIP/32"; then
      echo "port 3306 already open to $MYIP"
    else
      aws ec2 authorize-security-group-ingress --group-id "$SG" \
        --protocol tcp --port 3306 --cidr "$MYIP/32" >/dev/null
    fi

    # database account authenticated by IAM instead of a password
    mariadb -h "$HOST" -u admin -p"$ADMIN_PWD" --ssl --connect-timeout=15 -e \
      "CREATE USER IF NOT EXISTS '$IAM_DB_USER' IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';
       CREATE DATABASE IF NOT EXISTS test; GRANT ALL ON test.* TO '$IAM_DB_USER';"

    # the connect permission, on the caller (direct tests) and on a role it may assume (STS test)
    CONNECT_POLICY="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"rds-db:connect\",\"Resource\":\"arn:aws:rds-db:$REGION:$ACCOUNT:dbuser:$RES_ID/$IAM_DB_USER\"}]}"
    aws iam put-user-policy --user-name "$USER_NAME" --policy-name "$POLICY_NAME" --policy-document "$CONNECT_POLICY"
    if ! ROLE_ARN=$(aws iam get-role --role-name "$ROLE" --query Role.Arn --output text 2>/dev/null); then
      ROLE_ARN=$(aws iam create-role --role-name "$ROLE" --query Role.Arn --output text \
        --assume-role-policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"$CALLER\"},\"Action\":\"sts:AssumeRole\"}]}")
    fi
    aws iam put-role-policy --role-name "$ROLE" --policy-name "$POLICY_NAME" --policy-document "$CONNECT_POLICY"
    echo -n "waiting for the role to become assumable"
    for i in $(seq 1 24); do
      aws sts assume-role --role-arn "$ROLE_ARN" --role-session-name probe >/dev/null 2>&1 && break
      echo -n "."; sleep 5
    done
    echo

    mkdir -p "$(dirname "$ENV_FILE")"
    umask 077
    cat > "$ENV_FILE" <<EOT
export AWS_ACCESS_KEY_ID='${AWS_ACCESS_KEY_ID:-}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET_ACCESS_KEY:-}'
export AWS_REGION='$REGION'
export AWS_IAM_TEST_URL='jdbc:mariadb://$HOST/test?user=$IAM_DB_USER&credentialType=AWS-IAM&region=$REGION'
export AWS_IAM_TEST_ROLE_ARN='$ROLE_ARN'
RDS_TEST_SG='$SG'
RDS_TEST_CIDR='$MYIP/32'
RDS_TEST_USER='$USER_NAME'
EOT
    echo
    echo "ready. Run the tests with:  $0 test"
    echo "or:  . $ENV_FILE && mvn -f $DIR/../pom.xml verify"
    echo "admin password: $ADMIN_PWD   (delete everything with: $0 down)"
    ;;
  test)
    [ -f "$ENV_FILE" ] || { echo "no $ENV_FILE: run '$0 up' first" >&2; exit 1; }
    mvn -f "$DIR/../pom.xml" verify
    ;;
  down)
    if aws rds describe-db-instances --db-instance-identifier "$ID" >/dev/null 2>&1; then
      aws rds delete-db-instance --db-instance-identifier "$ID" \
        --skip-final-snapshot --delete-automated-backups >/dev/null
      echo "deleting $ID..."
    fi
    aws iam delete-role-policy --role-name "$ROLE" --policy-name "$POLICY_NAME" 2>/dev/null || true
    aws iam delete-role --role-name "$ROLE" 2>/dev/null && echo "role $ROLE deleted" || true
    if [ -n "${RDS_TEST_USER:-}" ]; then
      aws iam delete-user-policy --user-name "$RDS_TEST_USER" --policy-name "$POLICY_NAME" 2>/dev/null || true
    fi
    if [ -n "${RDS_TEST_SG:-}" ]; then
      aws ec2 revoke-security-group-ingress --group-id "$RDS_TEST_SG" \
        --protocol tcp --port 3306 --cidr "$RDS_TEST_CIDR" >/dev/null 2>&1 || true
    fi
    aws rds wait db-instance-deleted --db-instance-identifier "$ID" 2>/dev/null || true
    rm -f "$ENV_FILE"
    echo "gone"
    ;;
  *)
    echo "usage: $0 up [accessKeys.csv] | test | down" >&2; exit 1 ;;
esac
