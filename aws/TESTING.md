# Testing the AWS IAM credential plugin

There are two layers of tests: offline ones, run with every build, and ones against a real RDS
instance, run when configured.

## Offline tests

`AwsIamCredentialPluginTest`, always run: the plugin is found by the connector's plugin lookup,
the bundled certificate resource holds the RDS authorities, the token is a presigned connect
request bound to the host, port and user and signed for the `rds-db` service of the region with
the given key, the token is cached per host and user, and `sslMode=disable` is overridden. No AWS
account is involved: the token is computed locally from static credentials.

## Tests against an RDS instance

`AwsIamRdsTest`, skipped unless configured: connection with the default `sslMode=verify-full` and
the bundled roots, with the bundled roots alone, refusal of an unrelated certificate authority
given as `serverSslCert`, encryption kept with `sslMode=disable`, refusal of a token signed for
another database user, refusal of a role that cannot be assumed, and token signing with the
temporary credentials of a role assumed through STS (`roleArn`). They read two environment
variables:

| variable                | role                                                                                |
|-------------------------|-------------------------------------------------------------------------------------|
| `AWS_IAM_TEST_URL`      | connection string to an instance with IAM authentication enabled, e.g. `jdbc:mariadb://mydb.123456789012.eu-west-1.rds.amazonaws.com/test?user=iam_user&credentialType=AWS-IAM&region=eu-west-1` |
| `AWS_IAM_TEST_ROLE_ARN` | a role the caller may assume, holding the `rds-db:connect` permission; enables the STS test |

The AWS credentials come from the SDK default chain (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`,
`~/.aws/credentials`, ...) unless the URL carries `accessKeyId` and `secretKey`. The database
account must use the RDS authentication plugin:

```sql
CREATE USER 'iam_user' IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';
```

## Test environment in one command

[scripts/rds-test.sh](scripts/rds-test.sh) creates everything the RDS tests need on a throwaway
basis and deletes it afterwards. Prerequisites, once:

1. An AWS account, and in IAM a user (no console access) with the managed policies
   `AmazonRDSFullAccess`, `AmazonEC2FullAccess` and `IAMFullAccess`. The last two cover the
   firewall rule and the test role; on a production account, narrow them to
   `ec2:DescribeSecurityGroups`, `ec2:AuthorizeSecurityGroupIngress`, `ec2:RevokeSecurityGroupIngress`
   and `iam:PutUserPolicy`, `iam:DeleteUserPolicy`, `iam:CreateRole`, `iam:GetRole`,
   `iam:PutRolePolicy`, `iam:DeleteRolePolicy`, `iam:DeleteRole`.
2. An access key for that user (Security credentials, "Command Line Interface"), downloaded as
   `accessKeys.csv`.
3. Locally: the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html),
   the `mariadb` client and `curl`.

Then:

```
aws/scripts/rds-test.sh up ~/Downloads/accessKeys.csv   # about 10 minutes
aws/scripts/rds-test.sh test                            # mvn verify with the RDS tests enabled
aws/scripts/rds-test.sh down                            # deletes everything
```

`up` creates a `db.t4g.micro` instance (`RDS_ENGINE=mysql` for MySQL, MariaDB by default) with IAM
authentication, opens port 3306 to the current public IP, creates the `iam_user` account and the
`test` database, puts the `rds-db:connect` permission on the IAM user and on a role
`conj-iam-test-role` trusted by that user, and writes `aws/target/rds-test.env` with the access
key and the two variables above. `test` runs `mvn verify` with that file sourced; it can also be
sourced in a shell to run the tests from an IDE. `up` is idempotent: rerun it after a failure, it
resumes on the existing instance (resetting its admin password, which it does not keep). `down`
deletes the instance without snapshot, the role, the user policy and the firewall rule, so that
nothing is billed afterwards; the instance costs about two cents an hour while it exists, plus a
few dollars a month of storage if it is kept.

