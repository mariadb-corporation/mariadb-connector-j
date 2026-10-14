# MariaDB Connector/J AWS IAM credential plugin

AWS IAM authentication for MariaDB Connector/J (`credentialType=AWS-IAM`): the connector
authenticates to an RDS MariaDB/MySQL instance with an IAM authentication token in place
of a password.

The plugin is a separate artifact, so that the connector itself carries no AWS SDK dependency. Add
it next to the connector; it brings the AWS SDK modules it needs:

```xml
<dependency>
    <groupId>org.mariadb.jdbc</groupId>
    <artifactId>mariadb-java-client-aws</artifactId>
    <version>${mariadb-java-client-aws.version}</version>
</dependency>
```

## Usage

```
jdbc:mariadb://mydb.123456789012.eu-west-1.rds.amazonaws.com/db?user=iam_user&credentialType=AWS-IAM
```

The token is signed with the AWS credentials and region resolved by the SDK default chains
(environment variables, system properties, profile files, instance metadata). Connection string
options can override them:

| option            | role                                                                  |
|-------------------|-----------------------------------------------------------------------|
| `accessKeyId`, `secretKey` | static credentials in place of the default credential chain  |
| `region`          | AWS region in place of the default region chain                       |
| `roleArn`         | IAM role to assume through STS; the token is then signed with the role's temporary credentials |
| `roleSessionName` | session name of the assumed role, default `mariadb-connector-j`       |
| `externalId`      | external id of the assumed role, when the role requires one           |

## TLS

TLS is mandatory with IAM authentication: the plugin refuses `sslMode=disable`.

The RDS root certificate authorities (the
[global bundle](https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem)) are shipped with
the plugin, so the connector's default `sslMode=verify-full` validates the RDS certificate chain and
hostname without any option. The system trust store stays trusted next to them, so RDS Proxy
endpoints, which use publicly issued certificates, work as well. The `serverSslCert` or `trustStore`
options override the bundle, for example for the AWS China or GovCloud regions, which publish their
own bundles.

## Build

The plugin is built against a released connector, independently of the connector's own build:

```
mvn -f aws/pom.xml verify
```

The connector version it builds against is the `mariadb.version` property of `aws/pom.xml`. For a
snapshot of the connector, install it first (`mvn install -DskipTests` at the repository root).

## Testing

Offline tests run with the build. The tests against a real RDS instance, and the script that
creates a throwaway test environment, are described in [TESTING.md](TESTING.md).

## RDS certificate bundle

The bundled RDS root certificates come from the
[global bundle](https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem). AWS adds
authorities when it opens a region; the existing ones are valid until 2061 or later. CI warns when
the bundled copy differs from the published one. To refresh it:

```
aws/scripts/rds-ca-bundle.sh          # replaces the resource when the published bundle changed
aws/scripts/rds-ca-bundle.sh check    # only reports (exit 1 when out of date)
```

The script checks that the download holds only Amazon RDS authorities and lists the added or
removed ones.
