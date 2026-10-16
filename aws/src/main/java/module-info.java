// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc

/** AWS IAM authentication for MariaDB Connector/J: the AWS-IAM credential plugin. */
module org.mariadb.jdbc.aws {
  requires java.sql;
  requires org.mariadb.jdbc;
  requires software.amazon.awssdk.services.rds;
  requires software.amazon.awssdk.services.sts;
  requires software.amazon.awssdk.regions;
  requires software.amazon.awssdk.auth;

  exports org.mariadb.jdbc.plugin.credential.aws;

  provides org.mariadb.jdbc.plugin.CredentialPlugin with
      org.mariadb.jdbc.plugin.credential.aws.AwsIamCredentialPlugin;
}
