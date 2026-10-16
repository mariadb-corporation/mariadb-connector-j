// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin.credential.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;

/**
 * IAM authentication against a real RDS instance. Runs when the environment variable
 * AWS_IAM_TEST_URL holds a connection string to an RDS MariaDB/MySQL instance with IAM
 * authentication enabled and a database account using the AWS authentication plugin, for example
 * {@code jdbc:mariadb://mydb.123456789012.eu-west-1.rds.amazonaws.com/test?user=iam_user
 * &credentialType=AWS-IAM&region=eu-west-1}. The AWS credentials come from the SDK default chain
 * (environment, profile, ...) unless the URL carries accessKeyId and secretKey. The role test runs
 * when AWS_IAM_TEST_ROLE_ARN additionally names an IAM role the caller may assume, which holds the
 * rds-db:connect permission.
 */
public class AwsIamRdsTest {

  private static final String URL = System.getenv("AWS_IAM_TEST_URL");
  private static final String ROLE_ARN = System.getenv("AWS_IAM_TEST_ROLE_ARN");

  /** A certificate authority unrelated to RDS: a chain it cannot validate must be refused. */
  private static final String UNRELATED_CA =
      "-----BEGIN CERTIFICATE-----\n"
          + "MIIDGzCCAgOgAwIBAgIUA5kA3NGz4fKZ0cooy6e0SLSIMuEwDQYJKoZIhvcNAQEL\n"
          + "BQAwHDEaMBgGA1UEAwwRdW5yZWxhdGVkLXRlc3QtY2EwIBcNMjYxMDE0MTM1MDI1\n"
          + "WhgPMjEyNjA5MjAxMzUwMjVaMBwxGjAYBgNVBAMMEXVucmVsYXRlZC10ZXN0LWNh\n"
          + "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxmxORI/4TzHYevkwgVsr\n"
          + "lN+1zMUiwygLw9s5epIBfWXxOMdbZu7KI/RWQqxKMbAAEY3MVWhunORL7PyIBgBa\n"
          + "KfDS49DpZ9LJ/rXO2ANMSsU5dGAYoIh4NsoSbimd469kuBIp+q52i8ARVRAe/hjA\n"
          + "dDV/MA49sN2sVYKD6kfZJvdtDqzcI+Y4YUhzV4Bw+t9/bYDAt13/zp1RdmSDJjBH\n"
          + "jFUWy3G9OTgmuQrTgIN/+sdpbSmqwTBJ+e4P4UkFXPtRcdAQSiWqZ7tXKe+bLMhV\n"
          + "AHo4K2s4STnCI/ZcanLMt2crqgpHirTiRX1u5B1aJBY64tr7JHKT/aKJSfY/r3Wj\n"
          + "CQIDAQABo1MwUTAdBgNVHQ4EFgQUbeSpHc7jXMHpjQLYlp3l+WTJE5cwHwYDVR0j\n"
          + "BBgwFoAUbeSpHc7jXMHpjQLYlp3l+WTJE5cwDwYDVR0TAQH/BAUwAwEB/zANBgkq\n"
          + "hkiG9w0BAQsFAAOCAQEAcs1sLZ4oufEmX4uAkdVC7xD0HlMb+S5WxRCrHqb/XMW2\n"
          + "Lp4C7CRU0y3SoiYknGe5ZbCCB9jSvYfNvcIHwVJMGPYOs5azUoY3y+ff6o8NJF1f\n"
          + "+dtj5wxZmdJkP+5WbaxIylz/pG3Ujy7cJCcTba8bb9sHRhKo3quy/PtF4N4l1KAP\n"
          + "o4IuGMgKLyNqxR8qgGho6s1DvkQboiiMFJ8Wx0s/Q1EZmrzTfuuvRRk1dd/R4CEU\n"
          + "kfuZzfuSsatjNyS40uls9e1TsQ7NU+3PwAM+y7g4abq0mxOQ6yW08sUDh+Tiz1pn\n"
          + "Vc8gUhYviomarWIqz+90IAiE0c8RVznbysgbfnp+fQ==\n"
          + "-----END CERTIFICATE-----\n";

  @BeforeAll
  public static void requiresInstance() {
    assumeTrue(URL != null && !URL.isEmpty(), "AWS_IAM_TEST_URL not set");
  }

  private static String query(Connection con, String sql) throws SQLException {
    try (Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      assertTrue(rs.next());
      return rs.getString(rs.getMetaData().getColumnCount());
    }
  }

  private static void assertAuthenticatedOverTls(String url) throws SQLException {
    String user = Configuration.parse(url).user();
    try (Connection con = DriverManager.getConnection(url)) {
      assertTrue(
          query(con, "select current_user()").startsWith(user + "@"),
          "authenticated as the IAM database user");
      assertFalse(query(con, "show status like 'Ssl_cipher'").isEmpty(), "connection encrypted");
    }
  }

  @Test
  public void connectsWithDefaultTls() throws SQLException {
    // the default sslMode=verify-full validates the RDS chain with the bundled root certificates
    assertAuthenticatedOverTls(URL);
  }

  @Test
  public void bundledRootsAloneValidateTheChain() throws SQLException {
    assertAuthenticatedOverTls(URL + "&fallbackToSystemTrustStore=false");
  }

  @Test
  public void explicitUnrelatedCertificateIsRefused() {
    // serverSslCert overrides the bundle, and the chain is validated against it
    SQLException e =
        assertThrows(
            SQLException.class,
            () -> DriverManager.getConnection(URL + "&serverSslCert=" + UNRELATED_CA).close());
    assertTrue(e.getMessage().contains("certif"), e.getMessage());
  }

  @Test
  public void sslDisableIsOverridden() throws SQLException {
    // the token is a bearer credential: the connection stays encrypted
    assertAuthenticatedOverTls(URL + "&sslMode=disable");
  }

  @Test
  public void tokenIsBoundToTheUser() throws SQLException {
    // a token signed for another database user does not authenticate it
    String user = Configuration.parse(URL).user();
    String url = URL.replace("user=" + user, "user=" + user + "_nobody");
    SQLException e =
        assertThrows(SQLException.class, () -> DriverManager.getConnection(url).close());
    assertEquals("28000", e.getSQLState(), e.getMessage());
  }

  @Test
  public void unknownRoleIsRefused() {
    // roleArn is honored: the token is signed with the role's credentials, so a role that cannot
    // be assumed fails before any connection, whatever the caller's own permissions
    String url = URL + "&roleArn=arn:aws:iam::123456789012:role/does-not-exist";
    Exception e = assertThrows(Exception.class, () -> DriverManager.getConnection(url).close());
    String message = e.toString() + (e.getCause() == null ? "" : e.getCause().toString());
    assertTrue(message.contains("AssumeRole") || message.contains("Sts"), message);
  }

  @Test
  public void assumesRoleThroughSts() throws SQLException {
    assumeTrue(ROLE_ARN != null && !ROLE_ARN.isEmpty(), "AWS_IAM_TEST_ROLE_ARN not set");
    assertAuthenticatedOverTls(URL + "&roleArn=" + ROLE_ARN + "&roleSessionName=connector-test");
  }
}
