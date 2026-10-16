// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin.credential.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.net.URI;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.export.SslMode;
import org.mariadb.jdbc.plugin.Credential;
import org.mariadb.jdbc.plugin.CredentialPlugin;

/**
 * Offline tests: the token is a presigned URL computed locally from static credentials, so no AWS
 * account is involved. {@link AwsIamRdsTest} covers the connection to a real instance.
 */
public class AwsIamCredentialPluginTest {

  private static final String RDS_HOST = "mydb.123456789012.eu-west-1.rds.amazonaws.com";

  /**
   * Connection string with static credentials, so that the SDK default chains are not consulted.
   */
  private static final String URL =
      "jdbc:mariadb://"
          + RDS_HOST
          + "/db?user=iam_user&credentialType=AWS-IAM&region=eu-west-1"
          + "&accessKeyId=AKIAIOSFODNN7EXAMPLE&secretKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

  private static Credential token(String url) throws Exception {
    Configuration conf = Configuration.parse(url);
    return new AwsIamCredentialPlugin()
        .initialize(conf, conf.user(), conf.addresses().get(0))
        .get();
  }

  @Test
  public void registeredAsAwsIam() {
    CredentialPlugin plugin =
        ServiceLoader.load(CredentialPlugin.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(p -> "AWS-IAM".equals(p.type()))
            .findFirst()
            .orElse(null);
    assertNotNull(plugin, "AWS-IAM plugin not registered");
    assertEquals(AwsIamCredentialPlugin.class, plugin.getClass());
    assertTrue(plugin.mustUseSsl(), "IAM tokens must travel over TLS");
  }

  @Test
  public void bundledRdsCertificateAuthorities() throws Exception {
    // the connector opens defaultServerSslCert as a URL: the RDS global bundle must be readable
    // from
    // the jar and hold the root certificate authorities of every region
    String url = new AwsIamCredentialPlugin().defaultServerSslCert();
    assertNotNull(url, "RDS certificate bundle not found in the plugin resources");
    try (InputStream in = new URI(url).toURL().openStream()) {
      Collection<? extends java.security.cert.Certificate> cas =
          CertificateFactory.getInstance("X.509").generateCertificates(in);
      assertTrue(cas.size() > 100, "RDS global bundle holds " + cas.size() + " certificates");
      for (java.security.cert.Certificate ca : cas) {
        X509Certificate x509 = (X509Certificate) ca;
        assertTrue(
            x509.getSubjectX500Principal().getName().contains("Amazon RDS"),
            "unexpected authority " + x509.getSubjectX500Principal());
      }
    }
  }

  @Test
  public void tokenIsAPresignedConnectRequest() throws Exception {
    Credential credential = token(URL);
    assertEquals("iam_user", credential.user());
    String token = credential.password();
    assertTrue(
        token.startsWith(RDS_HOST + ":3306/?")
            && token.contains("DBUser=iam_user&")
            && token.contains("Action=connect&"),
        "token bound to the host, port and user: " + token);
    assertTrue(token.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"), token);
    assertTrue(
        token.contains("X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F")
            && token.contains("%2Feu-west-1%2Frds-db%2Faws4_request"),
        "signed with the static key for the rds-db service of the region: " + token);
    assertTrue(token.contains("X-Amz-Expires=900"), "RDS tokens are valid 15 minutes: " + token);
    assertTrue(token.contains("X-Amz-Signature="), token);
    assertTrue(!token.contains("EXAMPLEKEY"), "the secret key must not appear in the token");
  }

  @Test
  public void tokenIsCachedPerHostAndUser() throws Exception {
    // the same host and user reuse the token (10 minute cache) across plugin instances
    assertEquals(token(URL).password(), token(URL).password());
    // another host, or user, gets its own token
    String other = token(URL.replace(RDS_HOST, "other." + RDS_HOST)).password();
    assertTrue(
        other.startsWith("other." + RDS_HOST + ":3306/?") && other.contains("DBUser=iam_user&"));
    assertTrue(token(URL.replace("user=iam_user", "user=app")).password().contains("DBUser=app&"));
  }

  @Test
  public void tlsCannotBeDisabled() throws Exception {
    // the token is a bearer credential: sslMode=disable is overridden by the connector
    assertEquals(SslMode.VERIFY_FULL, Configuration.parse(URL + "&sslMode=disable").sslMode());
    assertEquals(SslMode.VERIFY_FULL, Configuration.parse(URL).sslMode());
    assertEquals(SslMode.TRUST, Configuration.parse(URL + "&sslMode=trust").sslMode());
  }
}
