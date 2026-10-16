// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin.credential.aws;

import static java.time.LocalDateTime.now;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.plugin.Credential;
import org.mariadb.jdbc.plugin.CredentialPlugin;

/**
 * Permit AWS database IAM authentication.
 *
 * <p>Token is generated using IAM credential and region.
 *
 * <p>Implementation use SDK DefaultAWSCredentialsProviderChain and DefaultAwsRegionProviderChain
 * (environment variable / system properties, files, ...) or using connection string options :
 * accessKeyId, secretKey, region.
 *
 * <p>TLS is mandatory. The RDS root certificate authorities are bundled, so that the default
 * sslMode=verify-full works without configuration; serverSslCert or trustStore override them.
 *
 * <p>With the connection string option roleArn, the token is signed with the temporary credentials
 * of that IAM role, assumed through STS with the credentials above; roleSessionName (default
 * mariadb-connector-j) and externalId are optional.
 *
 * @see <a
 *     href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html">DefaultCredentialsProvider</a>
 * @see <a
 *     href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/regions/providers/DefaultAwsRegionProviderChain.html">DefaultAwsRegionProviderChain</a>
 */
public class AwsIamCredentialPlugin implements CredentialPlugin {

  private static final int TOKEN_TTL = 10;

  private static final Map<KeyCache, IdentityExpire> cache = new ConcurrentHashMap<>();

  private AwsCredentialGenerator generator;
  private KeyCache key;

  @Override
  public String type() {
    return "AWS-IAM";
  }

  @Override
  public boolean mustUseSsl() {
    return true;
  }

  /**
   * The RDS root certificate authorities (global bundle) shipped with the plugin, so that the
   * default sslMode=verify-full works without configuration. serverSslCert and trustStore override
   * them; the system trust store stays trusted next to them (RDS Proxy endpoints use publicly
   * issued certificates).
   */
  @Override
  public String defaultServerSslCert() {
    return AwsIamCredentialPlugin.class.getResource("rds-global-bundle.pem").toExternalForm();
  }

  @Override
  public CredentialPlugin initialize(Configuration conf, String userName, HostAddress hostAddress)
      throws SQLException {
    this.generator = new AwsCredentialGenerator(conf.nonMappedOptions(), conf.user(), hostAddress);
    this.key = new KeyCache(conf, conf.user(), hostAddress);
    return this;
  }

  @Override
  public Credential get() {
    IdentityExpire val = cache.get(key);
    if (val != null && val.isValid()) {
      return val.getCredential();
    }

    Credential credential = generator.getToken();
    cache.put(key, new IdentityExpire(credential));
    return credential;
  }

  private static class IdentityExpire {

    private final LocalDateTime expiration;
    private final Credential credential;

    public IdentityExpire(Credential credential) {
      this.credential = credential;
      expiration = now().plusMinutes(TOKEN_TTL);
    }

    public boolean isValid() {
      return expiration.isAfter(now());
    }

    public Credential getCredential() {
      return credential;
    }
  }

  private static class KeyCache {
    private final Configuration conf;
    private final String userName;
    private final HostAddress hostAddress;

    public KeyCache(Configuration conf, String userName, HostAddress hostAddress) {
      this.conf = conf;
      this.userName = userName;
      this.hostAddress = hostAddress;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      KeyCache keyCache = (KeyCache) o;
      return conf.equals(keyCache.conf)
          && Objects.equals(userName, keyCache.userName)
          && hostAddress.equals(keyCache.hostAddress);
    }

    @Override
    public int hashCode() {
      return Objects.hash(conf, userName, hostAddress);
    }
  }
}
