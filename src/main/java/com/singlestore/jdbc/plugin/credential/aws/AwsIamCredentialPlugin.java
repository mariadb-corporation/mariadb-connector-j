// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential.aws;

import static java.time.LocalDateTime.now;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.plugin.Credential;
import com.singlestore.jdbc.plugin.CredentialPlugin;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Permit AWS database IAM authentication.
 *
 * <p>Token is generated using IAM credential and region.
 *
 * <p>Implementation use SDK DefaultAWSCredentialsProviderChain and DefaultAwsRegionProviderChain
 * (environment variable / system properties, files, ...) or using connection string options :
 * accessKeyId, secretKey, region.
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

  @Override
  public CredentialPlugin initialize(Configuration conf, String userName, HostAddress hostAddress)
      throws SQLException {
    try {
      Class.forName("software.amazon.awssdk.auth.credentials.AwsBasicCredentials");
    } catch (ClassNotFoundException ex) {
      throw new SQLException(
          "Identity plugin 'AWS-IAM' is used without having AWS SDK in "
              + "classpath. "
              + "Please add 'software.amazon.awssdk:rds' to classpath");
    }
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
