/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.credential.aws;

import org.mariadb.jdbc.*;
import org.mariadb.jdbc.credential.*;
import org.mariadb.jdbc.util.*;

import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

import static java.time.LocalDateTime.*;

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
 *     href="https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html">DefaultAWSCredentialsProviderChain</a>
 * @see <a
 *     href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/regions/providers/DefaultAwsRegionProviderChain.html">DefaultAwsRegionProviderChain</a>
 */
public class AwsIamCredentialPlugin implements CredentialPlugin {

  private static final int TOKEN_TTL = 10;

  private static Map<KeyCache, IdentityExpire> cache = new ConcurrentHashMap<>();

  private AwsCredentialGenerator generator;
  private KeyCache key;

  @Override
  public String type() {
    return "AWS-IAM";
  }

  @Override
  public String name() {
    return "AWS IAM Authentication plugin";
  }

  @Override
  public boolean mustUseSsl() {
    return true;
  }

  @Override
  public CredentialPlugin initialize(Options options, String userName, HostAddress hostAddress)
      throws SQLException {
    try {
      Class.forName("com.amazonaws.auth.BasicAWSCredentials");
    } catch (ClassNotFoundException ex) {
      throw new SQLException(
          "Identity plugin 'AWS-IAM' is used without having AWS SDK in "
              + "classpath. "
              + "Please add 'com.amazonaws:aws-java-sdk-rds' to classpath");
    }
    this.generator = new AwsCredentialGenerator(options.nonMappedOptions, userName, hostAddress);
    this.key = new KeyCache(options, userName, hostAddress);
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

  private class IdentityExpire {

    private LocalDateTime expiration;
    private Credential credential;

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

  private class KeyCache {
    private Options options;
    private String userName;
    private HostAddress hostAddress;

    public KeyCache(Options options, String userName, HostAddress hostAddress) {
      this.options = options;
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
      return options.equals(keyCache.options)
          && Objects.equals(userName, keyCache.userName)
          && hostAddress.equals(keyCache.hostAddress);
    }

    @Override
    public int hashCode() {
      return Objects.hash(options, userName, hostAddress);
    }
  }
}
