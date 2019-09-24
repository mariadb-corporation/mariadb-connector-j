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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.rds.auth.GetIamAuthTokenRequest;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;
import java.util.Properties;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.credential.Credential;

public class AwsCredentialGenerator {

  private RdsIamAuthTokenGenerator generator;
  private GetIamAuthTokenRequest request;
  private String userName;

  /**
   * AWS Generator.
   *
   * @param nonMappedOptions non standard options
   * @param userName user
   * @param hostAddress current server information
   */
  public AwsCredentialGenerator(Properties nonMappedOptions, String userName,
                                HostAddress hostAddress) {
    // Build RDS IAM-auth token generator
    this.userName = userName;
    AWSCredentialsProvider awsCredentialsProvider;
    String accessKeyId = nonMappedOptions.getProperty("accessKeyId");
    String secretKey = nonMappedOptions.getProperty("secretKey");
    String region = nonMappedOptions.getProperty("region");

    if (accessKeyId != null && secretKey != null) {
      awsCredentialsProvider =
          new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretKey));
    } else {
      awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
    }

    this.generator =
        RdsIamAuthTokenGenerator.builder()
            .credentials(awsCredentialsProvider)
            .region(region != null ? region : new DefaultAwsRegionProviderChain().getRegion())
            .build();
    this.request =
        GetIamAuthTokenRequest.builder()
            .hostname(hostAddress.host)
            .port(hostAddress.port)
            .userName(userName)
            .build();

  }

  public Credential getToken() {
    return new Credential(userName, generator.getAuthToken(this.request));
  }
}
