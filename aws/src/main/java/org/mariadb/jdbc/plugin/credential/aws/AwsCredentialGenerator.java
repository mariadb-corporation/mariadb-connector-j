// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin.credential.aws;

import java.util.Properties;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.plugin.Credential;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/** AWS credential generator */
public class AwsCredentialGenerator {

  private final String authenticationToken;
  private final String userName;

  /**
   * AWS Generator.
   *
   * @param nonMappedOptions non-standard options
   * @param userName user
   * @param hostAddress current server information
   */
  public AwsCredentialGenerator(
      Properties nonMappedOptions, String userName, HostAddress hostAddress) {
    // Build RDS IAM-auth token generator
    this.userName = userName;
    String accessKeyId = nonMappedOptions.getProperty("accessKeyId");
    String secretKey = nonMappedOptions.getProperty("secretKey");
    String region = nonMappedOptions.getProperty("region");
    String roleArn = nonMappedOptions.getProperty("roleArn");

    AwsCredentialsProvider baseProvider =
        (accessKeyId != null && secretKey != null)
            ? StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretKey))
            : DefaultCredentialsProvider.builder().build();
    Region awsRegion =
        region != null ? Region.of(region) : new DefaultAwsRegionProviderChain().getRegion();

    // CONJ-877: with roleArn, the token is signed with the temporary credentials of that role,
    // assumed through STS with the credentials above (roleSessionName and externalId optional)
    final AwsCredentialsProvider awsCredentialsProvider;
    StsClient stsClient = null;
    if (roleArn != null) {
      stsClient = StsClient.builder().region(awsRegion).credentialsProvider(baseProvider).build();
      String sessionName = nonMappedOptions.getProperty("roleSessionName", "mariadb-connector-j");
      String externalId = nonMappedOptions.getProperty("externalId");
      AssumeRoleRequest.Builder request =
          AssumeRoleRequest.builder().roleArn(roleArn).roleSessionName(sessionName);
      if (externalId != null) request.externalId(externalId);
      awsCredentialsProvider =
          StsAssumeRoleCredentialsProvider.builder()
              .stsClient(stsClient)
              .refreshRequest(request.build())
              .build();
    } else {
      awsCredentialsProvider = baseProvider;
    }

    RdsUtilities utilities =
        RdsUtilities.builder()
            .credentialsProvider(awsCredentialsProvider)
            .region(awsRegion)
            .build();

    try {
      this.authenticationToken =
          utilities.generateAuthenticationToken(
              builder ->
                  builder
                      .username(userName)
                      .hostname(hostAddress.host)
                      .port(hostAddress.port)
                      .credentialsProvider(awsCredentialsProvider));
    } finally {
      // the providers and the STS client are closeable (SdkAutoCloseable extends AutoCloseable)
      for (Object resource : new Object[] {awsCredentialsProvider, stsClient, baseProvider}) {
        if (resource instanceof AutoCloseable closeable) {
          try {
            closeable.close();
          } catch (Exception e) {
            // eat
          }
        }
      }
    }
  }

  /**
   * Generate authentication token
   *
   * @return token
   */
  public Credential getToken() {
    return new Credential(userName, authenticationToken);
  }
}
