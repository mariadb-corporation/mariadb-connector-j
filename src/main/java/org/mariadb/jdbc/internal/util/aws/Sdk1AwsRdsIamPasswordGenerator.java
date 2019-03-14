package org.mariadb.jdbc.internal.util.aws;

import static java.time.LocalDateTime.now;
import static java.time.LocalDateTime.ofEpochSecond;
import static java.time.ZoneOffset.UTC;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.rds.auth.GetIamAuthTokenRequest;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;

import java.time.LocalDateTime;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.UrlParser;

public class Sdk1AwsRdsIamPasswordGenerator implements AwsRdsIamPasswordGenerator {

  // IAM RDS authentication tokens live for 15 minutes, however we keep them
  // around only for 10 minutes as a safety precaution to account for network
  // and other potential latencies.

  private static final int TOKEN_TTL = 10;

  private final RdsIamAuthTokenGenerator tokenGenerator;

  private final GetIamAuthTokenRequest request;

  private LocalDateTime expiration = ofEpochSecond(0, 0, UTC);

  /**
   * Constructor for AwsRdsIamPasswordGenerator implementation based on AWS SDK v1
   *
   * @param urlParser
   */
  public Sdk1AwsRdsIamPasswordGenerator(UrlParser urlParser) {

    // Build RDS IAM-auth token generator
    this.tokenGenerator = RdsIamAuthTokenGenerator.builder()
            .credentials(DefaultAWSCredentialsProviderChain.getInstance())
            .region(new DefaultAwsRegionProviderChain().getRegion())
            .build();

    HostAddress hostAddress = urlParser.getHostAddresses().get(0);

    this.request = GetIamAuthTokenRequest.builder()
            .hostname(hostAddress.host)
            .port(hostAddress.port)
            .userName(urlParser.getUsername())
            .build();
  }

  @Override
  public boolean isPasswordExpired() {
    return expiration.isBefore(now());
  }

  @Override
  public String generateNewPassword() {

    // Generate new token, i.e. password
    String newToken = tokenGenerator.getAuthToken(request);

    // Mark next expiration time
    expiration = now().plusMinutes(TOKEN_TTL);

    // Return new token
    return newToken;
  }

}
