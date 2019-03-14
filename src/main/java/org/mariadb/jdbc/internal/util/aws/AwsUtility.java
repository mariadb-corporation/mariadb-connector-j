package org.mariadb.jdbc.internal.util.aws;

public class AwsUtility {

  /**
   * Check whether AWS SDK 1.x RDS is present on the classpath.
   *
   * @return true if AWS SDK RDS is available
   */
  public static boolean isAwsSdk1RdsPresent() {
    try {
      Class.forName("com.amazonaws.services.rds.auth.GetIamAuthTokenRequest");
      return true;
    } catch (ClassNotFoundException ex) {
      return false;
    }
  }

}
