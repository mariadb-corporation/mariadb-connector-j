package org.mariadb.jdbc.internal.util.aws;

/**
 * Mechanism for generating AWS RDS IAM authentication tokens, which
 * are then used in place of password to authentication with RDS databases.
 *
 * @see <a href="https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html">
 * AWS Documentation » Amazon Relational Database Service (RDS) » User Guide » Configuring Security in Amazon RDS » Authentication and Access Control » IAM Database Authentication for MySQL and PostgreSQL
 * </a>
 */
public interface AwsRdsIamPasswordGenerator {

  boolean isPasswordExpired();

  String generateNewPassword();

}
