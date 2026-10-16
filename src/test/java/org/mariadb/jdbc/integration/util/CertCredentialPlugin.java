// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.integration.util;

import org.mariadb.jdbc.plugin.Credential;
import org.mariadb.jdbc.plugin.CredentialPlugin;

/**
 * Test credential plugin that vouches for a server certificate authority (inline PEM), the way the
 * AWS-IAM plugin bundles the RDS root certificates.
 */
public class CertCredentialPlugin implements CredentialPlugin {

  /** Self-signed CA certificate. */
  public static final String PEM =
      "-----BEGIN CERTIFICATE-----\n"
          + "MIIDGTCCAgGgAwIBAgIUB4ujQD4NLSLXvCnsIfwwQR+flfIwDQYJKoZIhvcNAQEL\n"
          + "BQAwGzEZMBcGA1UEAwwQbWFyaWFkYi1jZXJ0dGVzdDAgFw0yNjEwMTQxMzA2NTNa\n"
          + "GA8yMTI2MDkyMDEzMDY1M1owGzEZMBcGA1UEAwwQbWFyaWFkYi1jZXJ0dGVzdDCC\n"
          + "ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKSbdQzEHOcWEbu3SWc4s08O\n"
          + "p5zm2Yx8hfFPsfQiUoxStDUxOu6wvrS5kZzJisKSaNXzEgQxWZYsJtGGu+JBXcJU\n"
          + "wLOfJB+aYDgOXgpaY9lpEDzpyQaN4tf44OX/Z0iqQC6+s0npUlB4LQa8rG/v6Jz1\n"
          + "eeAUfADi+TMNlxHu3cwylvAhcx2zMPsyPwjB27pNVMBIUezY83K6EY1MtMrL0SzB\n"
          + "W/zv/OyadTZtIeOxjUe/73rxx17qcdfNR2b6POEmkQmNUhFFsTnShnTa47SEbezD\n"
          + "6lfH9hoP+A+wjoAhWRjF+aAElxlSjUdp5jjxarZ4I1onJGtp6M8f3NZaxNtSRU0C\n"
          + "AwEAAaNTMFEwHQYDVR0OBBYEFFelB7wo+UPTvt5pEvooA9kwMkLVMB8GA1UdIwQY\n"
          + "MBaAFFelB7wo+UPTvt5pEvooA9kwMkLVMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZI\n"
          + "hvcNAQELBQADggEBABWSHq8muy5RyfxJYvODD417MFNtud6dLQ+9Dy2956bbZGjj\n"
          + "ZI8/M+qLfqsJFTtD3kkVmWelv1Qu+EjKvdOZfTADVAUX7C2wuBSmOfHCpg8JJDSy\n"
          + "p30qA81+KhR+yKNjr6YaDXZvYWvVLEv8McjK8h3LG7vUTf2j+HZ9HKuDzphdFPL9\n"
          + "PmLsOvh7ZdLGiCzXaRLR2ddhc2NO6wbN/r9hJtcw95KBXOWIXI5O3sOXWrYDffVx\n"
          + "Xu3Rrsc29xG4iP3dYZgkgu0jVwexAsAN5A+GI4iHm+nfyqihpBYXddMGs+802sgz\n"
          + "IbRh0cl0RypUkdyC+YUPjmOsPyXHAMAyuj8aAg4=\n"
          + "-----END CERTIFICATE-----\n";

  @Override
  public String type() {
    return "CERTTEST";
  }

  @Override
  public boolean mustUseSsl() {
    return true;
  }

  @Override
  public String defaultServerSslCert() {
    return PEM;
  }

  @Override
  public Credential get() {
    return new Credential("user", "pwd");
  }
}
