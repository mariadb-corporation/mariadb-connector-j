// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.tls;

import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;

/**
 * Class to accept any server certificate.
 *
 * <p>This permit to have network encrypted, BUT client doesn't validate server identity !!
 */
public class MariaDbX509TrustingManager implements X509TrustManager {

  /** Constructor */
  public MariaDbX509TrustingManager() {}

  @Override
  public void checkClientTrusted(X509Certificate[] x509Certificates, String string) {}

  @Override
  public void checkServerTrusted(X509Certificate[] x509Certificates, String string) {}

  public X509Certificate[] getAcceptedIssuers() {
    return null;
  }
}
