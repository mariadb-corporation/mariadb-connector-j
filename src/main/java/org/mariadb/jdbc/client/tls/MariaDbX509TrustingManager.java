/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
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

package org.mariadb.jdbc.client.tls;

import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;

/**
 * Class to accept any server certificate.
 *
 * <p>This permit to have network encrypted, BUT client doesn't validate server identity !!
 */
public class MariaDbX509TrustingManager implements X509TrustManager {

  public MariaDbX509TrustingManager() {}

  @Override
  public void checkClientTrusted(X509Certificate[] x509Certificates, String string) {}

  @Override
  public void checkServerTrusted(X509Certificate[] x509Certificates, String string) {}

  public X509Certificate[] getAcceptedIssuers() {
    return null;
  }
}
