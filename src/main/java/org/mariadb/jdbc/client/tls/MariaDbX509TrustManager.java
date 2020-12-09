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

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.*;
import java.sql.SQLException;
import java.util.Collection;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.SslMode;
import org.mariadb.jdbc.util.exceptions.ExceptionFactory;

public class MariaDbX509TrustManager implements X509TrustManager {

  private X509TrustManager trustManager;

  public MariaDbX509TrustManager(Configuration conf, ExceptionFactory exceptionFactory)
      throws SQLException {

    if (SslMode.NO_VERIFICATION == conf.sslMode()) {
      return;
    }

    KeyStore ks;
    try {
      ks = KeyStore.getInstance(KeyStore.getDefaultType());
    } catch (GeneralSecurityException generalSecurityEx) {
      throw exceptionFactory.create(
          "Failed to create keystore instance", "08000", generalSecurityEx);
    }

    InputStream inStream = null;
    try {
      // generate a keyStore from the provided cert
      inStream = getInputStreamFromPath(conf.serverSslCert());

      // Note: KeyStore requires it be loaded even if you don't load anything into it
      // (will be initialized with "javax.net.ssl.trustStore") values.
      ks.load(null);
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      Collection<? extends Certificate> caList = cf.generateCertificates(inStream);
      for (Certificate ca : caList) {
        ks.setCertificateEntry(UUID.randomUUID().toString(), ca);
      }

      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
      for (TrustManager tm : tmf.getTrustManagers()) {
        if (tm instanceof X509TrustManager) {
          trustManager = (X509TrustManager) tm;
          break;
        }
      }

      if (trustManager == null) {
        throw new SQLException("No X509TrustManager found");
      }

    } catch (IOException ioEx) {
      throw exceptionFactory.create("Failed load keyStore", "08000", ioEx);
    } catch (GeneralSecurityException generalSecurityEx) {
      throw exceptionFactory.create(
          "Failed to store certificate from serverSslCert into a keyStore",
          "08000",
          generalSecurityEx);
    } finally {
      if (inStream != null) {
        try {
          inStream.close();
        } catch (IOException ioEx) {
          // ignore error
        }
      }
    }
  }

  private static InputStream getInputStreamFromPath(String path) throws IOException {
    InputStream is;
    String protocol = path.replaceFirst("^(\\w+):.+$", "$1").toLowerCase();
    switch (protocol) {
      case "http":
      case "https":
        HttpURLConnection connection = (HttpURLConnection) new URL(path).openConnection();
        int code = connection.getResponseCode();
        if (code >= 400) throw new IOException("Server returned error code #" + code);
        is = connection.getInputStream();
        String contentEncoding = connection.getContentEncoding();
        if (contentEncoding != null && contentEncoding.equalsIgnoreCase("gzip"))
          is = new GZIPInputStream(is);
        break;
      case "file":
        is = new URL(path).openStream();
        break;
      case "classpath":
        is =
            Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(path.replaceFirst("^\\w+:", ""));
        break;
      default:
        if (path.startsWith("-----")) {
          is = new ByteArrayInputStream(path.getBytes());
          break;
        }
        throw new IOException(
            String.format("Wrong value for option `serverSslCert` (value: '%s')", path));
    }
    return is;
  }

  /**
   * Check client trusted.
   *
   * @param x509Certificates certificate
   * @param string string
   * @throws CertificateException exception
   */
  @Override
  public void checkClientTrusted(X509Certificate[] x509Certificates, String string)
      throws CertificateException {
    if (trustManager == null) {
      return;
    }
    trustManager.checkClientTrusted(x509Certificates, string);
  }

  /**
   * Check server trusted.
   *
   * @param x509Certificates certificate
   * @param string string
   * @throws CertificateException exception
   */
  @Override
  public void checkServerTrusted(X509Certificate[] x509Certificates, String string)
      throws CertificateException {
    if (trustManager == null) {
      return;
    }
    trustManager.checkServerTrusted(x509Certificates, string);
  }

  public X509Certificate[] getAcceptedIssuers() {
    return null;
  }
}
