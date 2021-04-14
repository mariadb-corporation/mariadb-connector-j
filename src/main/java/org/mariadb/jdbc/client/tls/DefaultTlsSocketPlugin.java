/*
 *
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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import javax.net.ssl.*;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.plugin.tls.TlsSocketPlugin;
import org.mariadb.jdbc.util.exceptions.ExceptionFactory;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

public class DefaultTlsSocketPlugin implements TlsSocketPlugin {
  private static final Logger logger = Loggers.getLogger(DefaultTlsSocketPlugin.class);

  private static KeyManager loadClientCerts(
      String keyStoreUrl,
      String keyStorePassword,
      String storeType,
      ExceptionFactory exceptionFactory)
      throws SQLException {

    try {
      try (InputStream inStream = loadFromUrl(keyStoreUrl)) {
        char[] keyStorePasswordChars =
            keyStorePassword == null ? null : keyStorePassword.toCharArray();
        KeyStore ks =
            KeyStore.getInstance(storeType != null ? storeType : KeyStore.getDefaultType());
        ks.load(inStream, keyStorePasswordChars);
        return new MariaDbX509KeyManager(ks, keyStorePasswordChars);
      }
    } catch (IOException | GeneralSecurityException ex) {
      throw exceptionFactory.create(
          "Failed to read keyStore file. Option keyStore=" + keyStoreUrl, "08000", ex);
    }
  }

  private static InputStream loadFromUrl(String keyStoreUrl) throws FileNotFoundException {
    try {
      return new URL(keyStoreUrl).openStream();
    } catch (IOException ioexception) {
      return new FileInputStream(keyStoreUrl);
    }
  }

  @Override
  public String type() {
    return "DEFAULT";
  }

  @Override
  public SSLSocketFactory getSocketFactory(Configuration conf, ExceptionFactory exceptionFactory)
      throws SQLException {

    TrustManager[] trustManager = null;
    KeyManager[] keyManager = null;

    switch (conf.sslMode()) {
      case TRUST:
        trustManager = new X509TrustManager[] {new MariaDbX509TrustingManager()};
        break;

      default:
        // if certificate is provided, load it.
        // if not, relying on default truststore
        if (conf.serverSslCert() != null) {
          trustManager =
              new X509TrustManager[] {new MariaDbX509TrustManager(conf, exceptionFactory)};
        }
    }

    if (conf.keyStore() != null) {
      keyManager =
          new KeyManager[] {
            loadClientCerts(
                conf.keyStore(), conf.keyStorePassword(), conf.keyStoreType(), exceptionFactory)
          };
    } else {
      String keyStore = System.getProperty("javax.net.ssl.keyStore");
      String keyStorePassword =
          System.getProperty("javax.net.ssl.keyStorePassword", conf.keyStorePassword());
      String keyStoreType = System.getProperty("javax.net.ssl.keyStoreType", conf.keyStoreType());
      if (keyStore != null) {
        try {
          keyManager =
              new KeyManager[] {
                loadClientCerts(keyStore, keyStorePassword, keyStoreType, exceptionFactory)
              };
        } catch (SQLException queryException) {
          keyManager = null;
          logger.error("Error loading key manager from system properties", queryException);
        }
      }
    }

    try {
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(keyManager, trustManager, null);
      return sslContext.getSocketFactory();
    } catch (KeyManagementException keyManagementEx) {
      throw exceptionFactory.create("Could not initialize SSL context", "08000", keyManagementEx);
    } catch (NoSuchAlgorithmException noSuchAlgorithmEx) {
      throw exceptionFactory.create(
          "SSLContext TLS Algorithm not unknown", "08000", noSuchAlgorithmEx);
    }
  }

  @Override
  public void verify(String host, SSLSession session, Configuration conf, long serverThreadId)
      throws SSLException {
    try {
      Certificate[] certs = session.getPeerCertificates();
      X509Certificate cert = (X509Certificate) certs[0];
      HostnameVerifier.verify(host, cert, serverThreadId);
    } catch (SSLException ex) {
      logger.info(ex.getMessage(), ex);
      throw ex;
    }
  }
}
