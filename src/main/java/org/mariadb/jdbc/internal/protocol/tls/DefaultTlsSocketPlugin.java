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

package org.mariadb.jdbc.internal.protocol.tls;

import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionFactory;
import org.mariadb.jdbc.tls.TlsSocketPlugin;
import org.mariadb.jdbc.util.Options;

import javax.net.ssl.*;
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

public class DefaultTlsSocketPlugin implements TlsSocketPlugin {
  private static final Logger logger = LoggerFactory.getLogger(DefaultTlsSocketPlugin.class);

  private static KeyManager loadClientCerts(
      String keyStoreUrl, String keyStorePassword, String keyPassword, String storeType)
      throws SQLException {
    InputStream inStream = null;
    try {

      char[] keyStorePasswordChars =
          keyStorePassword == null ? null : keyStorePassword.toCharArray();

      try {
        inStream = new URL(keyStoreUrl).openStream();
      } catch (IOException ioexception) {
        inStream = new FileInputStream(keyStoreUrl);
      }

      KeyStore ks = KeyStore.getInstance(storeType != null ? storeType : KeyStore.getDefaultType());
      ks.load(inStream, keyStorePasswordChars);
      char[] keyStoreChars =
          (keyPassword == null) ? keyStorePasswordChars : keyPassword.toCharArray();
      return new MariaDbX509KeyManager(ks, keyStoreChars);
    } catch (GeneralSecurityException generalSecurityEx) {
      throw ExceptionFactory.INSTANCE.create(
          "Failed to create keyStore instance", "08000", generalSecurityEx);
    } catch (FileNotFoundException fileNotFoundEx) {
      throw ExceptionFactory.INSTANCE.create(
          "Failed to find keyStore file. Option keyStore=" + keyStoreUrl, "08000", fileNotFoundEx);
    } catch (IOException ioEx) {
      throw ExceptionFactory.INSTANCE.create(
          "Failed to read keyStore file. Option keyStore=" + keyStoreUrl, "08000", ioEx);
    } finally {
      try {
        if (inStream != null) {
          inStream.close();
        }
      } catch (IOException ioEx) {
        // ignore error
      }
    }
  }

  @Override
  public String name() {
    return "Default TLS socket factory";
  }

  @Override
  public String type() {
    return "DEFAULT";
  }

  @Override
  public SSLSocketFactory getSocketFactory(Options options) throws SQLException {

    TrustManager[] trustManager = null;
    KeyManager[] keyManager = null;

    if (options.trustServerCertificate
        || options.serverSslCert != null
        || options.trustStore != null) {
      trustManager = new X509TrustManager[] {new MariaDbX509TrustManager(options)};
    }

    if (options.keyStore != null) {
      keyManager =
          new KeyManager[] {
            loadClientCerts(
                options.keyStore,
                options.keyStorePassword,
                options.keyPassword,
                options.keyStoreType)
          };
    } else {
      String keyStore = System.getProperty("javax.net.ssl.keyStore");
      String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
      if (keyStore != null) {
        try {
          keyManager =
              new KeyManager[] {
                loadClientCerts(keyStore, keyStorePassword, keyStorePassword, options.keyStoreType)
              };
        } catch (SQLException queryException) {
          keyManager = null;
          logger.error("Error loading keymanager from system properties", queryException);
        }
      }
    }

    try {
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(keyManager, trustManager, null);
      return sslContext.getSocketFactory();
    } catch (KeyManagementException keyManagementEx) {
      throw ExceptionFactory.INSTANCE.create(
          "Could not initialize SSL context", "08000", keyManagementEx);
    } catch (NoSuchAlgorithmException noSuchAlgorithmEx) {
      throw ExceptionFactory.INSTANCE.create(
          "SSLContext TLS Algorithm not unknown", "08000", noSuchAlgorithmEx);
    }
  }

  @Override
  public void verify(String host, SSLSession session, Options options, long serverThreadId)
      throws SSLException {
    HostnameVerifierImpl hostnameVerifier = new HostnameVerifierImpl();
    if (!hostnameVerifier.verify(host, session, serverThreadId)) {

      // Use proprietary verify method in order to have an exception with a better description
      // of error.
      Certificate[] certs = session.getPeerCertificates();
      X509Certificate cert = (X509Certificate) certs[0];
      hostnameVerifier.verify(host, cert, serverThreadId);
    }
  }
}
