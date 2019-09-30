/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
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

import java.io.*;
import java.net.*;
import java.security.*;
import java.security.cert.*;
import java.security.cert.Certificate;
import java.sql.*;
import javax.net.ssl.*;
import org.mariadb.jdbc.internal.logging.*;
import org.mariadb.jdbc.internal.util.exceptions.*;
import org.mariadb.jdbc.tls.*;
import org.mariadb.jdbc.util.*;

public class DefaultTlsSocketPlugin implements TlsSocketPlugin {
  private static final Logger logger = LoggerFactory.getLogger(DefaultTlsSocketPlugin.class);

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
      throw ExceptionMapper.connException("Could not initialize SSL context", keyManagementEx);
    } catch (NoSuchAlgorithmException noSuchAlgorithmEx) {
      throw ExceptionMapper.connException(
          "SSLContext TLS Algorithm not unknown", noSuchAlgorithmEx);
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
      throw ExceptionMapper.connException("Failed to create keyStore instance", generalSecurityEx);
    } catch (FileNotFoundException fileNotFoundEx) {
      throw ExceptionMapper.connException(
          "Failed to find keyStore file. Option keyStore=" + keyStoreUrl, fileNotFoundEx);
    } catch (IOException ioEx) {
      throw ExceptionMapper.connException(
          "Failed to read keyStore file. Option keyStore=" + keyStoreUrl, ioEx);
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
}
