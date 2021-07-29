// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.tls;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.util.Collection;
import java.util.UUID;
import javax.net.ssl.*;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.SslMode;
import org.mariadb.jdbc.plugin.TlsSocketPlugin;
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

    if (conf.sslMode() == SslMode.TRUST) {
      trustManager = new X509TrustManager[] {new MariaDbX509TrustingManager()};
    } else { // if certificate is provided, load it.
      // if not, relying on default truststore
      if (conf.serverSslCert() != null) {

        KeyStore ks;
        try {
          ks = KeyStore.getInstance(KeyStore.getDefaultType());
        } catch (GeneralSecurityException generalSecurityEx) {
          throw exceptionFactory.create(
              "Failed to create keystore instance", "08000", generalSecurityEx);
        }

        try (InputStream inStream = getInputStreamFromPath(conf.serverSslCert())) {
          // generate a keyStore from the provided cert

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
              trustManager = new X509TrustManager[] {(X509TrustManager) tm};
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
        }
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

  private static InputStream getInputStreamFromPath(String path) throws IOException {
    try {
      return new URL(path).openStream();
    } catch (MalformedURLException e) {
      if (path.startsWith("-----")) {
        return new ByteArrayInputStream(path.getBytes());
      } else {
        File f = new File(path);
        if (f.exists() && !f.isDirectory()) {
          return f.toURI().toURL().openStream();
        }
      }
      throw new IOException(
          String.format("Wrong value for option `serverSslCert` (value: '%s')", path));
    }
  }

  @Override
  public void verify(String host, SSLSession session, long serverThreadId) throws SSLException {
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
