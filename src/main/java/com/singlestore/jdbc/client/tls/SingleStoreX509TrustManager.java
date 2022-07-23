// Copyright (c) 2022 SingleStore, Inc.

package com.singlestore.jdbc.client.tls;

import com.singlestore.jdbc.util.exceptions.ExceptionFactory;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

public class SingleStoreX509TrustManager implements X509TrustManager {

  private X509TrustManager trustManager;

  /**
   * SingleStoreX509TrustManager.
   *
   * @param ks KeyStore containing the trusted server certificates
   * @param exceptionFactory exception factory
   * @throws SQLException exception
   */
  public SingleStoreX509TrustManager(KeyStore ks, ExceptionFactory exceptionFactory)
      throws SQLException {
    try {
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
      for (TrustManager tm : tmf.getTrustManagers()) {
        if (tm instanceof X509TrustManager) {
          trustManager = (X509TrustManager) tm;
          break;
        }
      }
    } catch (NoSuchAlgorithmException noSuchAlgorithmEx) {
      throw exceptionFactory.create(
          "Failed to create TrustManagerFactory default instance", "08000", noSuchAlgorithmEx);
    } catch (GeneralSecurityException generalSecurityEx) {
      throw exceptionFactory.create(
          "Failed to initialize trust manager", "08000", generalSecurityEx);
    }

    if (trustManager == null) {
      throw exceptionFactory.create("No X509TrustManager found", "08000");
    }
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
    trustManager.checkServerTrusted(x509Certificates, string);
  }

  public X509Certificate[] getAcceptedIssuers() {
    return null;
  }
}
