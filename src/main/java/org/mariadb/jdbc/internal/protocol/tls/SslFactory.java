package org.mariadb.jdbc.internal.protocol.tls;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionMapper;
import org.mariadb.jdbc.util.Options;

public class SslFactory {

  private static final Logger logger = LoggerFactory.getLogger(SslFactory.class);

  /**
   * Create an SSL factory according to connection string options.
   *
   * @param options   connection options
   * @return SSL socket factory
   * @throws SQLException in case of error initializing context.
   */
  public static SSLSocketFactory getSslSocketFactory(Options options) throws SQLException {

    TrustManager[] trustManager = null;
    KeyManager[] keyManager = null;

    if (options.trustServerCertificate || options.serverSslCert != null
        || options.trustStore != null) {
      trustManager = new X509TrustManager[]{new MariaDbX509TrustManager(options)};
    }

    if (options.keyStore != null) {
      keyManager = new KeyManager[]{
          loadClientCerts(options.keyStore, options.keyStorePassword, options.keyPassword, options.keyStoreType)};
    } else {
      String keyStore = System.getProperty("javax.net.ssl.keyStore");
      String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
      if (keyStore != null) {
        try {
          keyManager = new KeyManager[]{
              loadClientCerts(keyStore, keyStorePassword, keyStorePassword, options.keyStoreType)};
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
      throw ExceptionMapper
          .connException("SSLContext TLS Algorithm not unknown", noSuchAlgorithmEx);
    }
  }

  private static KeyManager loadClientCerts(String keyStoreUrl, String keyStorePassword,
                                            String keyPassword, String storeType) throws SQLException {
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
      throw ExceptionMapper
          .connException("Failed to find keyStore file. Option keyStore=" + keyStoreUrl,
              fileNotFoundEx);
    } catch (IOException ioEx) {
      throw ExceptionMapper
          .connException("Failed to read keyStore file. Option keyStore=" + keyStoreUrl, ioEx);
    } finally {
      try {
        if (inStream != null) {
          inStream.close();
        }
      } catch (IOException ioEx) {
        //ignore error
      }
    }

  }
}
