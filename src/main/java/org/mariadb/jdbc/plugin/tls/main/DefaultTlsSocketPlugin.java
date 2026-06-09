// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.tls.main;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.tls.HostnameVerifier;
import org.mariadb.jdbc.client.tls.MariaDbX509DeferredIdentityTrustManager;
import org.mariadb.jdbc.client.tls.MariaDbX509KeyManager;
import org.mariadb.jdbc.client.tls.MariaDbX509TrustingManager;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.export.SslMode;
import org.mariadb.jdbc.plugin.TlsSocketPlugin;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

/** Default TLS socket plugin */
public class DefaultTlsSocketPlugin implements TlsSocketPlugin {
  private static final Logger logger = Loggers.getLogger(DefaultTlsSocketPlugin.class);

  private static final ConcurrentHashMap<String, CachedTrust> TRUST_CACHE =
      new ConcurrentHashMap<>();

  private static final ConcurrentHashMap<String, KeyManager[]> KEY_CACHE =
      new ConcurrentHashMap<>();

  private static final int MAX_CACHE_SIZE = 256;

  /**
   * Non-mapped option {@code disableSslContextCache} to disable the trust/key-manager cache and
   * rebuild the managers on every connection.
   */
  private static boolean cacheDisabled(Configuration conf) {
    return Boolean.parseBoolean(
        conf.nonMappedOptions().getProperty("disableSslContextCache", "false"));
  }

  private static KeyManager loadClientCerts(
      String keyStoreUrl,
      String keyStorePassword,
      String keyPassword,
      String storeType,
      ExceptionFactory exceptionFactory)
      throws SQLException {

    try {
      try (InputStream inStream = loadFromUrl(keyStoreUrl)) {
        char[] keyStorePasswordChars =
            keyStorePassword == null
                ? null
                : (keyStorePassword.isEmpty()) ? null : keyStorePassword.toCharArray();
        char[] keyStoreChars =
            (keyPassword == null)
                ? keyStorePasswordChars
                : (keyPassword.isEmpty()) ? null : keyPassword.toCharArray();
        KeyStore ks =
            KeyStore.getInstance(storeType != null ? storeType : KeyStore.getDefaultType());
        ks.load(inStream, keyStorePasswordChars);
        return new MariaDbX509KeyManager(ks, keyStoreChars);
      }
    } catch (IOException | GeneralSecurityException ex) {
      throw exceptionFactory.create(
          "Failed to read keyStore file. Option keyStore=" + keyStoreUrl, "08000", ex);
    }
  }

  private static InputStream loadFromUrl(String keyStoreUrl) throws FileNotFoundException {
    try {
      return new URI(keyStoreUrl).toURL().openStream();
    } catch (Exception exception) {
      return new FileInputStream(keyStoreUrl);
    }
  }

  private static InputStream getInputStreamFromPath(String path) throws IOException {
    try {
      return new URI(path).toURL().openStream();
    } catch (Exception e) {
      if (path.startsWith("-----")) {
        return new ByteArrayInputStream(path.getBytes());
      } else {
        File f = new File(path);
        if (f.exists() && !f.isDirectory()) {
          return f.toURI().toURL().openStream();
        }
        throw new IOException(
            String.format("File not found for option `serverSslCert` (value: '%s')", path), e);
      }
    }
  }

  @Override
  public String type() {
    return "DEFAULT";
  }

  @Override
  public TrustManager[] getTrustManager(
      Configuration conf, ExceptionFactory exceptionFactory, HostAddress hostAddress)
      throws SQLException {
    if (cacheDisabled(conf)) {
      return buildTrustManager(conf, exceptionFactory, hostAddress);
    }
    String key = trustCacheKey(conf, hostAddress);
    CachedTrust cached = TRUST_CACHE.get(key);
    if (cached == null) {
      cached = buildTrust(conf, exceptionFactory, hostAddress);
      // bound memory: store rotation creates new keys over time. Reset rather than grow unbounded.
      if (TRUST_CACHE.size() >= MAX_CACHE_SIZE) TRUST_CACHE.clear();
      CachedTrust previous = TRUST_CACHE.putIfAbsent(key, cached);
      if (previous != null) cached = previous;
    }
    return wrapTrust(cached);
  }

  // package-private (not private) so the JMH benchmark can measure an uncached baseline
  TrustManager[] buildTrustManager(
      Configuration conf, ExceptionFactory exceptionFactory, HostAddress hostAddress)
      throws SQLException {
    return wrapTrust(buildTrust(conf, exceptionFactory, hostAddress));
  }

  /**
   * Wraps the cached base trust manager for a single connection: paths that defer identity
   * validation to authentication get a fresh {@link MariaDbX509DeferredIdentityTrustManager} (so
   * the captured fingerprint is per-connection state); other paths use the stateless base directly.
   */
  private static TrustManager[] wrapTrust(CachedTrust cachedTrust) {
    return new TrustManager[] {
      cachedTrust.deferIdentity
          ? new MariaDbX509DeferredIdentityTrustManager(cachedTrust.base)
          : cachedTrust.base
    };
  }

  private CachedTrust buildTrust(
      Configuration conf, ExceptionFactory exceptionFactory, HostAddress hostAddress)
      throws SQLException {
    SslMode sslMode = hostAddress.sslMode == null ? conf.sslMode() : hostAddress.sslMode;
    if (sslMode == SslMode.TRUST) {
      return new CachedTrust(new MariaDbX509TrustingManager(), false);
    }

    // if certificate is provided, load it.
    if (conf.serverSslCert() != null || conf.trustStore() != null) {
      KeyStore ks;
      try {
        ks =
            KeyStore.getInstance(
                conf.trustStoreType() != null ? conf.trustStoreType() : KeyStore.getDefaultType());
      } catch (GeneralSecurityException generalSecurityEx) {
        throw exceptionFactory.create(
            "Failed to create keystore instance", "08000", generalSecurityEx);
      }
      if (conf.trustStore() != null) {
        InputStream inStream;
        try {
          inStream = loadFromUrl(conf.trustStore());
        } catch (IOException ioexception) {
          try {
            inStream = new FileInputStream(conf.trustStore());
          } catch (FileNotFoundException fileNotFoundEx) {
            throw new SQLException(
                "Failed to find trustStore file. trustStore=" + conf.trustStore(),
                "08000",
                fileNotFoundEx);
          }
        }
        try {
          ks.load(
              inStream,
              conf.trustStorePassword() == null ? null : conf.trustStorePassword().toCharArray());
        } catch (IOException | NoSuchAlgorithmException | CertificateException ioEx) {
          throw exceptionFactory.create("Failed load keyStore", "08000", ioEx);
        } finally {
          try {
            inStream.close();
          } catch (IOException e) {
            // eat
          }
        }
        try {
          TrustManagerFactory tmf =
              TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          tmf.init(ks);
          for (TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
              return new CachedTrust((X509TrustManager) tm, true);
            }
          }
        } catch (GeneralSecurityException generalSecurityEx) {
          throw exceptionFactory.create(
              "Failed to load certificates from serverSslCert/trustStore",
              "08000",
              generalSecurityEx);
        }
      } else {
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

        } catch (IOException ioEx) {
          throw exceptionFactory.create("Failed load keyStore", "08000", ioEx);
        } catch (GeneralSecurityException generalSecurityEx) {
          throw exceptionFactory.create(
              "Failed to store certificate from serverSslCert into a keyStore",
              "08000",
              generalSecurityEx);
        }

        try {
          TrustManagerFactory tmf =
              TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          tmf.init(ks);
          for (TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
              return new CachedTrust((X509TrustManager) tm, false);
            }
          }
        } catch (GeneralSecurityException generalSecurityEx) {
          throw exceptionFactory.create(
              "Failed to load certificates from serverSslCert/trustStore",
              "08000",
              generalSecurityEx);
        }
      }
    } else if (conf.fallbackToSystemTrustStore()) {
      // relying on default truststore
      try {
        TrustManagerFactory tmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((KeyStore) null);

        for (TrustManager tm : tmf.getTrustManagers()) {
          if (tm instanceof X509TrustManager) {
            return new CachedTrust((X509TrustManager) tm, true);
          }
        }
      } catch (Exception e) {
        throw new SQLException("No X509TrustManager found", e);
      }
    }

    throw new SQLException("No X509TrustManager found");
  }

  @Override
  public KeyManager[] getKeyManager(Configuration conf, ExceptionFactory exceptionFactory)
      throws SQLException {
    if (conf.keyStore() == null || cacheDisabled(conf)) {
      return buildKeyManager(conf, exceptionFactory);
    }
    String key = keyCacheKey(conf);
    KeyManager[] cached = KEY_CACHE.get(key);
    if (cached != null) return cached;

    KeyManager[] built = buildKeyManager(conf, exceptionFactory);
    if (built == null) return null;
    if (KEY_CACHE.size() >= MAX_CACHE_SIZE) KEY_CACHE.clear();
    KeyManager[] previous = KEY_CACHE.putIfAbsent(key, built);
    return previous != null ? previous : built;
  }

  KeyManager[] buildKeyManager(Configuration conf, ExceptionFactory exceptionFactory)
      throws SQLException {

    KeyManager[] keyManager = null;

    if (conf.keyStore() != null) {
      keyManager =
          new KeyManager[] {
            loadClientCerts(
                conf.keyStore(),
                conf.keyStorePassword(),
                conf.keyPassword(),
                conf.keyStoreType(),
                exceptionFactory)
          };
    } else if (conf.fallbackToSystemKeyStore()) {
      String keyStore = System.getProperty("javax.net.ssl.keyStore");
      String keyStorePassword =
          System.getProperty("javax.net.ssl.keyStorePassword", conf.keyStorePassword());
      String keyStoreType = System.getProperty("javax.net.ssl.keyStoreType", conf.keyStoreType());
      if (keyStore != null) {
        try {
          keyManager =
              new KeyManager[] {
                loadClientCerts(
                    keyStore, keyStorePassword, keyStorePassword, keyStoreType, exceptionFactory)
              };
        } catch (SQLException queryException) {
          keyManager = null;
          logger.error("Error loading key manager from system properties", queryException);
        }
      }
    }
    return keyManager;
  }

  /**
   * Cache key for the trust managers. The options read by {@link #buildTrustManager}. A store
   * referenced by a file path contributes a freshness token (last-modified + length) so replacing
   * it on disk forces a rebuild. The system trust store used by the fallback path is identified by
   * the {@code javax.net.ssl.trustStore} property (+ token). The key is hashed so that store
   * passwords are not retained as plain text in a long-lived map.
   */
  private static String trustCacheKey(Configuration conf, HostAddress hostAddress) {
    SslMode sslMode = hostAddress.sslMode == null ? conf.sslMode() : hostAddress.sslMode;
    StringBuilder sb = new StringBuilder();
    // buildTrustManager only branches on TRUST; VERIFY_CA and VERIFY_FULL share the same managers.
    sb.append(sslMode == SslMode.TRUST ? "TRUST" : "VERIFY").append('\n');
    appendFile(sb, conf.serverSslCert());
    appendFile(sb, conf.trustStore());
    appendValue(sb, conf.trustStorePassword());
    appendValue(sb, conf.trustStoreType());
    sb.append(conf.fallbackToSystemTrustStore()).append('\n');
    // the system trust store the fallback path relies on (default cacerts when unset)
    appendFile(sb, System.getProperty("javax.net.ssl.trustStore"));
    appendValue(sb, System.getProperty("javax.net.ssl.trustStorePassword"));
    appendValue(sb, System.getProperty("javax.net.ssl.trustStoreType"));
    return sha256(sb.toString());
  }

  private static String keyCacheKey(Configuration conf) {
    StringBuilder sb = new StringBuilder();
    appendFile(sb, conf.keyStore());
    appendValue(sb, conf.keyStorePassword());
    appendValue(sb, conf.keyPassword());
    appendValue(sb, conf.keyStoreType());
    return sha256(sb.toString());
  }

  private static void appendValue(StringBuilder sb, String value) {
    sb.append(value == null ? "" : value).append('\n');
  }

  private static void appendFile(StringBuilder sb, String value) {
    appendValue(sb, value);
    sb.append(freshnessToken(value)).append('\n');
  }

  /**
   * Freshness token for a store reference. For a local file it is last-modified + length, so an
   * on-disk replacement invalidates the cache. Inline PEM is its own token (content addressed).
   * Remote/classpath references cannot be checked cheaply and are tracked by their string only.
   */
  private static String freshnessToken(String value) {
    if (value == null || value.startsWith("-----")) return "";
    File file = toFile(value);
    if (file != null && file.isFile()) return file.lastModified() + ":" + file.length();
    return "";
  }

  private static File toFile(String value) {
    try {
      URI uri = new URI(value);
      if ("file".equalsIgnoreCase(uri.getScheme())) return new File(uri);
    } catch (Exception e) {
      // not a URI, fall through to a plain path
    }
    try {
      return new File(value);
    } catch (Exception e) {
      return null;
    }
  }

  private static String sha256(String input) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      // unreachable, SHA-256 always exists
      return input;
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

  private static final class CachedTrust {
    private final X509TrustManager base;
    // when true, this path cannot validate the server certificate against the trust store and
    // defers identity validation to authentication (per-connection fingerprint capture)
    private final boolean deferIdentity;

    private CachedTrust(X509TrustManager base, boolean deferIdentity) {
      this.base = base;
      this.deferIdentity = deferIdentity;
    }
  }
}
