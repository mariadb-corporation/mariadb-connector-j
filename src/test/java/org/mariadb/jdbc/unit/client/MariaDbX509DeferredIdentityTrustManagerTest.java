// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.InputStream;
import java.net.InetAddress;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.tls.MariaDbX509DeferredIdentityTrustManager;

/**
 * Verifies, over a real loopback TLS handshake, that {@link
 * MariaDbX509DeferredIdentityTrustManager} captures the accepted self-signed certificate
 * fingerprint (and only when the certificate is not CA-trusted).
 */
class MariaDbX509DeferredIdentityTrustManagerTest {

  private static final char[] PASSWORD = "changeit".toCharArray();

  private static KeyStore serverKeyStore() throws Exception {
    KeyStore ks = KeyStore.getInstance("PKCS12");
    try (InputStream in =
        MariaDbX509DeferredIdentityTrustManagerTest.class.getResourceAsStream(
            "/loopback-server.p12")) {
      assertNotNull(in, "loopback-server.p12 test resource missing");
      ks.load(in, PASSWORD);
    }
    return ks;
  }

  private static byte[] sha256(X509Certificate cert) throws Exception {
    return MessageDigest.getInstance("SHA-256").digest(cert.getEncoded());
  }

  private static X509TrustManager firstX509(TrustManagerFactory tmf) {
    for (TrustManager tm : tmf.getTrustManagers()) {
      if (tm instanceof X509TrustManager) return (X509TrustManager) tm;
    }
    throw new IllegalStateException("no X509TrustManager");
  }

  /** System trust managers — do NOT trust the loopback server's self-signed certificate. */
  private static X509TrustManager systemTrustManager() throws Exception {
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init((KeyStore) null);
    return firstX509(tmf);
  }

  /** Trust managers that DO trust the given certificate. */
  private static X509TrustManager trustManagerTrusting(X509Certificate cert) throws Exception {
    KeyStore ts = KeyStore.getInstance("PKCS12");
    ts.load(null, null);
    ts.setCertificateEntry("server", cert);
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ts);
    return firstX509(tmf);
  }

  /**
   * Runs one loopback TLS handshake using the given (per-connection) deferred-identity trust
   * manager and returns the fingerprint it captured during the handshake.
   */
  private static byte[] handshakeAndReadFingerprint(
      MariaDbX509DeferredIdentityTrustManager clientTm) throws Exception {
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(serverKeyStore(), PASSWORD);
    SSLContext serverCtx = SSLContext.getInstance("TLS");
    serverCtx.init(kmf.getKeyManagers(), null, null);

    SSLServerSocket serverSocket =
        (SSLServerSocket)
            serverCtx
                .getServerSocketFactory()
                .createServerSocket(0, 1, InetAddress.getLoopbackAddress());
    serverSocket.setSoTimeout(15_000);
    int port = serverSocket.getLocalPort();

    AtomicReference<Throwable> serverError = new AtomicReference<>();
    Thread serverThread =
        new Thread(
            () -> {
              try (SSLSocket s = (SSLSocket) serverSocket.accept()) {
                s.setSoTimeout(15_000);
                s.startHandshake();
                s.getInputStream().read();
              } catch (Throwable t) {
                serverError.set(t);
              }
            },
            "loopback-tls-server");
    serverThread.setDaemon(true);
    serverThread.start();

    SSLContext clientCtx = SSLContext.getInstance("TLS");
    clientCtx.init(null, new TrustManager[] {clientTm}, null);

    try (SSLSocket client =
        (SSLSocket)
            clientCtx.getSocketFactory().createSocket(InetAddress.getLoopbackAddress(), port)) {
      client.setSoTimeout(15_000);
      client.startHandshake();
      client.getOutputStream().write(1);
    } finally {
      serverSocket.close();
    }

    serverThread.join(15_000);
    if (serverError.get() != null) {
      throw new AssertionError("server side failed", serverError.get());
    }
    return clientTm.getFingerprint();
  }

  @Test
  void fingerprintCapturedForSelfSignedCertificate() throws Exception {
    byte[] expected = sha256((X509Certificate) serverKeyStore().getCertificate("test"));

    byte[] fingerprint =
        handshakeAndReadFingerprint(
            new MariaDbX509DeferredIdentityTrustManager(systemTrustManager()));

    assertNotNull(fingerprint, "fingerprint must be captured for an untrusted self-signed cert");
    assertArrayEquals(
        expected, fingerprint, "captured fingerprint must be SHA-256 of the server certificate");
  }

  @Test
  void noFingerprintWhenCertificateIsTrusted() throws Exception {
    X509Certificate serverCert = (X509Certificate) serverKeyStore().getCertificate("test");

    byte[] fingerprint =
        handshakeAndReadFingerprint(
            new MariaDbX509DeferredIdentityTrustManager(trustManagerTrusting(serverCert)));

    assertNull(
        fingerprint, "a CA-trusted certificate must NOT capture a fingerprint (normal validation)");
  }
}
