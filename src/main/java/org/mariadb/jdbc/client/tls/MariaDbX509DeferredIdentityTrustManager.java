// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.tls;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;

/**
 * TrustManager that defers server-identity validation to the authentication phase.
 *
 * <p>It delegates the standard certificate-chain check to a real {@link X509TrustManager}.
 * When that check fails because the chain cannot be validated against the trust store, typically
 * because the server presented a self-signed certificate it generated on the fly (MariaDB does this
 * when TLS is enabled but no server certificate is configured), this manager does not reject the
 * connection. Instead, it records the leaf certificate fingerprint and lets the handshake proceed,
 * so the server's identity can be validated later, during authentication.
 *
 * <p>Identity is then proven cryptographically by the authentication plugin, which binds the
 * session to this exact certificate (password + scramble + certificate fingerprint). A
 * man-in-the-middle presenting a different certificate produces a different fingerprint, so
 * authentication fails, the fingerprint is what actually validates the peer, not the (self-signed)
 * certificate itself. A self-signed/ephemeral certificate is therefore only the usual trigger for
 * this path, not the basis of the validation.
 *
 * <p>This only <em>delays</em> identity validation; it does not weaken it. Expired or not-yet-valid
 * certificates are still rejected outright here, and this manager must only be used on connection
 * paths that go on to perform fingerprint-based authentication.
 *
 * <p>A new instance is created per connection, wrapping a shared (cached) delegate, so the captured
 * fingerprint is simply per-connection instance state, read afterward via {@link
 * #getFingerprint()}.
 */
public class MariaDbX509DeferredIdentityTrustManager implements X509TrustManager {

  private final X509TrustManager internal;
  private byte[] fingerprint = null;

  /**
   * Wraps a standard {@link X509TrustManager}, capturing the server certificate fingerprint when
   * the chain cannot be validated normally so that identity can be verified later, during
   * authentication.
   *
   * @param javaTrustManager real trust manager
   */
  public MariaDbX509DeferredIdentityTrustManager(X509TrustManager javaTrustManager) {
    internal = javaTrustManager;
  }

  private static byte[] getThumbprint(X509Certificate cert)
      throws NoSuchAlgorithmException, CertificateEncodingException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    return md.digest(cert.getEncoded());
  }

  @Override
  public void checkClientTrusted(X509Certificate[] x509Certificates, String authType)
      throws CertificateException {
    internal.checkClientTrusted(x509Certificates, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] x509Certificates, String authType)
      throws CertificateException {
    try {
      internal.checkServerTrusted(x509Certificates, authType);
    } catch (CertificateException e) {
      if (x509Certificates == null || x509Certificates.length < 1) throw e;
      // The JSSE validator usually surfaces an expired/not-yet-valid certificate wrapped in a
      // generic CertificateException, so catching CertificateExpiredException is unreliable. Check
      // validity explicitly: this throws CertificateExpired/NotYetValidException and rejects the
      // connection, even on the deferred-validation path.
      x509Certificates[0].checkValidity();
      try {
        fingerprint = getThumbprint(x509Certificates[0]);
      } catch (NoSuchAlgorithmException | CertificateEncodingException ex) {
        throw e;
      }
    }
  }

  /**
   * Fingerprint of the server's leaf certificate, captured when the certificate could not be
   * validated against the trust store and its identity must instead be verified during
   * authentication. Returns {@code null} when the certificate validated normally, so no deferred
   * fingerprint check is needed.
   *
   * @return captured fingerprint, or {@code null}
   */
  public byte[] getFingerprint() {
    return fingerprint;
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return new X509Certificate[0];
  }
}
