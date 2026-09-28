// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.client.tls;

import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * TrustManager that defers server-identity validation to the authentication phase.
 *
 * <p>It delegates the standard certificate-chain check, and the endpoint identification (hostname
 * verification) that JSSE performs when {@code SSLParameters.setEndpointIdentificationAlgorithm} is
 * set, to a real {@link X509TrustManager}. When the chain cannot be validated against the trust
 * store, typically because the server presented a self-signed certificate it generated on the fly
 * (MariaDB does this when TLS is enabled but no server certificate is configured), this manager
 * does not reject the connection. Instead, it records the leaf certificate fingerprint and lets the
 * handshake proceed, so the server's identity can be validated later, during authentication.
 *
 * <p>Identity is then proven cryptographically by the authentication plugin, which binds the
 * session to this exact certificate (password + scramble + certificate fingerprint). A
 * man-in-the-middle presenting a different certificate produces a different fingerprint, so
 * authentication fails: the fingerprint is what actually validates the peer, not the (self-signed)
 * certificate itself, and hostname verification is not needed on that path.
 *
 * <p>When the chain does validate, the delegate's verdict is final: a hostname mismatch is
 * rejected. Expired or not-yet-valid certificates are rejected outright on both paths. This manager
 * must only be used on connection paths that go on to perform fingerprint-based authentication.
 *
 * <p>A new instance is created per connection, wrapping a shared (cached) delegate, so the captured
 * fingerprint is simply per-connection instance state, read afterward via {@link
 * #getFingerprint()}.
 */
public class MariaDbX509DeferredIdentityTrustManager extends X509ExtendedTrustManager {

  private final X509TrustManager internal;
  private byte[] fingerprint = null;

  /**
   * Wraps a standard {@link X509TrustManager}, capturing the server certificate fingerprint when
   * the chain cannot be validated normally so that identity can be verified later, during
   * authentication.
   *
   * @param javaTrustManager real trust manager, expected to be a {@link X509ExtendedTrustManager}
   *     (the JSSE default) for endpoint identification to be performed
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
  public void checkClientTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    internal.checkClientTrusted(chain, authType);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
      throws CertificateException {
    if (internal instanceof X509ExtendedTrustManager extended) {
      extended.checkClientTrusted(chain, authType, socket);
    } else {
      internal.checkClientTrusted(chain, authType);
    }
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
      throws CertificateException {
    if (internal instanceof X509ExtendedTrustManager extended) {
      extended.checkClientTrusted(chain, authType, engine);
    } else {
      internal.checkClientTrusted(chain, authType);
    }
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    try {
      internal.checkServerTrusted(chain, authType);
    } catch (CertificateException e) {
      deferIdentity(chain, e);
    }
  }

  /** Chain validation plus endpoint identification, when the socket parameters ask for it. */
  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
      throws CertificateException {
    if (!(internal instanceof X509ExtendedTrustManager extended)) {
      checkServerTrusted(chain, authType);
      return;
    }
    try {
      extended.checkServerTrusted(chain, authType, socket);
    } catch (CertificateException e) {
      rejectIdentityFailure(chain, authType, e);
      deferIdentity(chain, e);
    }
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
      throws CertificateException {
    if (!(internal instanceof X509ExtendedTrustManager extended)) {
      checkServerTrusted(chain, authType);
      return;
    }
    try {
      extended.checkServerTrusted(chain, authType, engine);
    } catch (CertificateException e) {
      rejectIdentityFailure(chain, authType, e);
      deferIdentity(chain, e);
    }
  }

  /**
   * The extended check fails for an invalid chain or for a hostname mismatch. Only the former can
   * be deferred: when the chain alone validates, the failure is the identity check and is final.
   */
  private void rejectIdentityFailure(
      X509Certificate[] chain, String authType, CertificateException e)
      throws CertificateException {
    try {
      internal.checkServerTrusted(chain, authType);
    } catch (CertificateException chainFailure) {
      return; // chain is invalid: identity will be validated during authentication
    }
    throw e;
  }

  private void deferIdentity(X509Certificate[] chain, CertificateException e)
      throws CertificateException {
    if (chain == null || chain.length < 1) throw e;
    // The JSSE validator usually surfaces an expired/not-yet-valid certificate wrapped in a
    // generic CertificateException, so catching CertificateExpiredException is unreliable. Check
    // validity explicitly: this throws CertificateExpired/NotYetValidException and rejects the
    // connection, even on the deferred-validation path.
    chain[0].checkValidity();
    try {
      fingerprint = getThumbprint(chain[0]);
    } catch (NoSuchAlgorithmException | CertificateEncodingException ex) {
      throw e;
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
