// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.plugin;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.tls.MariaDbX509DeferredIdentityTrustManager;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.plugin.tls.main.DefaultTlsSocketPlugin;

class DefaultTlsSocketPluginTest {

  // Self-signed CA certificate, used to exercise the file-based serverSslCert trust path.
  private static final String PEM =
      "-----BEGIN CERTIFICATE-----\n"
          + "MIIDETCCAfmgAwIBAgIUFsbMgg3jv7RnEEXL2foIbYBjWU8wDQYJKoZIhvcNAQEL\n"
          + "BQAwGDEWMBQGA1UEAwwNbWFyaWFkYi1iZW5jaDAeFw0yNjA2MDIxMzE3NDlaFw0y\n"
          + "NzA2MDIxMzE3NDlaMBgxFjAUBgNVBAMMDW1hcmlhZGItYmVuY2gwggEiMA0GCSqG\n"
          + "SIb3DQEBAQUAA4IBDwAwggEKAoIBAQCPmKaBPjmJrM03E/vzdZrCjy5oPV/vbHNk\n"
          + "33btSv3qu/6dRdiPF2Wo9nbF/cieV7ErbIobJCq+eqnZ18Pj6zCdKROqQxIHbwLe\n"
          + "dJoGja5PLT4gXTLYoiXNvly+swR2139XHfC9xhXdmuN3SwHa2iBeEmjjYsjP9sr5\n"
          + "DbqVaarNugFcayxMXlqsyJ5CoQINcv9/IwZC2+BWx383YbWCiVTle5vNzcZsH9wL\n"
          + "qIcNVs+Q1fS1Aa5N2c6jcQ8/OeEctkcNyoCIovOvCNHfzJPxYHBkt8AjnvTL8MAZ\n"
          + "V2L4OLWnwqXMVhfORRDI+93Ka/wATWCQ1nRCVYN19StkYXVV25xjAgMBAAGjUzBR\n"
          + "MB0GA1UdDgQWBBTsNHsTPQEa3uDaExoi1kxx+ce/6jAfBgNVHSMEGDAWgBTsNHsT\n"
          + "PQEa3uDaExoi1kxx+ce/6jAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUA\n"
          + "A4IBAQBbtxszP6CCNpFFp6Gc51mJR08telWmd3QrqZVYzuLo2ahsDqxgBnhuK6Hs\n"
          + "lpXMGJnASUVdobpcfzOrxVNt4gxWFtwOrcoD+wi+T5hXBK2ZhisNLTJoB0o0SlUK\n"
          + "BS24NJj2QSGLqXoRQ6iIZNNRp4wxgkC0ymvZyZyTw8eL//fQN2by9GkTQ6krCF7X\n"
          + "ow2tSpMayJvGidnW1cow5JKdUjaf+lHaelJZVAyugweSCsZKAOIiTpyV5ETAlr5y\n"
          + "tU9Wr7rd0xm9Us+oSlxgjGqYQj3CKG2D1FxJu+77MTi7fZGjMdHGEFsi7FqkVq8a\n"
          + "QQAlP37ieG6K7fuMN++XIAJXlwLf\n"
          + "-----END CERTIFICATE-----\n";

  private static TrustManager firstTrustManager(DefaultTlsSocketPlugin plugin, Configuration conf)
      throws SQLException {
    HostAddress hostAddress = conf.addresses().get(0);
    return plugin.getTrustManager(conf, new ExceptionFactory(conf, hostAddress), hostAddress)[0];
  }

  private static SSLSocketFactory socketFactory(DefaultTlsSocketPlugin plugin, Configuration conf)
      throws SQLException {
    HostAddress hostAddress = conf.addresses().get(0);
    return plugin.getSocketFactory(conf, new ExceptionFactory(conf, hostAddress), hostAddress);
  }

  @Test
  void deferredIdentityWrapperIsPerConnection() throws SQLException {
    // The deferred-identity (system-trust-store) path must hand each connection its own
    // MariaDbX509DeferredIdentityTrustManager so the captured fingerprint is per-connection state.
    DefaultTlsSocketPlugin plugin = new DefaultTlsSocketPlugin();
    Configuration conf =
        Configuration.parse("jdbc:mariadb://localhost:3306/test?sslMode=verify_full");

    TrustManager first = firstTrustManager(plugin, conf);
    TrustManager second = firstTrustManager(plugin, conf);

    assertTrue(first instanceof MariaDbX509DeferredIdentityTrustManager);
    assertNotSame(
        first, second, "each connection must get its own deferred-identity trust manager");
  }

  @Test
  void serverSslCertReusesCachedBase(@TempDir Path tmp) throws SQLException, IOException {
    Path cert = tmp.resolve("ca.pem");
    Files.write(cert, PEM.getBytes(StandardCharsets.UTF_8));

    DefaultTlsSocketPlugin plugin = new DefaultTlsSocketPlugin();
    Configuration conf =
        Configuration.parse(
            "jdbc:mariadb://localhost:3306/test?sslMode=verify_ca&serverSslCert=" + cert);

    // serverSslCert validates normally (no deferred identity), so the cached base is returned as-is
    TrustManager first = firstTrustManager(plugin, conf);
    assertSame(
        first, firstTrustManager(plugin, conf), "same configuration must reuse the cached base");
  }

  @Test
  void changedCertificateFileInvalidatesCache(@TempDir Path tmp) throws SQLException, IOException {
    Path cert = tmp.resolve("ca.pem");
    Files.write(cert, PEM.getBytes(StandardCharsets.UTF_8));

    DefaultTlsSocketPlugin plugin = new DefaultTlsSocketPlugin();
    Configuration conf =
        Configuration.parse(
            "jdbc:mariadb://localhost:3306/test?sslMode=verify_ca&serverSslCert=" + cert);

    TrustManager first = firstTrustManager(plugin, conf);
    assertSame(first, firstTrustManager(plugin, conf), "unchanged file must stay cached");

    // simulate an on-disk rotation: the freshness token (last-modified) must change the cache key
    cert.toFile().setLastModified(cert.toFile().lastModified() - 60_000L);

    assertNotSame(
        first, firstTrustManager(plugin, conf), "replacing the certificate file must rebuild it");
  }

  @Test
  void differentSslModeReturnsDifferentTrustManager() throws SQLException {
    DefaultTlsSocketPlugin plugin = new DefaultTlsSocketPlugin();
    TrustManager verify =
        firstTrustManager(
            plugin, Configuration.parse("jdbc:mariadb://localhost:3306/test?sslMode=verify_ca"));
    TrustManager trust =
        firstTrustManager(
            plugin, Configuration.parse("jdbc:mariadb://localhost:3306/test?sslMode=trust"));

    assertNotSame(verify, trust, "TRUST and VERIFY use different trust managers");
  }

  @Test
  void cacheCanBeDisabledViaOption(@TempDir Path tmp) throws SQLException, IOException {
    Path cert = tmp.resolve("ca.pem");
    Files.write(cert, PEM.getBytes(StandardCharsets.UTF_8));

    DefaultTlsSocketPlugin plugin = new DefaultTlsSocketPlugin();
    // disableSslContextCache is a non-mapped option, so each call rebuilds the trust manager
    Configuration conf =
        Configuration.parse(
            "jdbc:mariadb://localhost:3306/test?sslMode=verify_ca&disableSslContextCache=true&serverSslCert="
                + cert);

    assertNotSame(
        firstTrustManager(plugin, conf),
        firstTrustManager(plugin, conf),
        "disabled cache must rebuild the trust manager on each call");
  }

  @Test
  void eachCallReturnsFreshSocketFactory() throws SQLException {
    // The SSLContext/SSLSocketFactory is intentionally NOT cached: a fresh one per connection means
    // its client SSL session cache is single-use, so TLS session resumption cannot occur across
    // connections. (Only the underlying trust/key material is cached.)
    DefaultTlsSocketPlugin plugin = new DefaultTlsSocketPlugin();
    Configuration conf =
        Configuration.parse("jdbc:mariadb://localhost:3306/test?sslMode=verify_full");

    assertNotSame(
        socketFactory(plugin, conf),
        socketFactory(plugin, conf),
        "each connection must get its own SSLSocketFactory (no shared session cache)");
  }
}
