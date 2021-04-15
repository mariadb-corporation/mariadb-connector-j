package org.mariadb.jdbc.unit.client.tls;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.CertificateException;
import javax.security.auth.x500.X500Principal;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.tls.MariaDbX509KeyManager;

public class MariaDbX509KeyManagerTest {

  @Test
  public void check() throws Exception {
    MariaDbX509KeyManager keyMger = get();
    String[] aliases = keyMger.getClientAliases("RSA", null);
    assertEquals(1, aliases.length);
    assertEquals("mysqlalias", aliases[0]);

    assertNull(
        keyMger.getClientAliases(
            "RSA", new Principal[] {new X500Principal("CN=Android Debug,O=Android,C=US")}));
    aliases =
        keyMger.getClientAliases(
            "RSA",
            new Principal[] {
              new X500Principal("EMAILADDRESS=X, OU=X, CN=ca.example.com, L=X, O=X, ST=X, C=XX")
            });
    assertEquals(1, aliases.length);
    assertEquals("mysqlalias", aliases[0]);

    assertNull(keyMger.getPrivateKey("wrong"));
    assertNotNull(keyMger.getPrivateKey("mysqlalias"));

    assertNull(keyMger.getCertificateChain("wrong"));
    assertNotNull(keyMger.getCertificateChain("mysqlalias"));

    assertEquals(
        "mysqlalias",
        keyMger.chooseEngineClientAlias(
            new String[] {"RSA"},
            new Principal[] {
              new X500Principal("EMAILADDRESS=X, OU=X, CN=ca.example.com, L=X, O=X, ST=X, C=XX")
            },
            null));
    assertNull(
        keyMger.chooseEngineClientAlias(
            new String[] {},
            new Principal[] {
              new X500Principal("EMAILADDRESS=X, OU=X, CN=ca.example.com, L=X, O=X, ST=X, C=XX")
            },
            null));
    assertNull(
        keyMger.chooseEngineClientAlias(
            null,
            new Principal[] {
              new X500Principal("EMAILADDRESS=X, OU=X, CN=ca.example.com, L=X, O=X, ST=X, C=XX")
            },
            null));
    assertNull(keyMger.getServerAliases("RSA", null));
    assertNull(keyMger.chooseServerAlias("RSA", null, null));
    assertNull(keyMger.chooseEngineServerAlias("RSA", null, null));
  }

  private MariaDbX509KeyManager get()
      throws KeyStoreException, CertificateException, IOException, NoSuchAlgorithmException {
    try (InputStream inStream =
        MariaDbX509KeyManagerTest.class
            .getClassLoader()
            .getResourceAsStream("testclient-keystore.p12")) {
      assertNotNull(inStream);
      char[] keyStorePasswordChars = "kspass".toCharArray();
      KeyStore ks = KeyStore.getInstance("PKCS12");
      ks.load(inStream, keyStorePasswordChars);
      return new MariaDbX509KeyManager(ks, keyStorePasswordChars);
    }
  }
}
