// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin.authentication.addon.gssapi;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.SetSystemProperty;

/** GSSAPI authentication (CONJ-1354) */
public class StandardGssapiAuthenticationTest {

  private static final String SPN = "mariadb/db.example.com@EXAMPLE.COM";

  @Test
  @SetSystemProperty(key = StandardGssapiAuthentication.JGSS_NATIVE_PROPERTY, value = "true")
  public void emptyPrincipal() {
    SQLException e =
        assertThrows(
            SQLException.class,
            () -> new StandardGssapiAuthentication().authenticate(null, null, "", "Kerberos"));
    assertTrue(e.getMessage().contains("No principal name defined on server"));
    assertEquals("28000", e.getSQLState());
  }

  @Test
  @SetSystemProperty(key = StandardGssapiAuthentication.JGSS_NATIVE_PROPERTY, value = "true")
  public void nativeBridgeWithoutCredentials() {
    // without any kerberos credential, initSecContext fails before any packet is sent
    SQLException e =
        assertThrows(
            SQLException.class,
            () -> new StandardGssapiAuthentication().authenticate(null, null, SPN, "Kerberos"));
    assertTrue(e.getMessage().startsWith("GSS-API authentication exception"), e.getMessage());
    assertFalse(e.getMessage().contains(StandardGssapiAuthentication.JGSS_NATIVE_PROPERTY));
    assertEquals("28000", e.getSQLState());
    assertEquals(1045, e.getErrorCode());
    assertInstanceOf(GSSException.class, e.getCause());
  }

  @Test
  @ClearSystemProperty(key = StandardGssapiAuthentication.JGSS_NATIVE_PROPERTY)
  @ClearSystemProperty(key = "java.security.auth.login.config")
  public void pureJavaWithoutCredentials() {
    // JAAS login from ticket cache fails when there is none, with a hint about native bridge
    SQLException e =
        assertThrows(
            SQLException.class,
            () -> new StandardGssapiAuthentication().authenticate(null, null, SPN, "Kerberos"));
    assertTrue(e.getMessage().startsWith("GSS-API authentication exception"), e.getMessage());
    assertTrue(e.getMessage().contains("-Dsun.security.jgss.native=true"), e.getMessage());
    assertEquals("28000", e.getSQLState());
    assertEquals(1045, e.getErrorCode());
  }

  @Test
  @ClearSystemProperty(key = "java.security.auth.login.config")
  public void defaultJaasConfiguration() throws Exception {
    assertFalse(StandardGssapiAuthentication.hasApplicationJaasEntry());
    // login context is created from the built-in configuration, no system property needed
    assertNotNull(StandardGssapiAuthentication.createLoginContext());
    assertNull(System.getProperty("java.security.auth.login.config"));
  }

  @Test
  public void applicationJaasConfiguration(@TempDir Path tempDir) throws Exception {
    Path conf = tempDir.resolve("jaas.conf");
    Files.write(
        conf,
        (StandardGssapiAuthentication.JAAS_ENTRY_NAME
                + " { com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true"
                + " keyTab=\"/etc/app.keytab\" principal=\"app@EXAMPLE.COM\"; };\n")
            .getBytes(StandardCharsets.UTF_8));
    String previous = System.getProperty("java.security.auth.login.config");
    try {
      System.setProperty("java.security.auth.login.config", conf.toString());
      Configuration.getConfiguration().refresh();
      assertTrue(StandardGssapiAuthentication.hasApplicationJaasEntry());
      AppConfigurationEntry[] entries =
          Configuration.getConfiguration()
              .getAppConfigurationEntry(StandardGssapiAuthentication.JAAS_ENTRY_NAME);
      assertEquals("/etc/app.keytab", entries[0].getOptions().get("keyTab"));
      assertNotNull(StandardGssapiAuthentication.createLoginContext());
    } finally {
      if (previous == null) {
        System.clearProperty("java.security.auth.login.config");
      } else {
        System.setProperty("java.security.auth.login.config", previous);
      }
      Configuration.getConfiguration().refresh();
    }
  }

  @Test
  public void mechanismSelection() throws Exception {
    Oid krb5 = new Oid(StandardGssapiAuthentication.KRB5_MECHANISM);
    Oid spnego = new Oid(StandardGssapiAuthentication.SPNEGO_MECHANISM);
    Oid[] both = new Oid[] {krb5, spnego};
    Oid[] krb5Only = new Oid[] {krb5};

    assertEquals(krb5, StandardGssapiAuthentication.selectMechanism(both, "Kerberos"));
    assertEquals(krb5, StandardGssapiAuthentication.selectMechanism(both, ""));
    assertEquals(krb5, StandardGssapiAuthentication.selectMechanism(both, null));
    assertEquals(spnego, StandardGssapiAuthentication.selectMechanism(both, "Negotiate"));
    assertEquals(spnego, StandardGssapiAuthentication.selectMechanism(both, "negotiate"));
    assertEquals(krb5, StandardGssapiAuthentication.selectMechanism(krb5Only, "Negotiate"));
    assertEquals(krb5, StandardGssapiAuthentication.selectMechanism(null, "Negotiate"));
  }
}
