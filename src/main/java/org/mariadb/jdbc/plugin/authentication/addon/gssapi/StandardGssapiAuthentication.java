// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin.authentication.addon.gssapi;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.util.ThreadUtils;

/**
 * GSSAPI authentication using the standard Java GSS-API (JGSS).
 *
 * <p>Two credential sources are supported, chosen by the JDK property {@code
 * sun.security.jgss.native}, that the connector never sets itself:
 *
 * <ul>
 *   <li><b>{@code -Dsun.security.jgss.native=true}</b> (recommended): JGSS delegates to the
 *       operating system GSS-API, SSPI on Windows (JDK 12+, 8u371+, 11.0.20+), libgssapi_krb5 on
 *       Linux, GSS.framework on macOS. Credentials are the ones of the OS session (logged-on
 *       Windows user, {@code kinit} credential cache), permitting single sign-on without any
 *       third-party library or JAAS configuration. The JDK reads this property once, at first
 *       GSS-API use, so it must be set on the command line or before any Kerberos operation.
 *   <li><b>property not set</b>: pure-Java Kerberos implementation. The connector logs in through
 *       JAAS using the "Krb5ConnectorContext" entry of the JAAS configuration if the application
 *       provides one (system property {@code java.security.auth.login.config}), or a default {@code
 *       Krb5LoginModule} reading the ticket cache otherwise. On Windows, the ticket cache is only
 *       readable by Java after {@code kinit}, or with the AllowTGTSessionKey registry key.
 * </ul>
 */
public class StandardGssapiAuthentication {

  /** JDK property enabling the native GSS-API bridge */
  public static final String JGSS_NATIVE_PROPERTY = "sun.security.jgss.native";

  /** JAAS login configuration entry name */
  public static final String JAAS_ENTRY_NAME = "Krb5ConnectorContext";

  /** Kerberos v5 mechanism OID */
  static final String KRB5_MECHANISM = "1.2.840.113554.1.2.2";

  /** SPNEGO mechanism OID (SSPI "Negotiate" package) */
  static final String SPNEGO_MECHANISM = "1.3.6.1.5.5.2";

  static final String NATIVE_HINT =
      " (pure-Java Kerberos implementation is used: for single sign-on with operating system"
          + " credentials, start the JVM with -D"
          + JGSS_NATIVE_PROPERTY
          + "=true)";

  /** Default JAAS configuration: Kerberos login from the ticket cache, without prompting */
  private static final Configuration DEFAULT_JAAS_CONFIGURATION =
      new Configuration() {
        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
          Map<String, String> options = new HashMap<>();
          options.put("useTicketCache", "true");
          options.put("renewTGT", "true");
          options.put("doNotPrompt", "true");
          return new AppConfigurationEntry[] {
            new AppConfigurationEntry(
                "com.sun.security.auth.module.Krb5LoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                options)
          };
        }
      };

  /**
   * Process GSS plugin authentication.
   *
   * @param out out stream
   * @param in in stream
   * @param servicePrincipalName service principal name
   * @param mechanisms gssapi mechanism announced by server ("Kerberos" or "Negotiate")
   * @throws IOException if socket error
   * @throws SQLException if authentication fails
   */
  public void authenticate(
      final Writer out, final Reader in, final String servicePrincipalName, final String mechanisms)
      throws SQLException, IOException {

    if ("".equals(servicePrincipalName)) {
      throw new SQLException(
          "No principal name defined on server. Please set server variable"
              + " \"gssapi-principal-name\" or set option \"servicePrincipalName\"",
          "28000");
    }

    if (Boolean.getBoolean(JGSS_NATIVE_PROPERTY)) {
      // native GSS-API bridge: credentials of the OS session
      try {
        exchangeTokens(out, in, servicePrincipalName, mechanisms);
      } catch (GSSException e) {
        throw new SQLException(
            "GSS-API authentication exception: " + e.getMessage(), "28000", 1045, e);
      }
      return;
    }

    // pure-Java implementation: credentials from a JAAS login
    final Subject subject;
    try {
      LoginContext loginContext = createLoginContext();
      loginContext.login();
      subject = loginContext.getSubject();
    } catch (LoginException e) {
      throw new SQLException(
          "GSS-API authentication exception: JAAS login failed: " + e.getMessage() + NATIVE_HINT,
          "28000",
          1045,
          e);
    }
    if (subject == null || subject.getPrincipals().isEmpty()) {
      throw new SQLException(
          "GSS-API authentication exception: no Kerberos credentials found" + NATIVE_HINT,
          "28000",
          1045);
    }

    try {
      ThreadUtils.callAs(
          subject,
          () ->
              (PrivilegedExceptionAction<Void>)
                  () -> {
                    exchangeTokens(out, in, servicePrincipalName, mechanisms);
                    return null;
                  });
    } catch (Exception e) {
      Throwable cause = e;
      while (cause.getCause() != null && !(cause instanceof GSSException)) {
        cause = cause.getCause();
      }
      if (cause instanceof IOException exception) {
        throw exception;
      }
      throw new SQLException(
          "GSS-API authentication exception: " + cause.getMessage() + NATIVE_HINT,
          "28000",
          1045,
          cause);
    }
  }

  /**
   * Create the JAAS login context: uses the application "Krb5ConnectorContext" configuration entry
   * if any, the default ticket-cache configuration otherwise.
   *
   * @return login context
   * @throws LoginException if login context cannot be created
   */
  static LoginContext createLoginContext() throws LoginException {
    if (hasApplicationJaasEntry()) {
      return new LoginContext(JAAS_ENTRY_NAME);
    }
    return new LoginContext(JAAS_ENTRY_NAME, null, null, DEFAULT_JAAS_CONFIGURATION);
  }

  /**
   * Indicate if the application JAAS configuration provides a "Krb5ConnectorContext" entry.
   *
   * @return true if application configured the connector JAAS entry
   */
  static boolean hasApplicationJaasEntry() {
    if (System.getProperty("java.security.auth.login.config") == null) {
      return false;
    }
    try {
      return Configuration.getConfiguration().getAppConfigurationEntry(JAAS_ENTRY_NAME) != null;
    } catch (Throwable t) {
      return false;
    }
  }

  private void exchangeTokens(Writer out, Reader in, String servicePrincipalName, String mechanisms)
      throws GSSException, IOException {
    GSSManager manager = GSSManager.getInstance();
    Oid mechanism = selectMechanism(manager.getMechs(), mechanisms);
    GSSName peerName = manager.createName(servicePrincipalName, GSSName.NT_USER_NAME);
    GSSContext context =
        manager.createContext(peerName, mechanism, null, GSSContext.DEFAULT_LIFETIME);
    try {
      context.requestMutualAuth(true);

      byte[] outToken = context.initSecContext(new byte[0], 0, 0);
      while (true) {
        // Send a token to the peer if one was generated by initSecContext
        if (outToken != null) {
          out.writeBytes(outToken);
          out.flush();
        }
        if (context.isEstablished()) {
          break;
        }
        ReadableByteBuf buf = in.readReusablePacket();

        // server cannot allow plugin data packet to start with 0, 255 or 254,
        // as connectors would treat it as an OK, Error or authentication switch packet
        // server then these bytes with 0x001. Consequently, it escaped 0x01 byte too.
        if (buf.getByte() == 0x01) buf.skip();

        byte[] inToken = new byte[buf.readableBytes()];
        buf.readBytes(inToken);
        outToken = context.initSecContext(inToken, 0, inToken.length);
      }
    } finally {
      try {
        context.dispose();
      } catch (GSSException e) {
        // eat
      }
    }
  }

  /**
   * Select the GSS mechanism according to the one announced by server: SPNEGO when the server uses
   * the SSPI "Negotiate" package (permitting NTLM fallback) and the GSS implementation supports it,
   * Kerberos otherwise.
   *
   * @param supportedMechs mechanisms supported by GSS manager
   * @param mechanisms mechanism announced by server
   * @return mechanism OID
   * @throws GSSException if OID is invalid (cannot happen)
   */
  static Oid selectMechanism(Oid[] supportedMechs, String mechanisms) throws GSSException {
    if ("Negotiate".equalsIgnoreCase(mechanisms) && supportedMechs != null) {
      Oid spnego = new Oid(SPNEGO_MECHANISM);
      for (Oid supported : supportedMechs) {
        if (spnego.equals(supported)) {
          return spnego;
        }
      }
    }
    return new Oid(KRB5_MECHANISM);
  }
}
