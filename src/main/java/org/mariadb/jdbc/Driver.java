// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import java.util.regex.Pattern;
import org.mariadb.jdbc.client.Client;
import org.mariadb.jdbc.client.impl.MultiPrimaryClient;
import org.mariadb.jdbc.client.impl.MultiPrimaryReplicaClient;
import org.mariadb.jdbc.client.impl.ReplayClient;
import org.mariadb.jdbc.client.impl.StandardClient;
import org.mariadb.jdbc.client.util.ClosableLock;
import org.mariadb.jdbc.export.HaMode;
import org.mariadb.jdbc.pool.Pools;
import org.mariadb.jdbc.util.VersionFactory;

/** MariaDB Driver */
public final class Driver implements java.sql.Driver {
  private static final Pattern identifierPattern =
      Pattern.compile("[0-9a-zA-Z$_\\u0080-\\uFFFF]*", Pattern.UNICODE_CASE);

  static {
    try {
      DriverManager.registerDriver(new Driver());
    } catch (SQLException e) {
      // eat
    }
  }

  /**
   * Connect according to configuration
   *
   * @param configuration configuration
   * @return a Connection
   * @throws SQLException if connect fails
   */
  public static Connection connect(Configuration configuration) throws SQLException {
    ClosableLock lock = new ClosableLock();

    if (configuration.haMode() == HaMode.NONE) {
      ClientInstance<Configuration, HostAddress, ClosableLock, Boolean, Client> clientInstance =
          (configuration.transactionReplay()) ? ReplayClient::new : StandardClient::new;

      if (configuration.addresses().isEmpty())
        throw new SQLException("host, pipe or local socket must be set to connect socket");

      // loop until finding
      SQLException lastException = null;
      for (HostAddress host : configuration.addresses()) {
        try {
          Client client = clientInstance.apply(configuration, host, lock, false);
          return new Connection(configuration, lock, client);
        } catch (SQLException e) {
          lastException = e;
        }
      }
      throw lastException;
    }

    Client client =
        configuration.havePrimaryHostOnly()
            ? new MultiPrimaryClient(configuration, lock)
            : new MultiPrimaryReplicaClient(configuration, lock);
    return new Connection(configuration, lock, client);
  }

  /**
   * Connect to the given connection string.
   *
   * @param url the url to connect to
   * @return a connection
   * @throws SQLException if it is not possible to connect
   */
  public Connection connect(final String url, final Properties props) throws SQLException {
    Configuration configuration = Configuration.parse(url, props);
    if (configuration != null) {
      if (configuration.pool()) {
        return Pools.retrievePool(configuration).getPoolConnection().getConnection();
      }
      return connect(configuration);
    }
    return null;
  }

  /**
   * returns true if the driver can accept the url.
   *
   * @param url the url to test
   * @return true if the url is valid for this driver
   */
  @Override
  public boolean acceptsURL(String url) {
    return Configuration.acceptsUrl(url);
  }

  /**
   * Get the property info.
   *
   * @param url the url to get properties for
   * @param info the info props
   * @return all possible connector options
   * @throws SQLException if there is a problem getting the property info
   */
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    Configuration conf = Configuration.parse(url, info);
    if (conf == null) {
      return new DriverPropertyInfo[0];
    }

    Properties propDesc = new Properties();
    try (InputStream inputStream =
        Driver.class.getClassLoader().getResourceAsStream("driver.properties")) {
      propDesc.load(inputStream);
    } catch (IOException io) {
      // eat
    }

    List<DriverPropertyInfo> props = new ArrayList<>();
    for (Field field : Configuration.Builder.class.getDeclaredFields()) {
      if (!field.getName().startsWith("_")) {
        try {
          Method getterMethod = Configuration.class.getDeclaredMethod(field.getName());
          Object obj = getterMethod.invoke(conf);
          String value = obj == null ? null : obj.toString();
          DriverPropertyInfo propertyInfo = new DriverPropertyInfo(field.getName(), value);
          propertyInfo.description = value == null ? "" : (String) propDesc.get(field.getName());
          propertyInfo.required = false;
          props.add(propertyInfo);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
          // eat error
        }
      }
    }
    return props.toArray(new DriverPropertyInfo[0]);
  }

  /**
   * gets the major version of the driver.
   *
   * @return the major versions
   */
  public int getMajorVersion() {
    return VersionFactory.getInstance().getMajorVersion();
  }

  /**
   * gets the minor version of the driver.
   *
   * @return the minor version
   */
  public int getMinorVersion() {
    return VersionFactory.getInstance().getMinorVersion();
  }

  /**
   * checks if the driver is jdbc compliant.
   *
   * @return true since the driver is not compliant
   */
  public boolean jdbcCompliant() {
    return true;
  }

  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("Use logging parameters for enabling logging.");
  }

  public static String enquoteIdentifier(String identifier, boolean alwaysQuote)
      throws SQLException {
    int len = identifier.length();

    if (isSimpleIdentifier(identifier)) {
      if (len < 1 || len > 64) {
        throw new SQLException("Invalid identifier length");
      }
      if (alwaysQuote) return "`" + identifier + "`";

      // Identifier names may begin with a numeral, but can't only contain numerals unless quoted.
      for (int i = 0; i < identifier.length(); i++) {
        if (!Character.isDigit(identifier.charAt(i))) {
          return identifier;
        }
      }
      // identifier containing only numerals must be quoted
      return "`" + identifier + "`";
    } else {
      if (identifier.contains("\u0000")) {
        throw new SQLException("Invalid name - containing u0000 character", "42000");
      }

      if (identifier.matches("^`.+`$")) {
        identifier = identifier.substring(1, identifier.length() - 1);
      }
      if (len < 1 || len > 64) {
        throw new SQLException("Invalid identifier length");
      }
      return "`" + identifier.replace("`", "``") + "`";
    }
  }

  /**
   * Enquote String value.
   *
   * @param val string value to enquote
   * @return enquoted string value
   */
  // @Override when not supporting java 8
  public static String enquoteLiteral(String val) {
    return enquoteLiteral(val, false);
  }

  /**
   * Enquote String value, escaping according to the server sql_mode.
   *
   * <p>The single quote is always doubled ({@code ''}), which is safe under every sql_mode. When
   * backslash escaping is enabled (default sql_mode) the backslash and the usual control characters
   * are backslash-escaped too; under {@code NO_BACKSLASH_ESCAPES} the backslash is a literal
   * character, so only the quote is doubled and every other character is left untouched.
   *
   * @param val string value to enquote
   * @param noBackslashEscapes whether the server runs with NO_BACKSLASH_ESCAPES sql_mode
   * @return enquoted string value
   */
  public static String enquoteLiteral(String val, boolean noBackslashEscapes) {
    int len = val.length();

    // fast scan : find the first character requiring escaping
    int i = 0;
    for (; i < len; i++) {
      if (mustEscapeLiteral(val.charAt(i), noBackslashEscapes)) break;
    }
    // nothing to escape : just wrap in quotes, no per-character work
    if (i == len) {
      return "'" + val + "'";
    }

    StringBuilder sb = new StringBuilder(len + 16);
    sb.append('\'').append(val, 0, i);
    if (noBackslashEscapes) {
      // backslash is a literal character : only the quote must be doubled
      for (; i < len; i++) {
        char c = val.charAt(i);
        if (c == '\'') sb.append("''");
        else sb.append(c);
      }
    } else {
      for (; i < len; i++) {
        char c = val.charAt(i);
        switch (c) {
          case '\'':
            sb.append("''"); // double the quote
            break;
          case '\\':
            sb.append("\\\\"); // double the backslash
            break;
          case '"':
            sb.append("\\\"");
            break;
          case 0:
            sb.append("\\0");
            break;
          case '\b':
            sb.append("\\b");
            break;
          case '\n':
            sb.append("\\n");
            break;
          case '\r':
            sb.append("\\r");
            break;
          case '\t':
            sb.append("\\t");
            break;
          case 26:
            sb.append("\\Z");
            break;
          default:
            sb.append(c);
        }
      }
    }
    sb.append('\'');
    return sb.toString();
  }

  private static boolean mustEscapeLiteral(char c, boolean noBackslashEscapes) {
    if (c == '\'') return true;
    if (noBackslashEscapes) return false; // only the quote is special
    return c == '\\' || c == '"' || c == 0 || c == '\b' || c == '\n' || c == '\r' || c == '\t'
        || c == 26;
  }

  /**
   * Retrieves whether identifier is a simple SQL identifier. The first character is an alphabetic
   * character from a through z, or from A through Z The string only contains alphanumeric
   * characters or the characters "_" and "$"
   *
   * @param identifier identifier
   * @return true if identifier doesn't have to be quoted
   * @see <a href="https://mariadb.com/kb/en/library/identifier-names/">mariadb identifier name</a>
   */
  public static boolean isSimpleIdentifier(String identifier) {
    return identifier != null
        && !identifier.isEmpty()
        && identifierPattern.matcher(identifier).matches();
  }

  @FunctionalInterface
  private interface ClientInstance<T, U, V, W, R> {
    R apply(T t, U u, V v, W w) throws SQLException;
  }
}
