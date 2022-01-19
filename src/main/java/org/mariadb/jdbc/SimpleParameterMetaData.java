// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc;

import java.sql.SQLException;
import org.mariadb.jdbc.export.ExceptionFactory;

/** Simple parameter metadata, when the only reliable think is the number of parameter */
public class SimpleParameterMetaData implements java.sql.ParameterMetaData {

  private final int paramCount;
  private final ExceptionFactory exceptionFactory;

  /**
   * Constructor
   *
   * @param exceptionFactory connection exception factory
   * @param paramCount parameter count
   */
  protected SimpleParameterMetaData(ExceptionFactory exceptionFactory, int paramCount) {
    this.exceptionFactory = exceptionFactory;
    this.paramCount = paramCount;
  }

  /**
   * Retrieves the number of parameters in the <code>PreparedStatement</code> object for which this
   * <code>ParameterMetaData</code> object contains information.
   *
   * @return the number of parameters
   */
  @Override
  public int getParameterCount() {
    return paramCount;
  }

  private void checkIndex(int index) throws SQLException {
    if (index < 1 || index > paramCount) {
      throw exceptionFactory.create(
          String.format(
              "Wrong index position. Is %s but must be in 1-%s range", index, paramCount));
    }
  }

  /**
   * Retrieves whether null values are allowed in the designated parameter.
   *
   * @param idx the first parameter is 1, the second is 2, ...
   * @return the nullability status of the given parameter; one of <code>
   *     ParameterMetaData.parameterNoNulls</code>, <code>ParameterMetaData.parameterNullable</code>
   * @throws SQLException if wrong index
   */
  @Override
  public int isNullable(int idx) throws SQLException {
    checkIndex(idx);
    return java.sql.ParameterMetaData.parameterNullable;
  }

  /**
   * Retrieves whether values for the designated parameter can be signed numbers.
   *
   * @param idx the first parameter is 1, the second is 2, ...
   * @return <code>true</code> if so; <code>false</code> otherwise
   * @throws SQLException if wrong index
   */
  @Override
  public boolean isSigned(int idx) throws SQLException {
    checkIndex(idx);
    return true;
  }

  /**
   * Retrieves the designated parameter's specified column size.
   *
   * <p>The returned value represents the maximum column size for the given parameter. For numeric
   * data, this is the maximum precision. For character data, this is the length in characters. For
   * datetime datatypes, this is the length in characters of the String representation (assuming the
   * maximum allowed precision of the fractional seconds component). For binary data, this is the
   * length in bytes. For the ROWID datatype, this is the length in bytes. 0 is returned for data
   * types where the column size is not applicable.
   *
   * @param idx the first parameter is 1, the second is 2, ...
   * @return precision
   * @throws SQLException if wrong index
   */
  @Override
  public int getPrecision(int idx) throws SQLException {
    checkIndex(idx);
    throw exceptionFactory.create("Unknown parameter metadata precision");
  }

  /**
   * Retrieves the designated parameter's number of digits to right of the decimal point. 0 is
   * returned for data types where the scale is not applicable. Parameter type are not sent by
   * server. See * https://jira.mariadb.org/browse/CONJ-568 and
   * https://jira.mariadb.org/browse/MDEV-15031
   *
   * @param idx the first parameter is 1, the second is 2, ...
   * @return scale
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getScale(int idx) throws SQLException {
    checkIndex(idx);
    throw exceptionFactory.create("Unknown parameter metadata scale");
  }

  /**
   * Retrieves the designated parameter's SQL type. Parameter type are not sent by server. See
   * https://jira.mariadb.org/browse/CONJ-568 and https://jira.mariadb.org/browse/MDEV-15031
   *
   * @param idx the first parameter is 1, the second is 2, ...
   * @return SQL types from <code>java.sql.Types</code>
   * @throws SQLException because not supported
   */
  @Override
  public int getParameterType(int idx) throws SQLException {
    checkIndex(idx);
    throw exceptionFactory.create("Getting parameter type metadata is not supported", "0A000", -1);
  }

  /**
   * Retrieves the designated parameter's database-specific type name.
   *
   * @param idx the first parameter is 1, the second is 2, ...
   * @return type the name used by the database. If the parameter type is a user-defined type, then
   *     a fully-qualified type name is returned.
   * @throws SQLException if wrong index
   */
  @Override
  public String getParameterTypeName(int idx) throws SQLException {
    checkIndex(idx);
    throw exceptionFactory.create("Unknown parameter metadata type name");
  }

  /**
   * Retrieves the fully-qualified name of the Java class whose instances should be passed to the
   * method <code>PreparedStatement.setObject</code>.
   *
   * @param idx the first parameter is 1, the second is 2, ...
   * @return the fully-qualified name of the class in the Java programming language that would be
   *     used by the method <code>PreparedStatement.setObject</code> to set the value in the
   *     specified parameter. This is the class name used for custom mapping.
   * @throws SQLException if wrong index
   */
  @Override
  public String getParameterClassName(int idx) throws SQLException {
    checkIndex(idx);
    throw exceptionFactory.create("Unknown parameter metadata class name", "0A000");
  }

  /**
   * Retrieves the designated parameter's mode.
   *
   * @param idx the first parameter is 1, the second is 2, ...
   * @return mode of the parameter; one of <code>ParameterMetaData.parameterModeIn</code>, <code>
   *     ParameterMetaData.parameterModeOut</code>, or <code>ParameterMetaData.parameterModeInOut
   *     </code> <code>ParameterMetaData.parameterModeUnknown</code>.
   */
  @Override
  public int getParameterMode(int idx) throws SQLException {
    checkIndex(idx);
    return java.sql.ParameterMetaData.parameterModeIn;
  }

  /**
   * Returns an object that implements the given interface to allow access to non-standard methods,
   * or standard methods not exposed by the proxy.
   *
   * <p>If the receiver implements the interface then the result is the receiver or a proxy for the
   * receiver. If the receiver is a wrapper and the wrapped object implements the interface then the
   * result is the wrapped object or a proxy for the wrapped object. Otherwise, return the result of
   * calling <code>unwrap</code> recursively on the wrapped object or a proxy for that result. If
   * the receiver is not a wrapper and does not implement the interface, then an <code>SQLException
   * </code> is thrown.
   *
   * @param iface A Class defining an interface that the result must implement.
   * @return an object that implements the interface. Maybe a proxy for the actual implementing
   *     object.
   * @throws SQLException If no object found that implements the interface
   */
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (isWrapperFor(iface)) {
      return iface.cast(this);
    }
    throw new SQLException("The receiver is not a wrapper for " + iface.getName());
  }

  /**
   * Returns true if this either implements the interface argument or is directly or indirectly a
   * wrapper for an object that does. Returns false otherwise. If this implements the interface then
   * return true, else if this is a wrapper then return the result of recursively calling <code>
   * isWrapperFor</code> on the wrapped object. If this does not implement the interface and is not
   * a wrapper, return false. This method should be implemented as a low-cost operation compared to
   * <code>unwrap</code> so that callers can use this method to avoid expensive <code>unwrap</code>
   * calls that may fail. If this method returns true then calling <code>unwrap</code> with the same
   * argument should succeed.
   *
   * @param iface a Class defining an interface.
   * @return true if this implements the interface or directly or indirectly wraps an object that
   *     does.
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }
}
