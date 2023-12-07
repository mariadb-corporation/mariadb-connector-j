// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc;

import java.math.BigDecimal;
import java.sql.*;
import java.util.BitSet;
import java.util.Locale;

public class CallableParameterMetaData implements java.sql.ParameterMetaData {
  private final ResultSet rs;
  private final int parameterCount;
  private final boolean isFunction;

  public CallableParameterMetaData(ResultSet rs, boolean isFunction) throws SQLException {
    this.rs = rs;
    int count = 0;
    while (rs.next()) count++;
    this.parameterCount = count;
    this.isFunction = isFunction;
  }

  /**
   * Retrieves the number of parameters in the <code>PreparedStatement</code> object for which this
   * <code>ParameterMetaData</code> object contains information.
   *
   * @return the number of parameters
   * @since 1.4
   */
  @Override
  public int getParameterCount() {
    return parameterCount;
  }

  /**
   * Retrieves whether null values are allowed in the designated parameter.
   *
   * @param index the first parameter is 1, the second is 2, ...
   * @return the nullability status of the given parameter; one of <code>
   *     ParameterMetaData.parameterNoNulls</code>, <code>ParameterMetaData.parameterNullable</code>
   *     , or <code>ParameterMetaData.parameterNullableUnknown</code>
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int isNullable(int index) throws SQLException {
    setIndex(index);
    return ParameterMetaData.parameterNullableUnknown;
  }

  private void setIndex(int index) throws SQLException {
    if (index < 1 || index > parameterCount) {
      throw new SQLException("invalid parameter index " + index);
    }
    rs.absolute(index);
  }

  /**
   * Retrieves whether values for the designated parameter can be signed numbers.
   *
   * @param index the first parameter is 1, the second is 2, ...
   * @return <code>true</code> if so; <code>false</code> otherwise
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public boolean isSigned(int index) throws SQLException {
    setIndex(index);
    String paramDetail = rs.getString("DTD_IDENTIFIER");
    return !paramDetail.contains(" unsigned");
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
   * @param index the first parameter is 1, the second is 2, ...
   * @return precision
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int getPrecision(int index) throws SQLException {
    setIndex(index);
    int characterMaxLength = rs.getInt("CHARACTER_MAXIMUM_LENGTH");
    int numericPrecision = rs.getInt("NUMERIC_PRECISION");
    return (numericPrecision > 0) ? numericPrecision : characterMaxLength;
  }

  /**
   * Retrieves the designated parameter's number of digits to right of the decimal point. 0 is
   * returned for data types where the scale is not applicable.
   *
   * @param index the first parameter is 1, the second is 2, ...
   * @return scale
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int getScale(int index) throws SQLException {
    setIndex(index);
    return rs.getInt("NUMERIC_SCALE");
  }

  public String getParameterName(int index) throws SQLException {
    setIndex(index);
    return rs.getString("PARAMETER_NAME");
  }

  /**
   * Retrieves the designated parameter's SQL type.
   *
   * @param index the first parameter is 1, the second is 2, ...
   * @return SQL type from <code>java.sql.Types</code>
   * @throws SQLException if a database access error occurs
   * @see Types
   * @since 1.4
   */
  @Override
  public int getParameterType(int index) throws SQLException {
    setIndex(index);
    String str = rs.getString("DATA_TYPE").toUpperCase(Locale.ROOT);
    boolean isBinary = isBinaryCharset();
    switch (str) {
      case "BIT":
        return Types.BIT;
      case "TINYINT":
        return Types.TINYINT;
      case "SMALLINT":
      case "YEAR":
        return Types.SMALLINT;
      case "MEDIUMINT":
      case "INT":
      case "INT24":
      case "INTEGER":
        return Types.INTEGER;
      case "LONG":
      case "BIGINT":
        return Types.BIGINT;
      case "REAL":
      case "DOUBLE":
        return Types.DOUBLE;
      case "FLOAT":
        return Types.FLOAT;
      case "DECIMAL":
      case "NEWDECIMAL":
        return Types.DECIMAL;
      case "CHAR":
        return isBinary ? Types.BINARY : Types.CHAR;
      case "VARCHAR":
      case "ENUM":
      case "SET":
        return isBinary ? Types.VARBINARY : Types.VARCHAR;
      case "DATE":
        return Types.DATE;
      case "TIME":
        return Types.TIME;
      case "TIMESTAMP":
      case "DATETIME":
        return Types.TIMESTAMP;
      case "BINARY":
        return Types.BINARY;
      case "VARBINARY":
        return Types.VARBINARY;
      case "TINYBLOB":
      case "BLOB":
      case "MEDIUMBLOB":
      case "LONGBLOB":
      case "GEOMETRY":
      case "TEXT":
      case "MEDIUMTEXT":
      case "LONGTEXT":
      case "TINYTEXT":
      case "JSON":
        return isBinary ? Types.BLOB : Types.CLOB;
      default:
        return Types.OTHER;
    }
  }

  private boolean isBinaryCharset() throws SQLException {
    String charset = rs.getString("CHARACTER_SET_NAME");
    return charset != null && charset.toUpperCase(Locale.ROOT).equals("BINARY");
  }

  /**
   * Retrieves the designated parameter's database-specific type name.
   *
   * @param index the first parameter is 1, the second is 2, ...
   * @return type the name used by the database. If the parameter type is a user-defined type, then
   *     a fully-qualified type name is returned.
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public String getParameterTypeName(int index) throws SQLException {
    setIndex(index);
    String datatype = rs.getString("DTD_IDENTIFIER").toUpperCase(Locale.ROOT);

    for (int i = 0; i < datatype.length(); i++) {
      if (datatype.charAt(i) == ' ' || datatype.charAt(i) == '(') {
        return datatype.substring(0, i);
      }
    }

    return datatype;
  }

  /**
   * Retrieves the fully-qualified name of the Java class whose instances should be passed to the
   * method <code>PreparedStatement.setObject</code>.
   *
   * @param index the first parameter is 1, the second is 2, ...
   * @return the fully-qualified name of the class in the Java programming language that would be
   *     used by the method <code>PreparedStatement.setObject</code> to set the value in the
   *     specified parameter. This is the class name used for custom mapping.
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public String getParameterClassName(int index) throws SQLException {
    setIndex(index);
    String str = rs.getString("DATA_TYPE").toUpperCase(Locale.ROOT);
    boolean isBinary = isBinaryCharset();
    switch (str) {
      case "BIT":
        return BitSet.class.getName();
      case "TINYINT":
        return byte.class.getName();
      case "SMALLINT":
      case "YEAR":
        return short.class.getName();
      case "MEDIUMINT":
      case "INT":
      case "INTEGER":
        return int.class.getName();
      case "BINARY":
      case "SET":
      case "GEOMETRY":
      case "VARBINARY":
      case "TINYBLOB":
        return byte[].class.getName();
      case "BIGINT":
        return long.class.getName();
      case "FLOAT":
        return float.class.getName();
      case "DECIMAL":
      case "NEWDECIMAL":
        return BigDecimal.class.getName();
      case "REAL":
      case "DOUBLE":
        return double.class.getName();
      case "CHAR":
      case "VARCHAR":
      case "ENUM":
      case "TINYTEXT":
        return isBinary ? byte[].class.getName() : String.class.getName();
      case "TEXT":
      case "MEDIUMTEXT":
      case "LONGTEXT":
      case "JSON":
        return isBinary ? Blob.class.getName() : Clob.class.getName();
      case "DATE":
        return Date.class.getName();
      case "TIME":
        return Time.class.getName();
      case "TIMESTAMP":
      case "DATETIME":
        return Timestamp.class.getName();
      case "BLOB":
      case "MEDIUMBLOB":
      case "LONGBLOB":
        return Blob.class.getName();
      default:
        return Object.class.getName();
    }
  }

  /**
   * Retrieves the designated parameter's mode.
   *
   * @param index the first parameter is 1, the second is 2, ...
   * @return mode of the parameter; one of <code>ParameterMetaData.parameterModeIn</code>, <code>
   *     ParameterMetaData.parameterModeOut</code>, or <code>ParameterMetaData.parameterModeInOut
   *     </code> <code>ParameterMetaData.parameterModeUnknown</code>.
   * @throws SQLException if a database access error occurs
   * @since 1.4
   */
  @Override
  public int getParameterMode(int index) throws SQLException {
    setIndex(index);
    if (isFunction) return ParameterMetaData.parameterModeOut;
    String str = rs.getString("PARAMETER_MODE");
    switch (str) {
      case "IN":
        return ParameterMetaData.parameterModeIn;
      case "OUT":
        return ParameterMetaData.parameterModeOut;
      case "INOUT":
        return ParameterMetaData.parameterModeInOut;
      default:
        return ParameterMetaData.parameterModeUnknown;
    }
  }

  /**
   * Returns an object that implements the given interface to allow access to non-standard methods,
   * or standard methods not exposed by the proxy.
   *
   * <p>If the receiver implements the interface then the result is the receiver or a proxy for the
   * receiver. If the receiver is a wrapper and the wrapped object implements the interface then the
   * result is the wrapped object or a proxy for the wrapped object. Otherwise return the the result
   * of calling <code>unwrap</code> recursively on the wrapped object or a proxy for that result. If
   * the receiver is not a wrapper and does not implement the interface, then an <code>SQLException
   * </code> is thrown.
   *
   * @param iface A Class defining an interface that the result must implement.
   * @return an object that implements the interface. May be a proxy for the actual implementing
   *     object.
   * @throws SQLException If no object found that implements the interface
   * @since 1.6
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
   * @throws SQLException if an error occurs while determining whether this is a wrapper for an
   *     object with the given interface.
   * @since 1.6
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }
}
