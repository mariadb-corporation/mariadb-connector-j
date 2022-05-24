// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.result;

import java.sql.SQLException;
import java.sql.Types;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.Column;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.util.constants.ColumnFlags;

/** Result-set metadata */
public class ResultSetMetaData implements java.sql.ResultSetMetaData {

  private final ExceptionFactory exceptionFactory;
  private final Column[] fieldPackets;
  private final Configuration conf;
  private final boolean forceAlias;

  /**
   * Constructor.
   *
   * @param exceptionFactory default exception handler
   * @param fieldPackets column informations
   * @param conf connection options
   * @param forceAlias force table and column name alias as original data
   */
  public ResultSetMetaData(
      final ExceptionFactory exceptionFactory,
      final Column[] fieldPackets,
      final Configuration conf,
      final boolean forceAlias) {
    this.exceptionFactory = exceptionFactory;
    this.fieldPackets = fieldPackets;
    this.conf = conf;
    this.forceAlias = forceAlias;
  }

  /**
   * Returns the number of columns in this <code>ResultSet</code> object.
   *
   * @return the number of columns
   */
  public int getColumnCount() {
    return fieldPackets.length;
  }

  /**
   * Indicates whether the designated column is automatically numbered.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return <code>true</code> if so; <code>false</code> otherwise
   * @throws SQLException if a database access error occurs
   */
  public boolean isAutoIncrement(final int column) throws SQLException {
    return (getColumn(column).getFlags() & ColumnFlags.AUTO_INCREMENT) != 0;
  }

  /**
   * Indicates whether a column's case matters.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return <code>true</code> if so; <code>false</code> otherwise
   */
  public boolean isCaseSensitive(final int column) {
    return true;
  }

  /**
   * Indicates whether the designated column can be used in a where clause.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return <code>true</code> if so; <code>false</code> otherwise
   */
  public boolean isSearchable(final int column) {
    return true;
  }

  /**
   * Indicates whether the designated column is a cash value.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return <code>true</code> if so; <code>false</code> otherwise
   */
  public boolean isCurrency(final int column) {
    return false;
  }

  /**
   * Indicates the nullability of values in the designated column.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return the nullability status of the given column; one of <code>columnNoNulls</code>, <code>
   *     columnNullable</code> or <code>columnNullableUnknown</code>
   * @throws SQLException if a database access error occurs
   */
  public int isNullable(final int column) throws SQLException {
    if ((getColumn(column).getFlags() & ColumnFlags.NOT_NULL) == 0) {
      return java.sql.ResultSetMetaData.columnNullable;
    } else {
      return java.sql.ResultSetMetaData.columnNoNulls;
    }
  }

  /**
   * Indicates whether values in the designated column are signed numbers.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return <code>true</code> if so; <code>false</code> otherwise
   * @throws SQLException if a database access error occurs
   */
  public boolean isSigned(int column) throws SQLException {
    return getColumn(column).isSigned();
  }

  /**
   * Indicates the designated column's normal maximum width in characters.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return the normal maximum number of characters allowed as the width of the designated column
   * @throws SQLException if a database access error occurs
   */
  public int getColumnDisplaySize(final int column) throws SQLException {
    return getColumn(column).getDisplaySize();
  }

  /**
   * Gets the designated column's suggested title for use in printouts and displays. The suggested
   * title is usually specified by the SQL <code>AS</code> clause. If an SQL <code>AS</code> is not
   * specified, the value returned from <code>getColumnLabel</code> will be the same as the value
   * returned by the <code>getColumnName</code> method.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return the suggested column title
   * @throws SQLException if a database access error occurs
   */
  public String getColumnLabel(final int column) throws SQLException {
    return getColumn(column).getColumnAlias();
  }

  /**
   * Get the designated column's name.
   *
   * @param idx the first column is 1, the second is 2, ...
   * @return column name
   * @throws SQLException if a database access error occurs
   */
  public String getColumnName(final int idx) throws SQLException {
    Column column = getColumn(idx);
    String columnName = column.getColumnName();
    if ("".equals(columnName) || forceAlias) {
      return column.getColumnAlias();
    }
    return columnName;
  }

  /**
   * Get the designated column's table's schema.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return schema name or "" if not applicable
   * @throws SQLException if a database access error occurs
   */
  public String getCatalogName(int column) throws SQLException {
    return getColumn(column).getSchema();
  }

  /**
   * Get the designated column's specified column size. For numeric data, this is the maximum
   * precision. For character data, this is the length in characters. For datetime datatypes, this
   * is the length in characters of the String representation (assuming the maximum allowed
   * precision of the fractional seconds component). For binary data, this is the length in bytes.
   * For the ROWID datatype, this is the length in bytes. 0 is returned for data types where the
   * column size is not applicable.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return precision
   * @throws SQLException if a database access error occurs
   */
  public int getPrecision(final int column) throws SQLException {
    return getColumn(column).getPrecision();
  }

  /**
   * Gets the designated column's number of digits to right of the decimal point. 0 is returned for
   * data types where the scale is not applicable.
   *
   * @param index the first column is 1, the second is 2, ...
   * @return scale
   * @throws SQLException if a database access error occurs
   */
  public int getScale(final int index) throws SQLException {
    return getColumn(index).getDecimals();
  }

  /**
   * Gets the designated column's table name.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return table name or "" if not applicable
   * @throws SQLException if a database access error occurs
   */
  public String getTableName(final int column) throws SQLException {
    if (forceAlias) {
      return getColumn(column).getTableAlias();
    }

    if (conf.blankTableNameMeta()) {
      return "";
    }

    return getColumn(column).getTable();
  }

  public String getSchemaName(int column) {
    return "";
  }

  /**
   * Retrieves the designated column's SQL type.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return SQL type from java.sql.Types
   * @throws SQLException if a database access error occurs
   * @see Types
   */
  public int getColumnType(final int column) throws SQLException {
    return getColumn(column).getColumnType(conf);
  }

  /**
   * Retrieves the designated column's database-specific type name.
   *
   * @param index the first column is 1, the second is 2, ...
   * @return type name used by the database. If the column type is a user-defined type, then a
   *     fully-qualified type name is returned.
   * @throws SQLException if a database access error occurs
   */
  public String getColumnTypeName(final int index) throws SQLException {
    return getColumn(index).getColumnTypeName(conf);
  }

  /**
   * Indicates whether the designated column is definitely not writable.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return <code>true</code> if so; <code>false</code> otherwise
   * @throws SQLException if a database access error occurs or in case of wrong index
   */
  public boolean isReadOnly(final int column) throws SQLException {
    Column ci = getColumn(column);
    return ci.getColumnName().isEmpty();
  }

  /**
   * Indicates whether it is possible for writing on the designated column to succeed.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return <code>true</code> if so; <code>false</code> otherwise
   * @throws SQLException if a database access error occurs or in case of wrong index
   */
  public boolean isWritable(final int column) throws SQLException {
    return !isReadOnly(column);
  }

  /**
   * Indicates whether writing on the designated column will definitely succeed.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return <code>true</code> if so; <code>false</code> otherwise
   * @throws SQLException if a database access error occurs or in case of wrong index
   */
  public boolean isDefinitelyWritable(final int column) throws SQLException {
    return !isReadOnly(column);
  }

  /**
   * Returns the fully-qualified name of the Java class whose instances are manufactured if the
   * method <code>ResultSet.getObject</code> is called to retrieve a value from the column. <code>
   * ResultSet.getObject</code> may return a subclass of the class returned by this method.
   *
   * @param column the first column is 1, the second is 2, ...
   * @return the fully-qualified name of the class in the Java programming language that would be
   *     used by the method <code>ResultSet.getObject</code> to retrieve the value in the specified
   *     column. This is the class name used for custom mapping.
   * @throws SQLException if a database access error occurs
   */
  public String getColumnClassName(int column) throws SQLException {
    return getColumn(column).getDefaultCodec(conf).className();
  }

  private Column getColumn(int column) throws SQLException {
    if (column >= 1 && column <= fieldPackets.length) {
      return fieldPackets[column - 1];
    }
    throw exceptionFactory.create(String.format("wrong column index %s", column));
  }

  /**
   * Returns an object that implements the given interface to allow access to non-standard methods,
   * or standard methods not exposed by the proxy. <br>
   * If the receiver implements the interface then the result is the receiver or a proxy for the
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
  public <T> T unwrap(final Class<T> iface) throws SQLException {
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
  public boolean isWrapperFor(final Class<?> iface) {
    return iface.isInstance(this);
  }
}
