package org.mariadb.jdbc.client;

public interface Column {

  /**
   * get column schema
   *
   * @return column schema
   */
  String getSchema();

  /**
   * retrieve table alias if any
   *
   * @return table alias
   */
  String getTableAlias();

  /**
   * retrieve table name if any
   *
   * @return table name
   */
  String getTable();

  /**
   * retrieve column alias if any
   *
   * @return column alias
   */
  String getColumnAlias();

  /**
   * retrieve column name if any
   *
   * @return column name
   */
  String getColumnName();

  /**
   * column maximum length if known
   *
   * @return column maximum length
   */
  long getColumnLength();

  /**
   * server data type
   *
   * @return server data type
   */
  DataType getType();

  /**
   * get number of decimal
   *
   * @return number of decimal
   */
  byte getDecimals();

  /**
   * Is column signed (for number only)
   *
   * @return is signed
   */
  boolean isSigned();

  /**
   * get display size
   *
   * @return display sier
   */
  int getDisplaySize();

  /**
   * Is column a primary key
   *
   * @return is a primary key
   */
  boolean isPrimaryKey();

  /**
   * Column autoincrement
   *
   * @return true if column auto-increment
   */
  boolean isAutoIncrement();

  /**
   * Column has a default value
   *
   * @return indicate if has a default value
   */
  boolean hasDefault();

  /**
   * indicate if column is of binary type. doesn't use flag BINARY filter, because char binary and
   * varchar binary are not binary (handle like string), but have binary flag
   *
   * @return is column type binary
   */
  boolean isBinary();

  /**
   * Retrieve metadata flag
   *
   * @return metadata flag
   */
  int getFlags();

  /**
   * retrieve extended metadata name if any
   *
   * @return extended metadata name
   */
  String getExtTypeName();

  /** Use column alias as name */
  void useAliasAsName();
}
