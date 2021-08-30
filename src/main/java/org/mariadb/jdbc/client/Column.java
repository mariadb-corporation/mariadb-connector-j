package org.mariadb.jdbc.client;

import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.plugin.Codec;

public interface Column {

  String getSchema();

  String getTableAlias();

  String getTable();

  String getColumnAlias();

  String getColumnName();

  long getLength();

  DataType getType();

  byte getDecimals();

  boolean isSigned();

  int getDisplaySize();

  boolean isPrimaryKey();

  boolean isAutoIncrement();

  boolean hasDefault();

  // doesn't use & 128 bit filter, because char binary and varchar binary are not binary (handle
  // like string), but have the binary flag
  boolean isBinary();

  int getFlags();

  String getExtTypeName();

  /**
   * Return metadata precision.
   *
   * @return precision
   */
  long getPrecision();

  int getColumnType(Configuration conf);

  Codec<?> getDefaultCodec(Configuration conf);

  void useAliasAsName();
}
