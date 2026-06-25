// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab

package org.mariadb.jdbc.fuzz.support;

import java.sql.SQLDataException;
import java.sql.Types;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;

/** Unified mock for MariaDB column metadata used in decoding. */
public class FuzzColumn implements ColumnDecoder {

  @Override
  public String getCatalog() {
    return "def";
  }

  @Override
  public String getSchema() {
    return "test";
  }

  @Override
  public String getTableAlias() {
    return "";
  }

  @Override
  public String getTable() {
    return "fuzz_table";
  }

  @Override
  public String getColumnAlias() {
    return "";
  }

  @Override
  public String getColumnName() {
    return "fuzz_column";
  }

  @Override
  public long getColumnLength() {
    return 100;
  }

  @Override
  public DataType getType() {
    return DataType.VARCHAR;
  }

  @Override
  public byte getDecimals() {
    return 0;
  }

  @Override
  public boolean isSigned() {
    return true;
  }

  @Override
  public int getDisplaySize() {
    return 100;
  }

  @Override
  public boolean isPrimaryKey() {
    return false;
  }

  @Override
  public boolean isAutoIncrement() {
    return false;
  }

  @Override
  public boolean hasDefault() {
    return false;
  }

  @Override
  public boolean isBinary() {
    return false;
  }

  @Override
  public int getFlags() {
    return 0;
  }

  @Override
  public String getExtTypeName() {
    return null;
  }

  @Override
  public String defaultClassname(Configuration conf) {
    return "java.lang.String";
  }

  @Override
  public int getColumnType(Configuration conf) {
    return Types.VARCHAR;
  }

  @Override
  public String getColumnTypeName(Configuration conf) {
    return "VARCHAR";
  }

  @Override
  public Object getDefaultText(ReadableByteBuf buf, MutableInt length, Context context)
      throws SQLDataException {
    return null;
  }

  @Override
  public Object getDefaultBinary(ReadableByteBuf buf, MutableInt length, Context context)
      throws SQLDataException {
    return null;
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, MutableInt length, Calendar cal, Context context)
      throws SQLDataException {
    return "";
  }

  @Override
  public String decodeStringBinary(
      ReadableByteBuf buf, MutableInt length, Calendar cal, Context context) throws SQLDataException {
    return "";
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return 0;
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return 0;
  }

  @Override
  public java.sql.Date decodeDateText(
      ReadableByteBuf buf, MutableInt length, Calendar cal, Context context) throws SQLDataException {
    return new java.sql.Date(0);
  }

  @Override
  public java.sql.Date decodeDateBinary(
      ReadableByteBuf buf, MutableInt length, Calendar cal, Context context) throws SQLDataException {
    return new java.sql.Date(0);
  }

  @Override
  public java.sql.Time decodeTimeText(
      ReadableByteBuf buf, MutableInt length, Calendar cal, Context context) throws SQLDataException {
    return new java.sql.Time(0);
  }

  @Override
  public java.sql.Time decodeTimeBinary(
      ReadableByteBuf buf, MutableInt length, Calendar cal, Context context) throws SQLDataException {
    return new java.sql.Time(0);
  }

  @Override
  public java.sql.Timestamp decodeTimestampText(
      ReadableByteBuf buf, MutableInt length, Calendar cal, Context context) throws SQLDataException {
    return new java.sql.Timestamp(0);
  }

  @Override
  public java.sql.Timestamp decodeTimestampBinary(
      ReadableByteBuf buf, MutableInt length, Calendar cal, Context context) throws SQLDataException {
    return new java.sql.Timestamp(0);
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return false;
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return false;
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return 0;
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return 0;
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return 0;
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return 0;
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return 0;
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return 0;
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return 0;
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return 0;
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return 0;
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return 0;
  }

  @Override
  public ColumnDecoder useAliasAsName() {
    return this;
  }
}
