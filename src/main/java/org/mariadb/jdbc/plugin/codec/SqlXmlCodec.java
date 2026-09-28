// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.MariaDbSqlXml;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.util.constants.ServerStatus;

/** {@link SQLXML} codec: an XMLTYPE (or any text) column, sent as a string. */
public class SqlXmlCodec implements Codec<SQLXML> {

  public static final SqlXmlCodec INSTANCE = new SqlXmlCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return SQLXML.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && !column.isBinary()
        && type.isAssignableFrom(MariaDbSqlXml.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof SQLXML;
  }

  public SQLXML decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    if (!COMPATIBLE_TYPES.contains(column.getType()) || column.isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as SQLXML", column.getType()));
    }
    return new MariaDbSqlXml(buf.readString(length.get()));
  }

  public SQLXML decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return decodeText(buf, length, column, cal, context);
  }

  private static String content(SQLXML value) throws IOException {
    try {
      return value instanceof MariaDbSqlXml xml ? xml.getValue() : value.getString();
    } catch (SQLException e) {
      throw new IOException("Cannot read SQLXML value", e);
    }
  }

  public void encodeText(Writer encoder, Context context, SQLXML value, Calendar cal, Long maxLen)
      throws IOException {
    String str = content(value);
    encoder.writeByte('\'');
    encoder.writeStringEscaped(
        maxLen == null ? str : str.substring(0, maxLen.intValue()),
        (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
    encoder.writeByte('\'');
  }

  @Override
  public int getApproximateTextProtocolLength(SQLXML value, Long length) {
    try {
      return content(value).length() * 3 + 2;
    } catch (IOException e) {
      return 2;
    }
  }

  public void encodeBinary(
      Writer writer, Context context, SQLXML value, Calendar cal, Long maxLength)
      throws IOException {
    byte[] b = content(value).getBytes(StandardCharsets.UTF_8);
    int len = maxLength != null ? Math.min(maxLength.intValue(), b.length) : b.length;
    writer.writeLength(len);
    writer.writeBytes(b, 0, len);
  }

  public int getBinaryEncodeType() {
    return DataType.VARSTRING.get();
  }
}
