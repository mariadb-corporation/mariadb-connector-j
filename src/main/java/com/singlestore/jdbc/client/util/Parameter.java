// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.client.util;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import java.io.IOException;
import java.sql.SQLException;

/** Parameter */
public interface Parameter {

  /**
   * Encode parameter in text format
   *
   * @param encoder packet writer
   * @param context connection context
   * @throws IOException if socket error occurs
   * @throws SQLException if other kind of error occurs
   */
  void encodeText(Writer encoder, Context context) throws IOException, SQLException;

  /**
   * Encode parameter in binary format
   *
   * @param encoder packet writer
   * @throws IOException if socket error occurs
   * @throws SQLException if other kind of error occurs
   */
  void encodeBinary(Writer encoder) throws IOException, SQLException;

  /**
   * Encode parameter in binary long format
   *
   * @param encoder packet writer
   * @throws IOException if socket error occurs
   * @throws SQLException if other kind of error occurs
   */
  void encodeLongData(Writer encoder) throws IOException, SQLException;

  /**
   * transform parameter in byte array
   *
   * @return bytes
   * @throws IOException if socket error occurs
   * @throws SQLException if other kind of error occurs
   */
  byte[] encodeData() throws IOException, SQLException;

  /**
   * Can parameter be encoded in binary long format
   *
   * @return can parameter be encoded in binary long format
   */
  boolean canEncodeLongData();

  /**
   * return binary encoding type
   *
   * @return binary encoding type
   */
  int getBinaryEncodeType();

  /**
   * Approximate length in bytes.
   *
   * @return value length in bytes
   */
  int getApproximateTextProtocolLength() throws IOException, SQLException;

  /**
   * is parameter null
   *
   * @return is null
   */
  boolean isNull();

  /**
   * Methods to return parameter as string if possible (Streaming parameter will return null)
   *
   * @param context current connection context
   * @return null if not available.
   */
  String bestEffortStringValue(Context context);
}
