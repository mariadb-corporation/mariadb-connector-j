// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client;

import org.mariadb.jdbc.MariaDbBlob;

public interface ReadableByteBuf {

  int readableBytes();

  int pos();

  byte[] buf();

  void buf(byte[] buf, int limit, int pos);

  void pos(int pos);

  void skip();

  void skip(int length);

  MariaDbBlob readBlob(int length);

  byte getByte();

  byte getByte(int index);

  short getUnsignedByte();

  int readLengthNotNull();

  int skipIdentifier();

  Integer readLength();

  byte readByte();

  short readUnsignedByte();

  short readShort();

  int readUnsignedShort();

  int readMedium();

  int readUnsignedMedium();

  int readInt();

  int readIntBE();

  long readUnsignedInt();

  long readLong();

  long readLongBE();

  void readBytes(byte[] dst);

  byte[] readBytesNullEnd();

  ReadableByteBuf readLengthBuffer();

  String readString(int length);

  String readAscii(int length);

  String readStringNullEnd();

  String readStringEof();

  float readFloat();

  double readDouble();

  double readDoubleBE();
}
