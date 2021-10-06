// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.client.context.Context;
import com.singlestore.jdbc.client.socket.PacketWriter;
import java.io.IOException;

public final class SslRequestPacket implements ClientMessage {

  private final long clientCapabilities;
  private final byte exchangeCharset;

  private SslRequestPacket(long clientCapabilities, byte exchangeCharset) {
    this.clientCapabilities = clientCapabilities;
    this.exchangeCharset = exchangeCharset;
  }

  public static SslRequestPacket create(long clientCapabilities, byte exchangeCharset) {
    return new SslRequestPacket(clientCapabilities, exchangeCharset);
  }

  @Override
  public int encode(PacketWriter writer, Context context) throws IOException {
    writer.writeInt((int) clientCapabilities);
    writer.writeInt(1024 * 1024 * 1024);
    writer.writeByte(exchangeCharset); // 1 byte
    writer.writeBytes(new byte[19]); // 19  bytes
    writer.writeInt((int) (clientCapabilities >> 32)); // Maria extended flag
    writer.flush();
    return 0;
  }
}
