// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.message.ClientMessage;

/** SSL request packet see https://mariadb.com/kb/en/connection/#sslrequest-packet */
public final class SslRequestPacket implements ClientMessage {

  private final long clientCapabilities;
  private final byte exchangeCharset;

  /**
   * Constructor
   *
   * @param clientCapabilities client capabilities
   * @param exchangeCharset connection charset to set
   */
  private SslRequestPacket(long clientCapabilities, byte exchangeCharset) {
    this.clientCapabilities = clientCapabilities;
    this.exchangeCharset = exchangeCharset;
  }

  /**
   * Create ssl request packet
   *
   * @param clientCapabilities client capabilities
   * @param exchangeCharset connection charset
   * @return ssl request packet
   */
  public static SslRequestPacket create(long clientCapabilities, byte exchangeCharset) {
    return new SslRequestPacket(clientCapabilities, exchangeCharset);
  }

  @Override
  public int encode(Writer writer, Context context) throws IOException {
    writer.writeInt((int) clientCapabilities);
    writer.writeInt(1024 * 1024 * 1024);
    writer.writeByte(exchangeCharset); // 1 byte
    writer.writeBytes(new byte[19]); // 19  bytes
    writer.writeInt((int) (clientCapabilities >> 32)); // Maria extended flag
    writer.flush();
    return 0;
  }
}
