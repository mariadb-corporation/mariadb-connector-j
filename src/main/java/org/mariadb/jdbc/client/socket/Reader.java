// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.client.socket;

import java.io.IOException;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableByte;

/** Packet Reader */
public interface Reader {

  /**
   * Get next MySQL packet. Packet is expected to have size &lt; 16M and will use if possible an
   * internal cached buffer. This packet bytes are expect to be read immediately
   *
   * @param traceEnable must trace pacjet
   * @return Readable byte array packet.
   * @throws IOException if socket exception occur.
   */
  ReadableByteBuf readReusablePacket(boolean traceEnable) throws IOException;

  /**
   * Get next MySQL packet. Packet is expected to have size &lt; 16M and will use if possible an
   * internal cached buffer. This packet bytes are expect to be read immediately
   *
   * @return Readable byte array packet.
   * @throws IOException if socket exception occur.
   */
  ReadableByteBuf readReusablePacket() throws IOException;

  /**
   * Get next MySQL packet. If packet is more than 16M, read as many packet needed to finish reading
   * MySQL packet. (first that has not length = 16Mb)
   *
   * @param traceEnable must trace packet.
   * @return array packet.
   * @throws IOException if socket exception occur.
   */
  byte[] readPacket(boolean traceEnable) throws IOException;

  /**
   * Get a readable byte array from byte array. This packet is expected to be read immediately,
   * since no lock is set on this packet.
   *
   * @param buf byte array to be parsed
   * @return array packet.
   */
  ReadableByteBuf readableBufFromArray(byte[] buf);

  /**
   * Skip next MySQL packet. Packet is expected to have size &lt; 16M
   *
   * @throws IOException if socket exception occur.
   */
  void skipPacket() throws IOException;

  /**
   * Get current sequence object
   *
   * @return current sequence
   */
  MutableByte getSequence();

  /**
   * Close stream
   *
   * @throws IOException if any error occurs
   */
  void close() throws IOException;

  /**
   * Set server thread id.
   *
   * @param serverThreadId current server thread id.
   * @param hostAddress host information
   */
  void setServerThreadId(Long serverThreadId, HostAddress hostAddress);
}
