// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2023 SingleStore, Inc.

package com.singlestore.jdbc.client.socket;

import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.util.MutableInt;
import java.io.IOException;

/** Packet Reader */
public interface Reader {

  /**
   * Get next MySQL packet. If packet is more than 16M, read as many packet needed to finish reading
   * MySQL packet. (first that has not length = 16Mb)
   *
   * @param reUsable if packet can use existing reusable buf to avoid creating array
   * @return array packet.
   * @throws IOException if socket exception occur.
   */
  ReadableByteBuf readPacket(boolean reUsable) throws IOException;

  /**
   * Get next MySQL packet. If packet is more than 16M, read as many packet needed to finish reading
   * MySQL packet. (first that has not length = 16Mb)
   *
   * @param reUsable if packet can use existing reusable buf to avoid creating array
   * @param traceEnable must trace packet.
   * @return array packet.
   * @throws IOException if socket exception occur.
   */
  ReadableByteBuf readPacket(boolean reUsable, boolean traceEnable) throws IOException;

  MutableInt getSequence();

  void close() throws IOException;

  /**
   * Set server thread id.
   *
   * @param serverThreadId current server thread id.
   * @param hostAddress host information
   */
  void setServerThreadId(Long serverThreadId, HostAddress hostAddress);
}
