// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab

package org.mariadb.jdbc.fuzz.support;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import java.io.IOException;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.util.MutableByte;

/** Unified mock for MariaDB protocol reader. */
public class FuzzReader implements Reader {
  private final FuzzedDataProvider data;
  private final MutableByte sequence = new MutableByte();

  public FuzzReader(FuzzedDataProvider data) {
    this.data = data;
    this.sequence.set((byte) 0);
  }

  @Override
  public ReadableByteBuf readReusablePacket(boolean traceEnable) throws IOException {
    return new StandardReadableByteBuf(data.consumeBytes(data.consumeInt(0, 1024)));
  }

  @Override
  public ReadableByteBuf readReusablePacket() throws IOException {
    return readReusablePacket(false);
  }

  @Override
  public byte[] readPacket(boolean traceEnable) throws IOException {
    return data.consumeBytes(data.consumeInt(0, 1024));
  }

  @Override
  public ReadableByteBuf readableBufFromArray(byte[] buf) {
    return new StandardReadableByteBuf(buf);
  }

  @Override
  public MutableByte getSequence() {
    return sequence;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void setServerThreadId(Long serverThreadId, HostAddress hostAddress) {}
}
