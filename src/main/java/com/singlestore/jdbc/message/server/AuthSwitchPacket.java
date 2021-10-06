// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.server;

import com.singlestore.jdbc.client.ReadableByteBuf;
import java.util.Arrays;

public class AuthSwitchPacket implements ServerMessage {

  private final String plugin;
  private final byte[] seed;

  public AuthSwitchPacket(String plugin, byte[] seed) {
    this.plugin = plugin;
    this.seed = seed;
  }

  public static AuthSwitchPacket decode(ReadableByteBuf buf) {
    buf.skip(1);
    String plugin = buf.readStringNullEnd();

    byte[] seed = new byte[buf.readableBytes()];
    buf.readBytes(seed);
    return new AuthSwitchPacket(plugin, seed);
  }

  public String getPlugin() {
    return plugin;
  }

  public byte[] getSeed() {
    return seed;
  }

  public static byte[] getTruncatedSeed(byte[] seed) {
    return (seed.length > 0) ? Arrays.copyOfRange(seed, 0, seed.length - 1) : new byte[0];
  }
}
