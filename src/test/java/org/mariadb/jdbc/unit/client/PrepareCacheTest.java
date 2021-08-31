// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.impl.PrepareCache;
import org.mariadb.jdbc.message.server.PrepareResultPacket;

public class PrepareCacheTest {

  @Test
  public void check() {
    PrepareCache cache = new PrepareCache(20, null);
    try {
      cache.get("dd");
      fail();
    } catch (IllegalStateException s) {
      assertTrue(s.getMessage().contains("not available method"));
    }
    try {
      cache.put("dd", (PrepareResultPacket) null);
      fail();
    } catch (IllegalStateException s) {
      assertTrue(s.getMessage().contains("not available method"));
    }
  }
}
