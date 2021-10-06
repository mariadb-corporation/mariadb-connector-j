// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.client.PrepareCache;
import com.singlestore.jdbc.message.server.PrepareResultPacket;
import org.junit.jupiter.api.Test;

public class PrepareCacheTest {

  @Test
  public void check() throws Exception {
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
