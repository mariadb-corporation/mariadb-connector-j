package org.mariadb.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.PrepareCache;
import org.mariadb.jdbc.message.server.PrepareResultPacket;

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
