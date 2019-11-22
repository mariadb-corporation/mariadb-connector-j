package org.mariadb.jdbc;

import org.junit.*;

import static org.junit.Assert.*;

public class MariaXaResourceTest {
  @Test
  public void xidToString() {
    assertEquals(
        "0x00,0x01,0x05",
        MariaXaResource.xidToString(new MariaDbXid(5, new byte[] {0x00}, new byte[] {0x01})));
    assertEquals(
        "0x,0x000100,0x0400",
        MariaXaResource.xidToString(
            new MariaDbXid(1024, new byte[] {}, new byte[] {0x00, 0x01, 0x00})));
    assertEquals(
        "0x00,0x000100,0xC3C20186",
        MariaXaResource.xidToString(
            new MariaDbXid(-1010695802, new byte[] {0x00}, new byte[] {0x00, 0x01, 0x00})));
  }
}
