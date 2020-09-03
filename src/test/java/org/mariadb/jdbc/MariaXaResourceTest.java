package org.mariadb.jdbc;

import static org.junit.Assert.*;

import javax.sql.XAConnection;
import org.junit.Test;

public class MariaXaResourceTest extends BaseTest {
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

  @Test
  public void xaRmTest() throws Exception {
    String url = System.getProperty("dbUrl", mDefUrl);
    MariaDbDataSource dataSource1 = new MariaDbDataSource(url);
    MariaDbDataSource dataSource2 = new MariaDbDataSource(url + "&test=t");
    XAConnection con1 = dataSource1.getXAConnection();
    XAConnection con2 = dataSource1.getXAConnection();
    XAConnection con3 = dataSource2.getXAConnection();
    assertTrue(con1.getXAResource().isSameRM(con1.getXAResource()));
    assertTrue(con1.getXAResource().isSameRM(con2.getXAResource()));
    assertFalse(con1.getXAResource().isSameRM(con3.getXAResource()));
    con1.close();
    con2.close();
    con3.close();
  }
}
