// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.sql.Connection;
import java.util.UUID;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.*;
import org.mariadb.jdbc.Statement;

public class XaTest extends Common {

  private static MariaDbDataSource dataSource;
  private static MariaDbPoolDataSource poolDataSource;
  private static MariaDbDataSource pinnedDataSource;
  private static MariaDbPoolDataSource pinnedPoolDataSource;

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS xatable");
    if (poolDataSource != null) poolDataSource.close();
    if (pinnedPoolDataSource != null) pinnedPoolDataSource.close();
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv"))
            && !isXpand());

    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS xatable");
    stmt.execute("CREATE TABLE xatable(i int)");
    stmt.execute("FLUSH TABLES");
    dataSource = new MariaDbDataSource(mDefUrl);
    poolDataSource = new MariaDbPoolDataSource(mDefUrl);
    pinnedDataSource = new MariaDbDataSource(mDefUrl + "&pinGlobalTxToPhysicalConnection");
    pinnedPoolDataSource = new MariaDbPoolDataSource(mDefUrl + "&pinGlobalTxToPhysicalConnection");
  }

  @Test
  public void xidToString() {
    MariaDbXid xid = new MariaDbXid(1575, new byte[] {0x00}, new byte[] {0x01});
    assertEquals("0x00,0x01,0x627", MariaDbPoolConnection.xidToString(xid));
    assertEquals(
        "0x00,0x,0x627",
        MariaDbPoolConnection.xidToString(new MariaDbXid(1575, new byte[] {0x00}, null)));
    assertEquals(
        "0x,0x000100,0x400",
        MariaDbPoolConnection.xidToString(
            new MariaDbXid(1024, new byte[] {}, new byte[] {0x00, 0x01, 0x00})));
    assertEquals(
        "0x00,0x000100,0xc3c20186",
        MariaDbPoolConnection.xidToString(
            new MariaDbXid(-1010695802, new byte[] {0x00}, new byte[] {0x00, 0x01, 0x00})));
    assertEquals(xid, xid);
    assertEquals(xid, new MariaDbXid(1575, new byte[] {0x00}, new byte[] {0x01}));
    assertNotEquals("dd", xid);
    assertNotEquals(null, xid);
    assertEquals(1544359, xid.hashCode());
  }

  @Test
  public void testPinGlobalTxToPhysicalConnection() throws Exception {

    MariaDbDataSource xads = new MariaDbDataSource(mDefUrl + "&pinGlobalTxToPhysicalConnection");
    Xid txid = new MariaDbXid(1, new byte[] {0xf}, new byte[] {0x01});

    XAConnection c1 = xads.getXAConnection();
    c1.getXAResource().start(txid, XAResource.TMNOFLAGS);
    c1.getXAResource().end(txid, XAResource.TMSUCCESS);

    XAConnection c2 = xads.getXAConnection();
    c2.getXAResource().prepare(txid);
    c2.getXAResource().commit(txid, false);

    c1.close();
    c2.close();
  }

  @Test
  public void testPinGlobalTxToPhysicalConnection2() throws Exception {

    MariaDbDataSource xads = new MariaDbDataSource(mDefUrl + "&pinGlobalTxToPhysicalConnection");
    Xid txid = new MariaDbXid(1, new byte[] {0xf}, new byte[] {0x01});

    XAConnection c1 = xads.getXAConnection();
    c1.getXAResource().start(txid, XAResource.TMNOFLAGS);
    c1.getXAResource().end(txid, XAResource.TMSUCCESS);

    XAConnection c2 = xads.getXAConnection();
    c2.getXAResource().start(txid, XAResource.TMJOIN);
    c2.getConnection().createStatement().executeQuery("SELECT 1");
    c2.getXAResource().end(txid, XAResource.TMSUCCESS);

    c1.close();
    c2.close();
  }

  @Test
  public void xaRmTest() throws Exception {
    xaRmTest(false);
    xaRmTest(true);
  }

  private void xaRmTest(boolean pinnedConnection) throws Exception {
    MariaDbDataSource dataSource1 =
        new MariaDbDataSource(
            mDefUrl + (pinnedConnection ? "&pinGlobalTxToPhysicalConnection" : ""));
    MariaDbDataSource dataSource2 =
        new MariaDbDataSource(
            mDefUrl + (pinnedConnection ? "&pinGlobalTxToPhysicalConnection" : "") + "&test=t");
    XAConnection con1 = dataSource1.getXAConnection();
    XAConnection con2 = dataSource1.getXAConnection();
    XAConnection con3 = dataSource2.getXAConnection();
    assertTrue(con1.getXAResource().isSameRM(con1.getXAResource()));
    assertTrue(con1.getXAResource().isSameRM(con2.getXAResource()));
    assertFalse(con1.getXAResource().isSameRM(con3.getXAResource()));

    assertEquals(0, con1.getXAResource().getTransactionTimeout());
    con1.getXAResource().setTransactionTimeout(10);
    assertEquals(0, con1.getXAResource().getTransactionTimeout());
    Xid xid = new MariaDbXid(1575, new byte[] {0x00}, new byte[] {0x01});
    con1.getXAResource().forget(xid);
    assertThrows(XAException.class, () -> con1.getXAResource().end(xid, XAResource.TMENDRSCAN));
    assertThrows(XAException.class, () -> con1.getXAResource().start(xid, XAResource.TMSUCCESS));

    con1.close();
    con2.close();
    con3.close();
  }

  private Xid newXid() {
    return new MariaDbXid(
        1, UUID.randomUUID().toString().getBytes(), UUID.randomUUID().toString().getBytes());
  }

  private Xid newXid(Xid branchFrom) {
    return new MariaDbXid(
        1, branchFrom.getGlobalTransactionId(), UUID.randomUUID().toString().getBytes());
  }

  /**
   * 2 phase commit , with either commit or rollback at the end.
   *
   * @param doCommit must commit
   * @throws Exception exception
   */
  private int test2PhaseCommit(boolean doCommit, XADataSource dataSource) throws Exception {

    int connectionNumber = 2;

    Xid parentXid = newXid();
    java.sql.Connection[] connections = new java.sql.Connection[connectionNumber];
    XAConnection[] xaConnections = new XAConnection[connectionNumber];
    XAResource[] xaResources = new XAResource[connectionNumber];
    Xid[] xids = new Xid[connectionNumber];

    try {

      for (int i = 0; i < connectionNumber; i++) {
        if (i == 0) {
          xaConnections[i] = dataSource.getXAConnection(user, password);
        } else xaConnections[i] = dataSource.getXAConnection();

        connections[i] = xaConnections[i].getConnection();
        xaResources[i] = xaConnections[i].getXAResource();
        xids[i] = newXid(parentXid);
      }

      startAllResources(connectionNumber, xaResources, xids);
      insertDatas(connectionNumber, connections);
      endAllResources(connectionNumber, xaResources, xids);
      prepareAllResources(connectionNumber, xaResources, xids);

      for (int i = 0; i < connectionNumber; i++) {
        if (doCommit) {
          xaResources[i].commit(xids[i], false);
        } else {
          xaResources[i].rollback(xids[i]);
        }
      }

    } finally {
      for (int i = 0; i < connectionNumber; i++) {
        try {
          if (xaConnections[i] != null) {
            xaConnections[i].close();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    return connectionNumber;
  }

  private void startAllResources(int connectionNumber, XAResource[] xaResources, Xid[] xids)
      throws XAException {
    for (int i = 0; i < connectionNumber; i++) {
      xaResources[i].start(xids[i], XAResource.TMNOFLAGS);
    }
  }

  private void endAllResources(int connectionNumber, XAResource[] xaResources, Xid[] xids)
      throws XAException {
    for (int i = 0; i < connectionNumber; i++) {
      xaResources[i].end(xids[i], XAResource.TMSUCCESS);
    }
  }

  private void prepareAllResources(int connectionNumber, XAResource[] xaResources, Xid[] xids)
      throws XAException {
    for (int i = 0; i < connectionNumber; i++) {
      xaResources[i].prepare(xids[i]);
    }
  }

  private void insertDatas(int connectionNumber, java.sql.Connection[] connections)
      throws SQLException {
    for (int i = 0; i < connectionNumber; i++) {
      connections[i].createStatement().executeUpdate("INSERT INTO xatable VALUES (" + i + ")");
    }
  }

  @Test
  public void testCommit() throws Exception {
    Assumptions.assumeFalse("galera".equals(System.getenv("srv")));
    testCommit(dataSource);
    testCommit(poolDataSource);

    testCommit(pinnedDataSource);
    testCommit(pinnedPoolDataSource);
  }

  public void testCommit(XADataSource dataSource) throws Exception {
    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE xatable");
    int connectionNumber = test2PhaseCommit(true, dataSource);

    // check the completion
    ResultSet rs = stmt.executeQuery("SELECT * from xatable order by i");
    for (int i = 0; i < connectionNumber; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getInt(1), i);
    }
  }

  @Test
  public void testRollback() throws Exception {
    Assumptions.assumeFalse("galera".equals(System.getenv("srv")));
    testRollback(dataSource);
    testRollback(poolDataSource);

    testRollback(pinnedDataSource);
    testRollback(pinnedPoolDataSource);
  }

  public void testRollback(XADataSource dataSource) throws Exception {
    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE xatable");
    test2PhaseCommit(false, dataSource);
    // check the completion
    ResultSet rs = stmt.executeQuery("SELECT * from xatable order by i");
    assertFalse(rs.next());
  }

  @Test
  public void testRecover() throws Exception {
    testRecover(dataSource);
    testRecover(pinnedDataSource);
  }

  private void testRecover(XADataSource dataSource) throws Exception {
    Assumptions.assumeFalse("galera".equals(System.getenv("srv")));
    XAConnection xaConnection = dataSource.getXAConnection();
    try {
      java.sql.Connection connection = xaConnection.getConnection();
      Xid xid = newXid();
      XAResource xaResource = xaConnection.getXAResource();
      xaResource.start(xid, XAResource.TMNOFLAGS);
      connection.createStatement().executeQuery("SELECT 1");
      xaResource.end(xid, XAResource.TMSUCCESS);
      xaResource.prepare(xid);
      Xid[] recoveredXids = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
      assertNotNull(recoveredXids);
      assertTrue(recoveredXids.length > 0);
      boolean found = false;

      for (Xid x : recoveredXids) {
        if (x != null && x.equals(xid)) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    } finally {
      xaConnection.close();
    }
  }

  @Test
  public void resumeAndJoinTest() throws Exception {
    resumeAndJoinTest(false);
    resumeAndJoinTest(true);
  }

  private void resumeAndJoinTest(boolean pinnedConnection) throws Exception {

    Assumptions.assumeFalse("galera".equals(System.getenv("srv")));
    Connection conn1;
    MariaDbDataSource ds =
        new MariaDbDataSource(
            mDefUrl + (pinnedConnection ? "&pinGlobalTxToPhysicalConnection" : ""));

    XAConnection xaConn1 = null;
    Xid xid = newXid();
    try {
      xaConn1 = ds.getXAConnection();
      XAResource xaRes1 = xaConn1.getXAResource();
      conn1 = xaConn1.getConnection();
      xaRes1.start(xid, XAResource.TMNOFLAGS);
      conn1.createStatement().executeQuery("SELECT 1");
      xaRes1.end(xid, XAResource.TMSUCCESS);
      xaRes1.start(xid, XAResource.TMRESUME);
      conn1.createStatement().executeQuery("SELECT 1");
      xaRes1.end(xid, XAResource.TMSUCCESS);
      xaRes1.commit(xid, true);
      xaConn1.close();

      xaConn1 = ds.getXAConnection();
      xaRes1 = xaConn1.getXAResource();
      conn1 = xaConn1.getConnection();
      xaRes1.start(xid, XAResource.TMNOFLAGS);
      conn1.createStatement().executeQuery("SELECT 1");
      xaRes1.end(xid, XAResource.TMSUCCESS);
      if (pinnedConnection) {
        xaRes1.start(xid, XAResource.TMJOIN);
      } else {
        try {
          xaRes1.start(xid, XAResource.TMJOIN);
          fail(); // without pinGlobalTxToPhysicalConnection=true
        } catch (XAException xaex) {
          xaConn1.close();
        }
      }

    } finally {
      if (xaConn1 != null) {
        xaConn1.close();
      }
    }
  }

  @Test
  public void errorCodeTest() throws SQLException, XAException {
    XAConnection con = poolDataSource.getXAConnection();
    try {
      Xid xid = newXid();
      Xid xid2 = newXid();
      XAResource xaResource = con.getXAResource();
      try {
        xaResource.prepare(xid);
        xaResource.commit(xid, true);
        fail();
      } catch (XAException xae) {
        assertEquals(XAException.XAER_RMFAIL, xae.errorCode); // 1399
      }
      try {
        xaResource.forget(xid);
        xaResource.rollback(xid);
        fail();
      } catch (XAException xae) {
        assertEquals(XAException.XAER_NOTA, xae.errorCode); // 1397
      }
      try {
        xaResource.commit(xid, true);
        fail();
      } catch (XAException xae) {
        assertTrue(
            XAException.XAER_INVAL == xae.errorCode
                || XAException.XAER_NOTA == xae.errorCode); // 1398
      }
      try {
        xaResource.commit(xid, true);
        fail();
      } catch (XAException xae) {
        assertTrue(
            XAException.XAER_INVAL == xae.errorCode
                || XAException.XAER_NOTA == xae.errorCode); // 1398
      }
      xaResource.start(xid2, XAResource.TMNOFLAGS);
      xaResource.end(xid2, XAResource.TMSUCCESS);
      try {
        xaResource.start(xid2, XAResource.TMRESUME);
      } catch (XAException e) {
        // eat
      }
      try {
        xaResource.end(xid2, XAResource.TMSUSPEND);
      } catch (XAException e) {
        // eat
      }

      try {
        xaResource.start(xid2, XAResource.TMJOIN);
      } catch (XAException e) {
        // eat
      }

      try {
        xaResource.end(xid2, XAResource.TMFAIL);
      } catch (XAException e) {
        // eat
      }
      xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
      try {
        xaResource.recover(XAResource.TMFAIL);
        fail();
      } catch (XAException e) {
        // eat
      }
      xaResource.recover(XAResource.TMNOFLAGS);
      con.close();
      try {
        xaResource.recover(XAResource.TMSTARTRSCAN);
        fail();
      } catch (XAException e) {
        // eat
      }
    } finally {
      con.close();
    }
  }
}
