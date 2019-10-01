/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DistributedTransactionTest extends BaseTest {

  private final MariaDbDataSource dataSource;

  /** Initialisation. */
  public DistributedTransactionTest() throws SQLException {
    dataSource = new MariaDbDataSource();
    dataSource.setServerName(hostname);
    dataSource.setPortNumber(port);
    dataSource.setDatabaseName(database);
    dataSource.setUser(username);
    dataSource.setPassword(password);
  }

  @BeforeClass()
  public static void initClass() throws SQLException {
    createTable("xatable", "i int", "ENGINE=InnoDB");
  }

  @Before
  public void checkSupported() throws SQLException {
    requireMinimumVersion(5, 0);
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
  private int test2PhaseCommit(boolean doCommit) throws Exception {

    int connectionNumber = 1;

    Xid parentXid = newXid();
    Connection[] connections = new Connection[connectionNumber];
    XAConnection[] xaConnections = new XAConnection[connectionNumber];
    XAResource[] xaResources = new XAResource[connectionNumber];
    Xid[] xids = new Xid[connectionNumber];

    try {

      for (int i = 0; i < connectionNumber; i++) {
        xaConnections[i] = dataSource.getXAConnection();
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

  private void insertDatas(int connectionNumber, Connection[] connections) throws SQLException {
    for (int i = 0; i < connectionNumber; i++) {
      connections[i].createStatement().executeUpdate("INSERT INTO xatable VALUES (" + i + ")");
    }
  }

  @Test
  public void testCommit() throws Exception {
    int connectionNumber = test2PhaseCommit(true);

    // check the completion
    try (ResultSet rs =
        sharedConnection.createStatement().executeQuery("SELECT * from xatable order by i")) {
      for (int i = 0; i < connectionNumber; i++) {
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), i);
      }
    }
  }

  @Test
  public void testRollback() throws Exception {
    test2PhaseCommit(false);
    // check the completion
    try (ResultSet rs =
        sharedConnection.createStatement().executeQuery("SELECT * from xatable order by i")) {
      assertFalse(rs.next());
    }
  }

  @Test
  public void testRecover() throws Exception {
    XAConnection xaConnection = dataSource.getXAConnection();
    try {
      Connection connection = xaConnection.getConnection();
      Xid xid = newXid();
      XAResource xaResource = xaConnection.getXAResource();
      xaResource.start(xid, XAResource.TMNOFLAGS);
      connection.createStatement().executeQuery("SELECT 1");
      xaResource.end(xid, XAResource.TMSUCCESS);
      xaResource.prepare(xid);
      Xid[] recoveredXids = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
      assertTrue(recoveredXids != null);
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
    Connection conn1;
    MariaDbDataSource ds = new MariaDbDataSource();
    ds.setUrl(connU);
    ds.setDatabaseName(database);
    ds.setUser(username);
    ds.setPassword(password);
    ds.setPort(port);
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
      try {
        xaRes1.start(xid, XAResource.TMJOIN);
        fail(); // without pinGlobalTxToPhysicalConnection=true
      } catch (XAException xaex) {
        xaConn1.close();
      }

      xid = newXid();
      ds.setUrl(connU + "?pinGlobalTxToPhysicalConnection=true");
      xaConn1 = ds.getXAConnection();
      xaRes1 = xaConn1.getXAResource();
      conn1 = xaConn1.getConnection();
      xaRes1.start(xid, XAResource.TMNOFLAGS);
      conn1.createStatement().executeQuery("SELECT 1");
      xaRes1.end(xid, XAResource.TMSUCCESS);
      xaRes1.start(xid, XAResource.TMJOIN);
      conn1.createStatement().executeQuery("SELECT 1");
      xaRes1.end(xid, XAResource.TMSUCCESS);
      xaRes1.commit(xid, true);
    } finally {
      if (xaConn1 != null) {
        xaConn1.close();
      }
    }
  }
}
