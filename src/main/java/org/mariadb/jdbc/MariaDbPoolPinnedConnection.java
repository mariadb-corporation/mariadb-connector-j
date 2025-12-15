// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.mariadb.jdbc.client.util.ClosableLock;

/** MariaDB pool connection implementation */
public class MariaDbPoolPinnedConnection extends MariaDbPoolConnection {
  private static final Map<Xid, Connection> xidToConnection = new ConcurrentHashMap<>();
  private Xid currentXid;

  /**
   * Constructor.
   *
   * @param connection connection to retrieve connection options
   */
  public MariaDbPoolPinnedConnection(Connection connection) {
    super(connection);
  }

  /**
   * Close underlying connection
   *
   * @throws SQLException if close fails
   */
  @Override
  public void close() throws SQLException {
    super.close();
    if (this.currentXid != null) xidToConnection.remove(this.currentXid);
  }

  @Override
  public XAResource getXAResource() {
    return new MariaDbXAPinnedResource();
  }

  private class MariaDbXAPinnedResource implements XAResource {

    private void execute(Xid xid, String command, boolean removeMappingAfterExecution)
        throws XAException {
      if (xid == null) throw new XAException();

      try {
        if (xid.equals(currentXid)) {
          getConnection().createStatement().execute(command);
        } else {
          Connection con = xidToConnection.get(xid);
          if (con == null) {
            con = getConnection();
            xidToConnection.putIfAbsent(xid, con);
            currentXid = xid;
          }
          try (ClosableLock ignore = con.getLock().closeableLock()) {
            con.createStatement().execute(command);
            currentXid = null;
            if (removeMappingAfterExecution) xidToConnection.remove(xid);
          }
        }
      } catch (SQLException sqle) {
        throw mapXaException(sqle);
      }
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
      execute(xid, "XA COMMIT " + xidToString(xid) + ((onePhase) ? " ONE PHASE" : ""), true);
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
      if (flags != TMSUCCESS && flags != TMSUSPEND && flags != TMFAIL) {
        throw new XAException(XAException.XAER_INVAL);
      }
      execute(xid, "XA END " + xidToString(xid) + " " + flagsToString(flags), false);
    }

    @Override
    public void forget(Xid xid) {
      // Not implemented by the server
      xidToConnection.remove(xid);
    }

    @Override
    public int getTransactionTimeout() {
      // not implemented
      return 0;
    }

    public Configuration getConf() {
      return getConnection().getContext().getConf();
    }

    @Override
    public boolean isSameRM(XAResource xaResource) {
      if (xaResource instanceof MariaDbXAPinnedResource) {
        MariaDbXAPinnedResource other = (MariaDbXAPinnedResource) xaResource;
        return other.getConf().equals(this.getConf());
      }
      return false;
    }

    @Override
    public int prepare(Xid xid) throws XAException {
      execute(xid, "XA PREPARE " + xidToString(xid), false);
      return XA_OK;
    }

    @Override
    public Xid[] recover(int flags) throws XAException {
      if (((flags & TMSTARTRSCAN) == 0) && ((flags & TMENDRSCAN) == 0) && (flags != TMNOFLAGS)) {
        throw new XAException(XAException.XAER_INVAL);
      }

      if ((flags & TMSTARTRSCAN) == 0) {
        return new MariaDbXid[0];
      }

      try {
        ResultSet rs = getConnection().createStatement().executeQuery("XA RECOVER");
        ArrayList<MariaDbXid> xidList = new ArrayList<>();

        while (rs.next()) {
          int formatId = rs.getInt(1);
          int len1 = rs.getInt(2);
          int len2 = rs.getInt(3);
          byte[] arr = rs.getBytes(4);

          byte[] globalTransactionId = new byte[len1];
          byte[] branchQualifier = new byte[len2];
          System.arraycopy(arr, 0, globalTransactionId, 0, len1);
          System.arraycopy(arr, len1, branchQualifier, 0, len2);
          xidList.add(new MariaDbXid(formatId, globalTransactionId, branchQualifier));
        }
        Xid[] xids = new Xid[xidList.size()];
        xidList.toArray(xids);
        return xids;
      } catch (SQLException sqle) {
        throw mapXaException(sqle);
      }
    }

    @Override
    public void rollback(Xid xid) throws XAException {
      execute(xid, "XA ROLLBACK " + xidToString(xid), true);
    }

    @Override
    public boolean setTransactionTimeout(int i) {
      return false;
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
      switch (flags) {
        case TMJOIN:
        case TMRESUME:
          //  specific to pinGlobalTxToPhysicalConnection option set,
          //  force resume in place of JOIN
          execute(xid, "XA START " + xidToString(xid) + " RESUME", false);
          break;
        case TMNOFLAGS:
          execute(xid, "XA START " + xidToString(xid), false);
          break;
        default:
          throw new XAException(XAException.XAER_INVAL);
      }
    }
  }
}
