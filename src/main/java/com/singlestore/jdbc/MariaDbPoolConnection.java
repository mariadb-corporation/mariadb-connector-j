// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

import com.singlestore.jdbc.util.StringUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.*;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class MariaDbPoolConnection implements PooledConnection, XAConnection {

  private final Connection connection;
  private final List<ConnectionEventListener> connectionEventListeners;
  private final List<StatementEventListener> statementEventListeners;

  /**
   * Constructor.
   *
   * @param connection connection to retrieve connection options
   */
  public MariaDbPoolConnection(Connection connection) {
    this.connection = connection;
    this.connection.setPoolConnection(this);
    statementEventListeners = new CopyOnWriteArrayList<>();
    connectionEventListeners = new CopyOnWriteArrayList<>();
  }

  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public void addConnectionEventListener(ConnectionEventListener listener) {
    connectionEventListeners.add(listener);
  }

  @Override
  public void removeConnectionEventListener(ConnectionEventListener listener) {
    connectionEventListeners.remove(listener);
  }

  @Override
  public void addStatementEventListener(StatementEventListener listener) {
    statementEventListeners.add(listener);
  }

  @Override
  public void removeStatementEventListener(StatementEventListener listener) {
    statementEventListeners.remove(listener);
  }

  public void fireStatementClosed(PreparedStatement statement) {
    StatementEvent event = new StatementEvent(this, statement);
    for (StatementEventListener listener : statementEventListeners) {
      listener.statementClosed(event);
    }
  }

  public void fireStatementErrorOccurred(PreparedStatement statement, SQLException returnEx) {
    StatementEvent event = new StatementEvent(this, statement, returnEx);
    for (StatementEventListener listener : statementEventListeners) {
      listener.statementErrorOccurred(event);
    }
  }

  public void fireConnectionClosed(ConnectionEvent event) {
    for (ConnectionEventListener listener : connectionEventListeners) {
      listener.connectionClosed(event);
    }
  }

  public void fireConnectionErrorOccurred(SQLException returnEx) {
    ConnectionEvent event = new ConnectionEvent(this, returnEx);
    for (ConnectionEventListener listener : connectionEventListeners) {
      listener.connectionErrorOccurred(event);
    }
  }

  @Override
  public void close() throws SQLException {
    fireConnectionClosed(new ConnectionEvent(this));
    connection.setPoolConnection(null);
    connection.close();
  }

  public static String xidToString(Xid xid) {
    return "0x"
        + StringUtils.byteArrayToHexString(xid.getGlobalTransactionId())
        + ",0x"
        + StringUtils.byteArrayToHexString(xid.getBranchQualifier())
        + ",0x"
        + Integer.toHexString(xid.getFormatId());
  }

  @Override
  public XAResource getXAResource() throws SQLException {
    return new MariaDbXAResource();
  }

  private class MariaDbXAResource implements XAResource {

    private String flagsToString(int flags) {
      switch (flags) {
        case TMJOIN:
          return "JOIN";
        case TMONEPHASE:
          return "ONE PHASE";
        case TMRESUME:
          return "RESUME";
        case TMSUSPEND:
          return "SUSPEND";
        default:
          return "";
      }
    }

    private XAException mapXaException(SQLException sqle) {
      int xaErrorCode;

      switch (sqle.getErrorCode()) {
        case 1397:
          xaErrorCode = XAException.XAER_NOTA;
          break;
        case 1398:
          xaErrorCode = XAException.XAER_INVAL;
          break;
        case 1399:
          xaErrorCode = XAException.XAER_RMFAIL;
          break;
        case 1400:
          xaErrorCode = XAException.XAER_OUTSIDE;
          break;
        case 1401:
          xaErrorCode = XAException.XAER_RMERR;
          break;
        case 1402:
          xaErrorCode = XAException.XA_RBROLLBACK;
          break;
        default:
          xaErrorCode = 0;
          break;
      }
      XAException xaException;
      if (xaErrorCode != 0) {
        xaException = new XAException(xaErrorCode);
      } else {
        xaException = new XAException(sqle.getMessage());
      }
      xaException.initCause(sqle);
      return xaException;
    }

    private void execute(String command) throws XAException {
      try {
        connection.createStatement().execute(command);
      } catch (SQLException sqle) {
        throw mapXaException(sqle);
      }
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
      execute("XA COMMIT " + xidToString(xid) + ((onePhase) ? " ONE PHASE" : ""));
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
      if (flags != TMSUCCESS && flags != TMSUSPEND && flags != TMFAIL) {
        throw new XAException(XAException.XAER_INVAL);
      }

      execute("XA END " + xidToString(xid) + " " + flagsToString(flags));
    }

    @Override
    public void forget(Xid xid) throws XAException {
      // Not implemented by the server
    }

    @Override
    public int getTransactionTimeout() throws XAException {
      // not implemented
      return 0;
    }

    public Configuration getConf() {
      return connection.getContext().getConf();
    }

    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
      if (xaResource instanceof MariaDbXAResource) {
        MariaDbXAResource other = (MariaDbXAResource) xaResource;
        return other.getConf().equals(this.getConf());
      }
      return false;
    }

    @Override
    public int prepare(Xid xid) throws XAException {
      execute("XA PREPARE " + xidToString(xid));
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
        ResultSet rs = connection.createStatement().executeQuery("XA RECOVER");
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
      execute("XA ROLLBACK " + xidToString(xid));
    }

    @Override
    public boolean setTransactionTimeout(int i) throws XAException {
      return false;
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
      if (flags != TMJOIN && flags != TMRESUME && flags != TMNOFLAGS) {
        throw new XAException(XAException.XAER_INVAL);
      }
      execute("XA START " + xidToString(xid) + " " + flagsToString(flags));
    }
  }
}
