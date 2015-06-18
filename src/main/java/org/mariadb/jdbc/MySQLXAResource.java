package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.mysql.MySQLProtocol;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class MySQLXAResource implements XAResource {
    MySQLConnection connection;

    XAException mapXAException(SQLException sqle) {
        int XAErrorCode;

        switch(sqle.getErrorCode()) {
        case 1397:
            XAErrorCode = XAException.XAER_NOTA;
            break;
        case 1398:
            XAErrorCode = XAException.XAER_INVAL;
            break;
        case 1399:
            XAErrorCode = XAException.XAER_RMFAIL;
            break;
        case 1400:
            XAErrorCode = XAException.XAER_OUTSIDE;
            break;
        case 1401:
            XAErrorCode = XAException.XAER_RMERR;
            break;
        case 1402:
            XAErrorCode = XAException.XA_RBROLLBACK;
            break;
        default:
            XAErrorCode = 0;
        }
        if (XAErrorCode != 0)
            return new XAException(XAErrorCode);
        else
            return new XAException(sqle.getMessage());
    }

    void execute(String command) throws XAException {
        //System.out.println(command);
        try {
            connection.createStatement().execute(command);
        } catch (SQLException sqle) {
            throw mapXAException(sqle);
        }
    }


    public MySQLXAResource(MySQLConnection connection) {
        this.connection = connection;
    }

    static String xidToString(Xid xid) {
        StringBuffer sb = new StringBuffer(2*Xid.MAXBQUALSIZE+2*Xid.MAXGTRIDSIZE+16);
        sb.append("0x")
        .append(MySQLProtocol.hexdump(xid.getGlobalTransactionId(),0))
        .append(",0x")
        .append(MySQLProtocol.hexdump(xid.getBranchQualifier(),0))
        .append(",").append(xid.getFormatId());
        return sb.toString();
    }
    public void commit(Xid xid, boolean onePhase) throws XAException {
        String command = "XA COMMIT " + xidToString(xid);
        if (onePhase)
            command += " ONE PHASE";
        execute(command);
    }

    public void end(Xid xid, int flags) throws XAException {
        if (flags != TMSUCCESS && flags != TMSUSPEND && flags != TMFAIL)
            throw new XAException(XAException.XAER_INVAL);

        execute("XA END " + xidToString(xid) + " " + flagsToString(flags));
    }

    public void forget(Xid xid) throws XAException {
        // Not implemented by the server
    }

    public int getTransactionTimeout() throws XAException {
        // not implemented
        return 0;
    }

    public boolean isSameRM(XAResource xaResource) throws XAException {
        // Typically used by transaction manager to "join" transactions. We do not support joins,
        // so always return false;
        return false;
    }

    public int prepare(Xid xid) throws XAException {
        execute("XA PREPARE " +xidToString(xid));
        return XA_OK;
    }

    public Xid[] recover(int flags) throws XAException {
        // Return all Xid  at once, when STARTRSCAN is specified
        // Return zero-length array otherwise.

        if ( ((flags & TMSTARTRSCAN) == 0) && ((flags & TMENDRSCAN) == 0) && (flags != TMNOFLAGS))
            throw new XAException(XAException.XAER_INVAL);

        if ((flags & TMSTARTRSCAN) == 0)
            return new MySQLXid[0];

        try {
            ResultSet rs = connection.createStatement().executeQuery("XA RECOVER");
            ArrayList<MySQLXid> xidList= new ArrayList<MySQLXid>();

            while(rs.next()) {
                int formatId = rs.getInt(1);
                int len1 = rs.getInt(2);
                int len2 = rs.getInt(3);
                byte[] arr = rs.getBytes(4);

                byte[] globalTransactionId = new byte[len1];
                byte[] branchQualifier = new byte[len2];
                System.arraycopy(arr, 0, globalTransactionId, 0, len1);
                System.arraycopy(arr, len1, branchQualifier, 0, len2);
                xidList.add(new MySQLXid(formatId,globalTransactionId, branchQualifier));
            }
            Xid[] xids = new Xid[xidList.size()];
            xidList.toArray(xids);
            return xids;
        } catch (SQLException sqle) {
            throw mapXAException(sqle);
        }
    }

    public void rollback(Xid xid) throws XAException {
        execute("XA ROLLBACK " + xidToString(xid));
    }

    public boolean setTransactionTimeout(int timeout) throws XAException {
        return false; // not implemented
    }

    public void start(Xid xid, int flags) throws XAException {
        if (flags != TMJOIN && flags != TMRESUME && flags != TMNOFLAGS)
            throw new XAException(XAException.XAER_INVAL);
        if (flags == TMJOIN && "true".equalsIgnoreCase(connection.getPinGlobalTxToPhysicalConnection())) {
            flags = TMRESUME;
        }
        execute("XA START " + xidToString(xid) + " "+flagsToString(flags));
    }

    static String flagsToString(int flags) {
        switch(flags) {
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
}
