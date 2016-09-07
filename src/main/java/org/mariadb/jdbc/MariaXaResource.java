/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.
Copyright (c) 2015-2016 MariaDB Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/


package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.protocol.MasterProtocol;
import org.mariadb.jdbc.internal.util.Utils;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class MariaXaResource implements XAResource {
    MariaDbConnection connection;

    public MariaXaResource(MariaDbConnection connection) {
        this.connection = connection;
    }

    static String xidToString(Xid xid) {
        StringBuffer sb = new StringBuffer(2 * Xid.MAXBQUALSIZE + 2 * Xid.MAXGTRIDSIZE + 16);
        sb.append("0x")
                .append(Utils.hexdump(xid.getGlobalTransactionId(), 0))
                .append(",0x")
                .append(Utils.hexdump(xid.getBranchQualifier(), 0))
                .append(",").append(xid.getFormatId());
        return sb.toString();
    }

    static String flagsToString(int flags) {
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

    XAException mapXaException(SQLException sqle) {
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
        }
        if (xaErrorCode != 0) {
            return new XAException(xaErrorCode);
        } else {
            return new XAException(sqle.getMessage());
        }
    }

    /**
     * Execute a query.
     * @param command query to run.
     * @throws XAException exception
     */
    void execute(String command) throws XAException {
        try {
            connection.createStatement().execute(command);
        } catch (SQLException sqle) {
            throw mapXaException(sqle);
        }
    }

    /**
     * Commits the global transaction specified by xid.
     * @param xid A global transaction identifier
     * @param onePhase If true, the resource manager should use a one-phase commit protocol to commit the work done on behalf of xid.
     * @throws XAException exception
     */
    public void commit(Xid xid, boolean onePhase) throws XAException {
        String command = "XA COMMIT " + xidToString(xid);
        if (onePhase) {
            command += " ONE PHASE";
        }
        execute(command);
    }

    /**
     * Ends the work performed on behalf of a transaction branch. The resource manager disassociates the XA resource from the transaction branch
     * specified and lets the transaction complete.
     * <p>If TMSUSPEND is specified in the flags, the transaction branch is temporarily suspended in an incomplete state. The transaction context is
     * in a suspended state and must be resumed via the start method with TMRESUME specified.</p>
     * <p>If TMFAIL is specified, the portion of work has failed. The resource manager may mark the transaction as rollback-only</p>
     * <p>If TMSUCCESS is specified, the portion of work has completed successfully.</p>
     * @param xid A global transaction identifier that is the same as the identifier used previously in the start method.
     * @param flags One of TMSUCCESS, TMFAIL, or TMSUSPEND.
     * @throws XAException An error has occurred. (XAException values are XAER_RMERR, XAER_RMFAILED, XAER_NOTA, XAER_INVAL, XAER_PROTO, or XA_RB*)
     */
    public void end(Xid xid, int flags) throws XAException {
        if (flags != TMSUCCESS && flags != TMSUSPEND && flags != TMFAIL) {
            throw new XAException(XAException.XAER_INVAL);
        }

        execute("XA END " + xidToString(xid) + " " + flagsToString(flags));
    }

    /**
     * Tells the resource manager to forget about a heuristically completed transaction branch.
     * @param xid  A global transaction identifier.
     * @throws XAException  An error has occurred. Possible exception values are XAER_RMERR, XAER_RMFAIL, XAER_NOTA, XAER_INVAL, or XAER_PROTO.
     */
    public void forget(Xid xid) throws XAException {
        // Not implemented by the server
    }

    /**
     * Obtains the current transaction timeout value set for this XAResource instance.
     * If XAResource.setTransactionTimeout was not used prior to invoking this method, the return value is the default timeout set
     * for the resource manager; otherwise, the value used in the previous setTransactionTimeout call is returned.
     * @return the transaction timeout value in seconds.
     * @throws XAException An error has occurred. Possible exception values are XAER_RMERR and XAER_RMFAIL.
     */
    public int getTransactionTimeout() throws XAException {
        // not implemented
        return 0;
    }

    /**
     * This method is called to determine if the resource manager instance represented by the target object is the same as
     * the resource manager instance represented by the parameter xares.
     * @param xaResource An XAResource object whose resource manager instance is to be compared with the target object.
     * @return true if it's the same RM instance; otherwise false.
     * @throws XAException An error has occurred. Possible exception values are XAER_RMERR and XAER_RMFAIL.
     */
    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        // Typically used by transaction manager to "join" transactions. We do not support joins,
        // so always return false;
        return false;
    }

    /**
     * Ask the resource manager to prepare for a transaction commit of the transaction specified in xid.
     * @param xid A global transaction identifier.
     * @return A value indicating the resource manager's vote on the outcome of the transaction.
     * @throws XAException An error has occurred. Possible exception values are: XA_RB*, XAER_RMERR, XAER_RMFAIL, XAER_NOTA, XAER_INVAL, XAER_PROTO.
     */
    public int prepare(Xid xid) throws XAException {
        execute("XA PREPARE " + xidToString(xid));
        return XA_OK;
    }

    /**
     * Obtains a list of prepared transaction branches from a resource manager.
     * The transaction manager calls this method during recovery to obtain the list of transaction branches that are currently in prepared
     * or heuristically completed states.
     * @param flags One of TMSTARTRSCAN, TMENDRSCAN, TMNOFLAGS. TMNOFLAGS must be used when no other flags are set in the parameter.
     * @return The resource manager returns zero or more XIDs of the transaction branches.
     * @throws XAException  An error has occurred. Possible values are XAER_RMERR, XAER_RMFAIL, XAER_INVAL, and XAER_PROTO.
     */
    public Xid[] recover(int flags) throws XAException {
        // Return all Xid  at once, when STARTRSCAN is specified
        // Return zero-length array otherwise.

        if (((flags & TMSTARTRSCAN) == 0) && ((flags & TMENDRSCAN) == 0) && (flags != TMNOFLAGS)) {
            throw new XAException(XAException.XAER_INVAL);
        }

        if ((flags & TMSTARTRSCAN) == 0) {
            return new MariaDbXid[0];
        }

        try {
            ResultSet rs = connection.createStatement().executeQuery("XA RECOVER");
            ArrayList<MariaDbXid> xidList = new ArrayList<MariaDbXid>();

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

    /**
     * Informs the resource manager to roll back work done on behalf of a transaction branch.
     * @param xid  A global transaction identifier.
     * @throws XAException An error has occurred.
     */
    public void rollback(Xid xid) throws XAException {
        execute("XA ROLLBACK " + xidToString(xid));
    }

    /**
     * Sets the current transaction timeout value for this XAResource instance.
     * Once set, this timeout value is effective until setTransactionTimeout is invoked again with a different value.
     * To reset the timeout value to the default value used by the resource manager, set the value to zero.
     * If the timeout operation is performed successfully, the method returns true; otherwise false.
     * If a resource manager does not support explicitly setting the transaction timeout value, this method returns false.
     * @param timeout The transaction timeout value in seconds.
     * @return true if the transaction timeout value is set successfully; otherwise false.
     * @throws XAException An error has occurred. Possible exception values are XAER_RMERR, XAER_RMFAIL, or XAER_INVAL.
     */
    public boolean setTransactionTimeout(int timeout) throws XAException {
        return false; // not implemented
    }

    /**
     * Starts work on behalf of a transaction branch specified in xid.
     * If TMJOIN is specified, the start applies to joining a transaction previously seen by the resource manager.
     * If TMRESUME is specified, the start applies to resuming a suspended transaction specified in the parameter xid.
     * If neither TMJOIN nor TMRESUME is specified and the transaction specified by xid has previously been seen by the resource manager,
     * the resource manager throws the XAException exception with XAER_DUPID error code.
     * @param xid A global transaction identifier to be associated with the resource.
     * @param flags  One of TMNOFLAGS, TMJOIN, or TMRESUME.
     * @throws XAException An error has occurred.
     */
    public void start(Xid xid, int flags) throws XAException {
        if (flags != TMJOIN && flags != TMRESUME && flags != TMNOFLAGS) {
            throw new XAException(XAException.XAER_INVAL);
        }
        if (flags == TMJOIN && connection.getPinGlobalTxToPhysicalConnection()) {
            flags = TMRESUME;
        }
        execute("XA START " + xidToString(xid) + " " + flagsToString(flags));
    }
}
