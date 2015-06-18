package org.mariadb.jdbc;

import javax.transaction.xa.Xid;
import java.util.Arrays;


public class MySQLXid implements Xid {
    public int formatId;
    public byte[] globalTransactionId;
    public byte[] branchQualifier;

    public MySQLXid(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
        this.formatId = formatId;
        this.globalTransactionId = globalTransactionId;
        this.branchQualifier = branchQualifier;
    }



    public boolean equals(Object o) {
        if (o instanceof Xid) {
            Xid other = (Xid) o;
            return formatId == other.getFormatId()
                   && Arrays.equals(globalTransactionId, other.getGlobalTransactionId())
                   && Arrays.equals(branchQualifier, other.getBranchQualifier());

        }
        return false;
    }
    public int getFormatId() {
        return formatId;
    }

    public byte[] getGlobalTransactionId() {
        return globalTransactionId;
    }

    public byte[] getBranchQualifier() {
        return branchQualifier;
    }

}
