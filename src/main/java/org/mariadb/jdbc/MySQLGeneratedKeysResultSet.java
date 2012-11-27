package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.common.queryresults.*;
import org.mariadb.jdbc.internal.mysql.MySQLProtocol;

import java.sql.*;

public class MySQLGeneratedKeysResultSet extends MySQLResultSet {
    public MySQLGeneratedKeysResultSet(QueryResult dqr, Statement statement, MySQLProtocol protocol) {
       super(dqr,statement, protocol);
    }

    public int findColumn(String columnLabel) throws SQLException {
        /*
         There is only a single column returned in generated keys (autoincrement value),
         and we do not know the real name of the primary  key, because it is not returned by the protocol
         (and because we're too lazy  to check in information schema, this is expensive operation).
         At the moment the solution is to map just any name to index 1.
        */
        return 1;
    }
}
