package org.mariadb.jdbc;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;


public class MySQLXAConnection extends MySQLPooledConnection implements XAConnection {
    public MySQLXAConnection(MySQLConnection connection) {
        super(connection);
    }

    public XAResource getXAResource() throws SQLException {
        return new MySQLXAResource(connection);
    }
}
