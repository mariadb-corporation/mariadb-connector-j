package org.mariadb.jdbc;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;


public class MariaXaConnection extends MariaDbPooledConnection implements XAConnection {
    public MariaXaConnection(MariaDbConnection connection) {
        super(connection);
    }

    @Override
    public XAResource getXAResource() throws SQLException {
        return new MariaXaResource(connection);
    }
}
