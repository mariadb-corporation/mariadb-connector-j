package org.mariadb.jdbc;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import java.sql.SQLException;

public class MyEventListener implements ConnectionEventListener, StatementEventListener {
    public SQLException sqlException;
    public boolean closed;
    public boolean connectionErrorOccured;
    public boolean statementClosed;
    public boolean statementErrorOccured;

    /**
     * MyEventListener initialisation.
     */
    public MyEventListener() {
        sqlException = null;
        closed = false;
        connectionErrorOccured = false;
    }

    public void connectionClosed(ConnectionEvent event) {
        sqlException = event.getSQLException();
        closed = true;
    }

    public void connectionErrorOccurred(ConnectionEvent event) {
        sqlException = event.getSQLException();
        connectionErrorOccured = true;
    }

    public void statementClosed(StatementEvent event) {
        statementClosed = true;
    }

    public void statementErrorOccurred(StatementEvent event) {
        sqlException = event.getSQLException();
        statementErrorOccured = true;
    }
}