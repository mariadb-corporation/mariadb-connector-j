package org.drizzle.jdbc;

import java.sql.*;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 19, 2009
 * Time: 10:10:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class DrizzleStatement implements Statement {
    private Protocol protocol;

    public DrizzleStatement(Protocol protocol) {
        this.protocol=protocol;
    }

    public ResultSet executeQuery(String s) throws SQLException {
        DrizzleRows rows = null;
        try {
            protocol.executeQuery(s);
        } catch (IOException e) {
            throw new SQLException("Could not execute query: "+e.getMessage());
        }
        return new DrizzleResultSet(rows);
    }

    public int executeUpdate(String s) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void close() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public int getMaxFieldSize() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setMaxFieldSize(int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public int getMaxRows() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setMaxRows(int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setEscapeProcessing(boolean b) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public int getQueryTimeout() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setQueryTimeout(int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void cancel() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void clearWarnings() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setCursorName(String s) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean execute(String query) throws SQLException {
        try {
            protocol.executeQuery(query);
            return true;
        } catch (IOException e) {
            throw new SQLException("Could not execute query: "+e.getMessage());
        }
    }

    public ResultSet getResultSet() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int getUpdateCount() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean getMoreResults() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setFetchDirection(int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public int getFetchDirection() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setFetchSize(int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public int getFetchSize() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int getResultSetConcurrency() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int getResultSetType() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void addBatch(String s) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void clearBatch() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public int[] executeBatch() throws SQLException {
        return new int[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Connection getConnection() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean getMoreResults(int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int executeUpdate(String s, int i) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int executeUpdate(String s, int[] ints) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int executeUpdate(String s, String[] strings) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean execute(String s, int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean execute(String s, int[] ints) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean execute(String s, String[] strings) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int getResultSetHoldability() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean isClosed() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setPoolable(boolean b) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean isPoolable() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public <T> T unwrap(Class<T> tClass) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
