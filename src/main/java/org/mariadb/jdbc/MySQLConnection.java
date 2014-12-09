/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

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

import org.mariadb.jdbc.internal.SQLExceptionMapper;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.Utils;
import org.mariadb.jdbc.internal.mysql.MySQLProtocol;

import java.net.SocketException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;


public final class MySQLConnection  implements Connection {
    /**
     * the protocol to communicate with.
     */
    private final MySQLProtocol protocol;
    /**
     * save point count - to generate good names for the savepoints.
     */
    private int savepointCount = 0;
    /**
     * the properties for the client.
     */
    private final Properties clientInfoProperties;

    public MySQLPooledConnection pooledConnection;


    private boolean warningsCleared;
    boolean noBackslashEscapes;
    boolean nullCatalogMeansCurrent = true;
    int autoIncrementIncrement;
    Calendar cal;

    /**
     * Creates a new connection with a given protocol and query factory.
     *
     * @param protocol     the protocol to use.
     */
    private MySQLConnection( MySQLProtocol protocol) {
        this.protocol = protocol;
        clientInfoProperties = protocol.getInfo();
    }
    
    MySQLProtocol getProtocol() {
    	return protocol;
    }

    static TimeZone getTimeZone(String id) throws SQLException {
        TimeZone tz = java.util.TimeZone.getTimeZone(id);

        // Validate the timezone ID. JDK maps invalid timezones to GMT
        if (tz.getID().equals("GMT") && !id.equals("GMT")) {
            throw new SQLException("invalid timezone id '" + id + "'");
        }
        return tz;
    }
    public static MySQLConnection newConnection(MySQLProtocol protocol) throws SQLException {
        MySQLConnection connection = new MySQLConnection(protocol);

        Properties info = protocol.getInfo();
        boolean fastConnect =  info.get("fastConnect") != null ;
        String sessionVariables = info.getProperty("sessionVariables");
        String timeZoneId = info.getProperty("serverTimezone");
        if (timeZoneId != null) {
            TimeZone tz = getTimeZone(timeZoneId);
            connection.cal = Calendar.getInstance(tz);
        }
        connection.noBackslashEscapes = protocol.noBackslashEscapes();
        String nullCatalogMeansCurrentString = info.getProperty("nullCatalogMeansCurrent");
        if (nullCatalogMeansCurrentString != null && nullCatalogMeansCurrentString.equals("false")) {
            connection.nullCatalogMeansCurrent = false;
        }

        Statement st = null;
        try {
            st = connection.createStatement();
            if (sessionVariables != null) {
                st.executeUpdate("set session " + sessionVariables);
            }
            ResultSet rs = st.executeQuery("show variables like 'max_allowed_packet'");
            rs.next();
            protocol.setMaxAllowedPacket(Integer.parseInt(rs.getString(2)));
        } finally {
            if (st != null)
                st.close();
        }
        return connection;
    }


    int getAutoIncrementIncrement() {
        if(autoIncrementIncrement == 0) {
            try {
                ResultSet rs = createStatement().executeQuery("select @@auto_increment_increment");
                rs.next();
                autoIncrementIncrement = rs.getInt(1);
            } catch (SQLException e) {
                autoIncrementIncrement = 1;
            }
        }
        return autoIncrementIncrement;
    }
    /**
     * creates a new statement.
     *
     * @return a statement
     * @throws SQLException if we cannot create the statement.
     */
    public Statement createStatement() throws SQLException {
    	if (getProtocol().isClosed()) {
    		throw new SQLException("Cannot create a statement: closed connection");
    	}
        return new MySQLStatement(this);
    }

    /**
     * creates a new prepared statement. Only client side prepared statement emulation right now.
     *
     * @param sql the query.
     * @return a prepared statement.
     * @throws SQLException if there is a problem preparing the statement.
     */
    public PreparedStatement prepareStatement(final String sql) throws SQLException {
        return new MySQLPreparedStatement(this, sql);
    }


    public CallableStatement prepareCall(final String sql) throws SQLException {
       return new MySQLCallableStatement(this, sql);
    }


    public String nativeSQL(final String sql) throws SQLException {
        return Utils.nativeSQL(sql,noBackslashEscapes);
    }

    /**
     * Sets whether this connection is auto commited.
     *
     * @param autoCommit if it should be auto commited.
     * @throws SQLException if something goes wrong talking to the server.
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (autoCommit == getAutoCommit())
            return;
        
        Statement stmt = createStatement();
        try {
            stmt.executeUpdate("set autocommit="+((autoCommit)?"1":"0"));
        } finally {
            stmt.close();
        }
    }

    /**
     * returns true if statements on this connection are auto commited.
     *
     * @return true if auto commit is on.
     * @throws SQLException
     */
    public boolean getAutoCommit() throws SQLException {
        return protocol.getAutocommit();
    }

    /**
     * sends commit to the server.
     *
     * @throws SQLException if there is an error commiting.
     */
    public void commit() throws SQLException {
        Statement st = createStatement();
        try {
            st.execute("COMMIT");
        } finally {
            st.close();
        }
    }

    /**
     * rolls back a transaction.
     *
     * @throws SQLException if there is an error rolling back.
     */
    public void rollback() throws SQLException {
        Statement st = createStatement();
        try {
            st.execute("ROLLBACK");
        } finally {
            st.close();
        }
    }

    /**
     * close the connection.
     *
     * @throws SQLException if there is a problem talking to the server.
     */
    public void close() throws SQLException {
        if (pooledConnection != null) {
            if (protocol != null && protocol.inTransaction()) {
                /* Rollback transaction prior to returning physical connection to the pool */
                rollback();
            }
            pooledConnection.fireConnectionClosed();
            return;
        }
        protocol.close();
    }

    /**
     * checks if the connection is closed.
     *
     * @return true if the connection is closed
     * @throws SQLException if the connection cannot be closed.
     */
    public boolean isClosed() throws SQLException {
        return protocol.isClosed();
    }

    /**
     * returns the meta data about the database.
     *
     * @return meta data about the db.
     * @throws SQLException if there is a problem creating the meta data.
     */
    public DatabaseMetaData getMetaData() throws SQLException {
        return new MySQLDatabaseMetaData(this,protocol.getUsername(),
        		"jdbc:mysql://" + protocol.getHost()  + ":" + protocol.getPort() + "/" + protocol.getDatabase());
    }

    /**
     * Sets whether this connection is read only.
     *
     * @param readOnly true if it should be read only.
     * @throws SQLException if there is a problem
     */
    public void setReadOnly(final boolean readOnly) throws SQLException {
        protocol.setReadonly(readOnly);
    }

    /**
     * Retrieves whether this <code>Connection</code> object is in read-only mode.
     *
     * @return <code>true</code> if this <code>Connection</code> object is read-only; <code>false</code> otherwise
     * @throws java.sql.SQLException SQLException if a database access error occurs or this method is called on a closed
     *                               connection
     */
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    public static String quoteIdentifier(String s) {
      return "`" + s.replaceAll("`","``") + "`";
    }
    
    public static String unquoteIdentifier(String s) {
    	if (s != null && s.startsWith("`") && s.endsWith("`") && s.length()>= 2) {
    		return s.substring(1, s.length()-1).replace("``", "`");
    	}
    	return s;
    }
    /**
     * Sets the given catalog name in order to select a subspace of this <code>Connection</code> object's database in
     * which to work.
     * <p/>
     * If the driver does not support catalogs, it will silently ignore this request.
     * 
     * MySQL treats catalogs and databases as equivalent
     *
     * @param catalog the name of a catalog (subspace in this <code>Connection</code> object's database) in which to
     *                work
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed connection
     * @see #getCatalog
     */
    public void setCatalog(final String catalog) throws SQLException {
    	if (catalog == null){
    		throw new SQLException("The catalog name may not be null", "XAE05");
    	}
        Statement st = createStatement();
        try {
            /* Quote modifiers correctly, with backtick char */
            st.execute("USE "+ quoteIdentifier(catalog) );
            st.close();
        } finally {
            st.close();
        }
    }

    /**
     * Retrieves this <code>Connection</code> object's current catalog name.
     * <p/>
     * catalogs are not supported in drizzle
     * <p/>
     * TODO: Explain the wrapper interface to be able to change database
     *
     * @return the current catalog name or <code>null</code> if there is none
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed connection
     * @see #setCatalog
     */
    public String getCatalog() throws SQLException {
    	String catalog = null;
        Statement st = null;
        try {
            st = createStatement();
            ResultSet rs = st.executeQuery("select database()");
            rs.next();
            catalog = rs.getString(1);
        } finally {
            if (st != null)
                st.close();
        }
        return catalog;
    }

    /**
     * Attempts to change the transaction isolation level for this <code>Connection</code> object to the one given. The
     * constants defined in the interface <code>Connection</code> are the possible transaction isolation levels.
     * <p/>
     * <B>Note:</B> If this method is called during a transaction, the result is implementation-defined.
     *
     * @param level one of the following <code>Connection</code> constants: <code>Connection.TRANSACTION_READ_UNCOMMITTED</code>,
     *              <code>Connection.TRANSACTION_READ_COMMITTED</code>, <code>Connection.TRANSACTION_REPEATABLE_READ</code>,
     *              or <code>Connection.TRANSACTION_SERIALIZABLE</code>. (Note that <code>Connection.TRANSACTION_NONE</code>
     *              cannot be used because it specifies that transactions are not supported.)
     * @throws java.sql.SQLException if a database access error occurs, this method is called on a closed connection or
     *                               the given parameter is not one of the <code>Connection</code> constants
     * @see java.sql.DatabaseMetaData#supportsTransactionIsolationLevel
     * @see #getTransactionIsolation
     */
    public void setTransactionIsolation(final int level) throws SQLException {
        String query = "SET SESSION TRANSACTION ISOLATION LEVEL";
        switch (level) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                query += " READ UNCOMMITTED";
                break;
            case Connection.TRANSACTION_READ_COMMITTED:
                query += " READ COMMITTED";
                break;
            case Connection.TRANSACTION_REPEATABLE_READ:
                query += " REPEATABLE READ";
                break;
            case Connection.TRANSACTION_SERIALIZABLE:
                query += " SERIALIZABLE";
                break;
            default:
                throw SQLExceptionMapper.getSQLException("Unsupported transaction isolation level");
        }

        Statement st = createStatement();
        try {
            st.execute(query);
        } finally {
            st.close();
        }

    }

    /**
     * Retrieves this <code>Connection</code> object's current transaction isolation level.
     *
     * @return the current transaction isolation level, which will be one of the following constants:
     *         <code>Connection.TRANSACTION_READ_UNCOMMITTED</code>, <code>Connection.TRANSACTION_READ_COMMITTED</code>,
     *         <code>Connection.TRANSACTION_REPEATABLE_READ</code>, <code>Connection.TRANSACTION_SERIALIZABLE</code>, or
     *         <code>Connection.TRANSACTION_NONE</code>.
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed connection
     * @see #setTransactionIsolation
     */
    public int getTransactionIsolation() throws SQLException {
        final Statement stmt = createStatement();
        try {
            final ResultSet rs = stmt.executeQuery("SELECT @@tx_isolation");
            rs.next();
            final String response = rs.getString(1);
            if (response.equals("REPEATABLE-READ")) {
                return Connection.TRANSACTION_REPEATABLE_READ;
            }
            if (response.equals("READ-UNCOMMITTED")) {
                return Connection.TRANSACTION_READ_UNCOMMITTED;
            }
            if (response.equals("READ-COMMITTED")) {
                return Connection.TRANSACTION_READ_COMMITTED;
            }
            if (response.equals("SERIALIZABLE")) {
                return Connection.TRANSACTION_SERIALIZABLE;
            }
        } finally {
            stmt.close();
        }
        throw SQLExceptionMapper.getSQLException("Could not get transaction isolation level");
    }

    /**
     * Not yet implemented: Protocol needs to store any warnings related to connections
     * <p/>
     * <p/>
     * Retrieves the first warning reported by calls on this <code>Connection</code> object.  If there is more than one
     * warning, subsequent warnings will be chained to the first one and can be retrieved by calling the method
     * <code>SQLWarning.getNextWarning</code> on the warning that was retrieved previously.
     * <p/>
     * This method may not be called on a closed connection; doing so will cause an <code>SQLException</code> to be
     * thrown.
     * <p/>
     * <P><B>Note:</B> Subsequent warnings will be chained to this SQLWarning.
     *
     * @return the first <code>SQLWarning</code> object or <code>null</code> if there are none
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed connection
     * @see java.sql.SQLWarning
     */
    public SQLWarning getWarnings() throws SQLException {
        if (warningsCleared || isClosed() || !protocol.hasWarnings) {
            return null;
        }
        Statement st = null;
        ResultSet rs = null;
        SQLWarning last = null;
        SQLWarning first = null;
        try {
           st = this.createStatement();
           rs = st.executeQuery("show warnings");
           // returned result set has 'level', 'code' and 'message' columns, in this order.
           while(rs.next()) {
               int code = rs.getInt(2);
               String message = rs.getString(3);
               SQLWarning w = new SQLWarning(message, SQLExceptionMapper.mapMySQLCodeToSQLState(code), code);
               if (first == null) {
                   first = w;
                   last = w;
               }
               else {
                   last.setNextWarning(w);
                   last = w;
               }
           }
        }
        finally {
           if (rs != null)
               rs.close();
            if(st != null)
                st.close();
        }
        return first;
    }

    /**
     * Clears all warnings reported for this <code>Connection</code> object. After a call to this method, the method
     * <code>getWarnings</code> returns <code>null</code> until a new warning is reported for this
     * <code>Connection</code> object.
     *
     * @throws java.sql.SQLException SQLException if a database access error occurs or this method is called on a closed
     *                               connection
     */
    public void clearWarnings() throws SQLException {
        warningsCleared = true;
    }

    /**
     * Reenable warnings, when next statement is executed
     */
    public void reenableWarnings() {
        warningsCleared = false;
    }

    /**
     * Creates a <code>Statement</code> object that will generate <code>ResultSet</code> objects with the given type and
     * concurrency. This method is the same as the <code>createStatement</code> method above, but it allows the default
     * result set type and concurrency to be overridden. The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * @param resultSetType        a result set type; one of <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                             <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency a concurrency type; one of <code>ResultSet.CONCUR_READ_ONLY</code> or
     *                             <code>ResultSet.CONCUR_UPDATABLE</code>
     * @return a new <code>Statement</code> object that will generate <code>ResultSet</code> objects with the given type
     *         and concurrency
     * @throws java.sql.SQLException if a database access error occurs, this method is called on a closed connection or
     *                               the given parameters are not <code>ResultSet</code> constants indicating type and
     *                               concurrency
     */
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {
        // for now resultSetType and resultSetConcurrency are ignored
        // TODO: fix
        return createStatement();
    }

    /**
     * Creates a <code>PreparedStatement</code> object that will generate <code>ResultSet</code> objects with the given
     * type and concurrency. This method is the same as the <code>prepareStatement</code> method above, but it allows
     * the default result set type and concurrency to be overridden. The holdability of the created result sets can be
     * determined by calling {@link #getHoldability}.
     *
     * @param sql                  a <code>String</code> object that is the SQL statement to be sent to the database;
     *                             may contain one or more '?' IN parameters
     * @param resultSetType        a result set type; one of <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                             <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency a concurrency type; one of <code>ResultSet.CONCUR_READ_ONLY</code> or
     *                             <code>ResultSet.CONCUR_UPDATABLE</code>
     * @return a new PreparedStatement object containing the pre-compiled SQL statement that will produce
     *         <code>ResultSet</code> objects with the given type and concurrency
     * @throws java.sql.SQLException if a database access error occurs, this method is called on a closed connection or
     *                               the given parameters are not <code>ResultSet</code> constants indicating type and
     *                               concurrency
     */
    public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency)
            throws SQLException {
        // for now resultSetType and resultSetConcurrency are ignored
        // TODO: fix
        return prepareStatement(sql);
    }

    /**
     * Creates a <code>CallableStatement</code> object that will generate <code>ResultSet</code> objects with the given
     * type and concurrency. This method is the same as the <code>prepareCall</code> method above, but it allows the
     * default result set type and concurrency to be overridden. The holdability of the created result sets can be
     * determined by calling {@link #getHoldability}.
     *
     * @param sql                  a <code>String</code> object that is the SQL statement to be sent to the database;
     *                             may contain on or more '?' parameters
     * @param resultSetType        a result set type; one of <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                             <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency a concurrency type; one of <code>ResultSet.CONCUR_READ_ONLY</code> or
     *                             <code>ResultSet.CONCUR_UPDATABLE</code>
     * @return a new <code>CallableStatement</code> object containing the pre-compiled SQL statement that will produce
     *         <code>ResultSet</code> objects with the given type and concurrency
     * @throws java.sql.SQLException if a database access error occurs, this method is called on a closed connection or
     *                               the given parameters are not <code>ResultSet</code> constants indicating type and
     *                               concurrency
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method or this method is not supported for
     *                               the specified result set type and result set concurrency.
     */
    public CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException {
        return new MySQLCallableStatement(this,sql);
    }

    /**
     * Retrieves the <code>Map</code> object associated with this <code>Connection</code> object. Unless the application
     * has added an entry, the type map returned will be empty.
     *
     * @return the <code>java.util.Map</code> object associated with this <code>Connection</code> object
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed connection
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @see #setTypeMap
     * @since 1.2
     */
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return  null;
    }

    /**
     * Installs the given <code>TypeMap</code> object as the type map for this <code>Connection</code> object.  The type
     * map will be used for the custom mapping of SQL structured types and distinct types.
     *
     * @param map the <code>java.util.Map</code> object to install as the replacement for this <code>Connection</code>
     *            object's default type map
     * @throws java.sql.SQLException if a database access error occurs, this method is called on a closed connection or
     *                               the given parameter is not a <code>java.util.Map</code> object
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @see #getTypeMap
     */
    public void setTypeMap(final Map<String, Class<?>> map) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Not yet supported");
    }

    /**
     * Changes the default holdability of <code>ResultSet</code> objects created using this <code>Connection</code>
     * object to the given holdability.  The default holdability of <code>ResultSet</code> objects can be be determined
     * by invoking {@link java.sql.DatabaseMetaData#getResultSetHoldability}.
     *
     * @param holdability a <code>ResultSet</code> holdability constant; one of <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code>
     *                    or <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @throws java.sql.SQLException if a database access occurs, this method is called on a closed connection, or the
     *                               given parameter is not a <code>ResultSet</code> constant indicating holdability
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the given holdability is not supported
     * @see #getHoldability
     * @see java.sql.DatabaseMetaData#getResultSetHoldability
     * @see java.sql.ResultSet
     * @since 1.4
     */
    public void setHoldability(final int holdability) throws SQLException {

    }

    /**
     * Retrieves the current holdability of <code>ResultSet</code> objects created using this <code>Connection</code>
     * object.
     *
     * @return the holdability, one of <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *         <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed connection
     * @see #setHoldability
     * @see java.sql.DatabaseMetaData#getResultSetHoldability
     * @see java.sql.ResultSet
     * @since 1.4
     */
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    /**
     * Creates an unnamed savepoint in the current transaction and returns the new <code>Savepoint</code> object that
     * represents it.
     * <p/>
     * <p> if setSavepoint is invoked outside of an active transaction, a transaction will be started at this newly
     * created savepoint.
     *
     * @return the new <code>Savepoint</code> object
     * @throws java.sql.SQLException if a database access error occurs, this method is called while participating in a
     *                               distributed transaction, this method is called on a closed connection or this
     *                               <code>Connection</code> object is currently in auto-commit mode
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @see java.sql.Savepoint
     * @since 1.4
     */
    public Savepoint setSavepoint() throws SQLException {
        return setSavepoint("unnamed");
    }

    /**
     * Creates a savepoint with the given name in the current transaction and returns the new <code>Savepoint</code>
     * object that represents it.
     * <p/>
     * <p> if setSavepoint is invoked outside of an active transaction, a transaction will be started at this newly
     * created savepoint.
     *
     * @param name a <code>String</code> containing the name of the savepoint
     * @return the new <code>Savepoint</code> object
     * @throws java.sql.SQLException if a database access error occurs, this method is called while participating in a
     *                               distributed transaction, this method is called on a closed connection or this
     *                               <code>Connection</code> object is currently in auto-commit mode
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @see java.sql.Savepoint
     * @since 1.4
     */
    public Savepoint setSavepoint(final String name) throws SQLException {
        Savepoint savepoint = new MySQLSavepoint(name, savepointCount++);
        Statement st = createStatement();
        st.execute("SAVEPOINT " + savepoint.toString());
        return savepoint;

    }

    /**
     * Undoes all changes made after the given <code>Savepoint</code> object was set.
     * <p/>
     * This method should be used only when auto-commit has been disabled.
     *
     * @param savepoint the <code>Savepoint</code> object to roll back to
     * @throws java.sql.SQLException if a database access error occurs, this method is called while participating in a
     *                               distributed transaction, this method is called on a closed connection, the
     *                               <code>Savepoint</code> object is no longer valid, or this <code>Connection</code>
     *                               object is currently in auto-commit mode
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @see java.sql.Savepoint
     * @see #rollback
     * @since 1.4
     */
    public void rollback(final Savepoint savepoint) throws SQLException {
       Statement st = createStatement();
       st.execute("ROLLBACK TO SAVEPOINT " + savepoint.toString());
       st.close();
    }

    /**
     * Removes the specified <code>Savepoint</code>  and subsequent <code>Savepoint</code> objects from the current
     * transaction. Any reference to the savepoint after it have been removed will cause an <code>SQLException</code> to
     * be thrown.
     *
     * @param savepoint the <code>Savepoint</code> object to be removed
     * @throws java.sql.SQLException if a database access error occurs, this method is called on a closed connection or
     *                               the given <code>Savepoint</code> object is not a valid savepoint in the current
     *                               transaction
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public void releaseSavepoint(final Savepoint savepoint) throws SQLException {
       Statement st = createStatement();
       st.execute("RELEASE SAVEPOINT " + savepoint.toString());
       st.close();
    }

    /**
     * Creates a <code>Statement</code> object that will generate <code>ResultSet</code> objects with the given type,
     * concurrency, and holdability. This method is the same as the <code>createStatement</code> method above, but it
     * allows the default result set type, concurrency, and holdability to be overridden.
     *
     * @param resultSetType        one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                             <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency one of the following <code>ResultSet</code> constants: <code>ResultSet.CONCUR_READ_ONLY</code>
     *                             or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param resultSetHoldability one of the following <code>ResultSet</code> constants: <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code>
     *                             or <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @return a new <code>Statement</code> object that will generate <code>ResultSet</code> objects with the given
     *         type, concurrency, and holdability
     * @throws java.sql.SQLException if a database access error occurs, this method is called on a closed connection or
     *                               the given parameters are not <code>ResultSet</code> constants indicating type,
     *                               concurrency, and holdability
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method or this method is not supported for
     *                               the specified result set type, result set holdability and result set concurrency.
     * @see java.sql.ResultSet
     * @since 1.4
     */
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability)
            throws SQLException {
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw SQLExceptionMapper.getFeatureNotSupportedException("Only read-only result sets allowed");
        }

        return createStatement();
    }

    /**
     * Creates a <code>PreparedStatement</code> object that will generate <code>ResultSet</code> objects with the given
     * type, concurrency, and holdability.
     * <p/>
     * This method is the same as the <code>prepareStatement</code> method above, but it allows the default result set
     * type, concurrency, and holdability to be overridden.
     *
     * @param sql                  a <code>String</code> object that is the SQL statement to be sent to the database;
     *                             may contain one or more '?' IN parameters
     * @param resultSetType        one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                             <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency one of the following <code>ResultSet</code> constants: <code>ResultSet.CONCUR_READ_ONLY</code>
     *                             or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param resultSetHoldability one of the following <code>ResultSet</code> constants: <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code>
     *                             or <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled SQL statement, that will
     *         generate <code>ResultSet</code> objects with the given type, concurrency, and holdability
     * @throws java.sql.SQLException if a database access error occurs, this method is called on a closed connection or
     *                               the given parameters are not <code>ResultSet</code> constants indicating type,
     *                               concurrency, and holdability
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method or this method is not supported for
     *                               the specified result set type, result set holdability and result set concurrency.
     * @see java.sql.ResultSet
     * @since 1.4
     */
    public PreparedStatement prepareStatement(final String sql,
                                              final int resultSetType,
                                              final int resultSetConcurrency,
                                              final int resultSetHoldability) throws SQLException {
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw SQLExceptionMapper.getFeatureNotSupportedException("Only read-only result sets allowed");
        }
        // resultSetType is ignored since we always are scroll insensitive
        return prepareStatement(sql);
    }

    /**
     * Creates a <code>CallableStatement</code> object that will generate <code>ResultSet</code> objects with the given
     * type and concurrency. This method is the same as the <code>prepareCall</code> method above, but it allows the
     * default result set type, result set concurrency type and holdability to be overridden.
     *
     * @param sql                  a <code>String</code> object that is the SQL statement to be sent to the database;
     *                             may contain on or more '?' parameters
     * @param resultSetType        one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                             <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency one of the following <code>ResultSet</code> constants: <code>ResultSet.CONCUR_READ_ONLY</code>
     *                             or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param resultSetHoldability one of the following <code>ResultSet</code> constants: <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code>
     *                             or <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @return a new <code>CallableStatement</code> object, containing the pre-compiled SQL statement, that will
     *         generate <code>ResultSet</code> objects with the given type, concurrency, and holdability
     * @throws java.sql.SQLException if a database access error occurs, this method is called on a closed connection or
     *                               the given parameters are not <code>ResultSet</code> constants indicating type,
     *                               concurrency, and holdability
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method or this method is not supported for
     *                               the specified result set type, result set holdability and result set concurrency.
     * @see java.sql.ResultSet
     * @since 1.4
     */
    public CallableStatement prepareCall(final String sql,
                                         final int resultSetType,
                                         final int resultSetConcurrency,
                                         final int resultSetHoldability) throws SQLException {
        return prepareCall(sql);
    }

    /**
     * Creates a default <code>PreparedStatement</code> object that has the capability to retrieve auto-generated keys.
     * The given constant tells the driver whether it should make auto-generated keys available for retrieval.  This
     * parameter is ignored if the SQL statement is not an <code>INSERT</code> statement, or an SQL statement able to
     * return auto-generated keys (the list of such statements is vendor-specific).
     * <p/>
     * <B>Note:</B> This method is optimized for handling parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation, the method <code>prepareStatement</code> will send the statement to the
     * database for precompilation. Some drivers may not support precompilation. In this case, the statement may not be
     * sent to the database until the <code>PreparedStatement</code> object is executed.  This has no direct effect on
     * users; however, it does affect which methods throw certain SQLExceptions.
     * <p/>
     * Result sets created using the returned <code>PreparedStatement</code> object will by default be type
     * <code>TYPE_FORWARD_ONLY</code> and have a concurrency level of <code>CONCUR_READ_ONLY</code>. The holdability of
     * the created result sets can be determined by calling {@link #getHoldability}.
     *
     * @param sql               an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param autoGeneratedKeys a flag indicating whether auto-generated keys should be returned; one of
     *                          <code>Statement.RETURN_GENERATED_KEYS</code> or <code>Statement.NO_GENERATED_KEYS</code>
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled SQL statement, that will have
     *         the capability of returning auto-generated keys
     * @throws java.sql.SQLException if a database access error occurs, this method is called on a closed connection or
     *                               the given parameter is not a <code>Statement</code> constant indicating whether
     *                               auto-generated keys should be returned
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method with a constant of
     *                               Statement.RETURN_GENERATED_KEYS
     * @since 1.4
     */
    public PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    /**
     * Creates a default <code>PreparedStatement</code> object capable of returning the auto-generated keys designated
     * by the given array. This array contains the indexes of the columns in the target table that contain the
     * auto-generated keys that should be made available.  The driver will ignore the array if the SQL statement is not
     * an <code>INSERT</code> statement, or an SQL statement able to return auto-generated keys (the list of such
     * statements is vendor-specific).
     * <p/>
     * An SQL statement with or without IN parameters can be pre-compiled and stored in a <code>PreparedStatement</code>
     * object. This object can then be used to efficiently execute this statement multiple times.
     * <p/>
     * <B>Note:</B> This method is optimized for handling parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation, the method <code>prepareStatement</code> will send the statement to the
     * database for precompilation. Some drivers may not support precompilation. In this case, the statement may not be
     * sent to the database until the <code>PreparedStatement</code> object is executed.  This has no direct effect on
     * users; however, it does affect which methods throw certain SQLExceptions.
     * <p/>
     * Result sets created using the returned <code>PreparedStatement</code> object will by default be type
     * <code>TYPE_FORWARD_ONLY</code> and have a concurrency level of <code>CONCUR_READ_ONLY</code>. The holdability of
     * the created result sets can be determined by calling {@link #getHoldability}.
     *
     * @param sql           an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param columnIndexes an array of column indexes indicating the columns that should be returned from the inserted
     *                      row or rows
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled statement, that is capable of
     *         returning the auto-generated keys designated by the given array of column indexes
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed connection
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public PreparedStatement prepareStatement(final String sql, final int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    /**
     * Creates a default <code>PreparedStatement</code> object capable of returning the auto-generated keys designated
     * by the given array. This array contains the names of the columns in the target table that contain the
     * auto-generated keys that should be returned. The driver will ignore the array if the SQL statement is not an
     * <code>INSERT</code> statement, or an SQL statement able to return auto-generated keys (the list of such
     * statements is vendor-specific).
     * <p/>
     * An SQL statement with or without IN parameters can be pre-compiled and stored in a <code>PreparedStatement</code>
     * object. This object can then be used to efficiently execute this statement multiple times.
     * <p/>
     * <B>Note:</B> This method is optimized for handling parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation, the method <code>prepareStatement</code> will send the statement to the
     * database for precompilation. Some drivers may not support precompilation. In this case, the statement may not be
     * sent to the database until the <code>PreparedStatement</code> object is executed.  This has no direct effect on
     * users; however, it does affect which methods throw certain SQLExceptions.
     * <p/>
     * Result sets created using the returned <code>PreparedStatement</code> object will by default be type
     * <code>TYPE_FORWARD_ONLY</code> and have a concurrency level of <code>CONCUR_READ_ONLY</code>. The holdability of
     * the created result sets can be determined by calling {@link #getHoldability}.
     *
     * @param sql         an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param columnNames an array of column names indicating the columns that should be returned from the inserted row
     *                    or rows
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled statement, that is capable of
     *         returning the auto-generated keys designated by the given array of column names
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed connection
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public PreparedStatement prepareStatement(final String sql, final String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    /**
     * Constructs an object that implements the <code>Clob</code> interface. The object returned initially contains no
     * data.  The <code>setAsciiStream</code>, <code>setCharacterStream</code> and <code>setString</code> methods of the
     * <code>Clob</code> interface may be used to add data to the <code>Clob</code>.
     *
     * @return An object that implements the <code>Clob</code> interface
     * @throws java.sql.SQLException if an object that implements the <code>Clob</code> interface can not be
     *                               constructed, this method is called on a closed connection or a database access
     *                               error occurs.
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this data type
     * @since 1.6
     */
    public Clob createClob() throws SQLException {
        return new MySQLClob();
    }

    /**
     * Constructs an object that implements the <code>Blob</code> interface. The object returned initially contains no
     * data.  The <code>setBinaryStream</code> and <code>setBytes</code> methods of the <code>Blob</code> interface may
     * be used to add data to the <code>Blob</code>.
     *
     * @return An object that implements the <code>Blob</code> interface
     * @throws java.sql.SQLException if an object that implements the <code>Blob</code> interface can not be
     *                               constructed, this method is called on a closed connection or a database access
     *                               error occurs.
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this data type
     * @since 1.6
     */
    public Blob createBlob() throws SQLException {
        return new MySQLBlob();
    }

    /**
     * Constructs an object that implements the <code>NClob</code> interface. The object returned initially contains no
     * data.  The <code>setAsciiStream</code>, <code>setCharacterStream</code> and <code>setString</code> methods of the
     * <code>NClob</code> interface may be used to add data to the <code>NClob</code>.
     *
     * @return An object that implements the <code>NClob</code> interface
     * @throws java.sql.SQLException if an object that implements the <code>NClob</code> interface can not be
     *                               constructed, this method is called on a closed connection or a database access
     *                               error occurs.
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this data type
     * @since 1.6
     */
    public java.sql.NClob createNClob() throws SQLException {
        return new MySQLClob();
    }

    /**
     * Constructs an object that implements the <code>SQLXML</code> interface. The object returned initially contains no
     * data. The <code>createXmlStreamWriter</code> object and <code>setString</code> method of the <code>SQLXML</code>
     * interface may be used to add data to the <code>SQLXML</code> object.
     *
     * @return An object that implements the <code>SQLXML</code> interface
     * @throws java.sql.SQLException if an object that implements the <code>SQLXML</code> interface can not be
     *                               constructed, this method is called on a closed connection or a database access
     *                               error occurs.
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this data type
     * @since 1.6
     */
    public java.sql.SQLXML createSQLXML() throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Not supported");
    }

    /**
     * Returns true if the connection has not been closed and is still valid. The driver shall submit a query on the
     * connection or use some other mechanism that positively verifies the connection is still valid when this method is
     * called.
     * <p/>
     * The query submitted by the driver to validate the connection shall be executed in the context of the current
     * transaction.
     *
     * @param timeout -             The time in seconds to wait for the database operation used to validate the
     *                connection to complete.  If the timeout period expires before the operation completes, this method
     *                returns false.  A value of 0 indicates a timeout is not applied to the database operation.
     *                <p/>
     * @return true if the connection is valid, false otherwise
     * @throws java.sql.SQLException if the value supplied for <code>timeout</code> is less then 0
     * @see java.sql.DatabaseMetaData#getClientInfoProperties
     * @since 1.6
     *        <p/>
     */
    public boolean isValid(final int timeout) throws SQLException {
    	if (timeout < 0) {
    		throw new SQLException("the value supplied for timeout is negative");
    	}
    	if (isClosed()) {
    		return false;
    	}
        try {
            return protocol.ping();
        } catch (QueryException e) {
        	return false;
        }
    }

    /**
     * Sets the value of the client info property specified by name to the value specified by value.
     * <p/>
     * Applications may use the <code>DatabaseMetaData.getClientInfoProperties</code> method to determine the client
     * info properties supported by the driver and the maximum length that may be specified for each property.
     * <p/>
     * The driver stores the value specified in a suitable location in the database.  For example in a special register,
     * session parameter, or system table column.  For efficiency the driver may defer setting the value in the database
     * until the next time a statement is executed or prepared.  Other than storing the client information in the
     * appropriate place in the database, these methods shall not alter the behavior of the connection in anyway.  The
     * values supplied to these methods are used for accounting, diagnostics and debugging purposes only.
     * <p/>
     * The driver shall generate a warning if the client info name specified is not recognized by the driver.
     * <p/>
     * If the value specified to this method is greater than the maximum length for the property the driver may either
     * truncate the value and generate a warning or generate a <code>SQLClientInfoException</code>.  If the driver
     * generates a <code>SQLClientInfoException</code>, the value specified was not set on the connection.
     * <p/>
     * The following are standard client info properties.  Drivers are not required to support these properties however
     * if the driver supports a client info property that can be described by one of the standard properties, the
     * standard property name should be used.
     * <p/>
     * <ul> <li>ApplicationName  -       The name of the application currently utilizing the connection</li>
     * <li>ClientUser               -       The name of the user that the application using the connection is performing
     * work for.  This may not be the same as the user name that was used in establishing the connection.</li>
     * <li>ClientHostname   -       The hostname of the computer the application using the connection is running
     * on.</li> </ul>
     * <p/>
     *
     * @param name  The name of the client info property to set
     * @param value The value to set the client info property to.  If the value is null, the current value of the
     *              specified property is cleared.
     *              <p/>
     * @throws java.sql.SQLClientInfoException
     *          if the database server returns an error while setting the client info value on the database server or
     *          this method is called on a closed connection
     *          <p/>
     * @since 1.6
     */
    public void setClientInfo(final String name, final String value) throws java.sql.SQLClientInfoException {
        this.clientInfoProperties.setProperty(name, value);
    }

    /**
     * Sets the value of the connection's client info properties.  The <code>Properties</code> object contains the names
     * and values of the client info properties to be set.  The set of client info properties contained in the
     * properties list replaces the current set of client info properties on the connection.  If a property that is
     * currently set on the connection is not present in the properties list, that property is cleared.  Specifying an
     * empty properties list will clear all of the properties on the connection.  See <code>setClientInfo (String,
     * String)</code> for more information.
     * <p/>
     * If an error occurs in setting any of the client info properties, a <code>SQLClientInfoException</code> is thrown.
     * The <code>SQLClientInfoException</code> contains information indicating which client info properties were not
     * set. The state of the client information is unknown because some databases do not allow multiple client info
     * properties to be set atomically.  For those databases, one or more properties may have been set before the error
     * occurred.
     * <p/>
     *
     * @param properties the list of client info properties to set
     *                   <p/>
     * @throws java.sql.SQLClientInfoException
     *          if the database server returns an error while setting the clientInfo values on the database server or
     *          this method is called on a closed connection
     *          <p/>
     * @see java.sql.Connection#setClientInfo(String, String) setClientInfo(String, String)
     * @since 1.6
     *        <p/>
     */
    public void setClientInfo(final Properties properties) throws java.sql.SQLClientInfoException {
        // TODO: actually use these!
        for (final String key : properties.stringPropertyNames()) {
            this.clientInfoProperties.setProperty(key, properties.getProperty(key));
        }
    }

    /**
     * Returns the value of the client info property specified by name.  This method may return null if the specified
     * client info property has not been set and does not have a default value.  This method will also return null if
     * the specified client info property name is not supported by the driver.
     * <p/>
     * Applications may use the <code>DatabaseMetaData.getClientInfoProperties</code> method to determine the client
     * info properties supported by the driver.
     * <p/>
     *
     * @param name The name of the client info property to retrieve
     *             <p/>
     * @return The value of the client info property specified
     *         <p/>
     * @throws java.sql.SQLException if the database server returns an error when fetching the client info value from
     *                               the database or this method is called on a closed connection
     *                               <p/>
     * @see java.sql.DatabaseMetaData#getClientInfoProperties
     * @since 1.6
     *        <p/>
     */
    public String getClientInfo(final String name) throws SQLException {
        return clientInfoProperties.getProperty(name);
    }

    /**
     * Returns a list containing the name and current value of each client info property supported by the driver.  The
     * value of a client info property may be null if the property has not been set and does not have a default value.
     * <p/>
     *
     * @return A <code>Properties</code> object that contains the name and current value of each of the client info
     *         properties supported by the driver.
     *         <p/>
     * @throws java.sql.SQLException if the database server returns an error when fetching the client info values from
     *                               the database or this method is called on a closed connection
     *                               <p/>
     * @since 1.6
     */
    public Properties getClientInfo() throws SQLException {
        return clientInfoProperties;
    }

    /**
     * Factory method for creating Array objects.
     * <p/>
     * <b>Note: </b>When <code>createArrayOf</code> is used to create an array object that maps to a primitive data
     * type, then it is implementation-defined whether the <code>Array</code> object is an array of that primitive data
     * type or an array of <code>Object</code>.
     * <p/>
     * <b>Note: </b>The JDBC driver is responsible for mapping the elements <code>Object</code> array to the default
     * JDBC SQL type defined in java.sql.Types for the given class of <code>Object</code>. The default mapping is
     * specified in Appendix B of the JDBC specification.  If the resulting JDBC type is not the appropriate type for
     * the given typeName then it is implementation defined whether an <code>SQLException</code> is thrown or the driver
     * supports the resulting conversion.
     *
     * @param typeName the SQL name of the type the elements of the array map to. The typeName is a database-specific
     *                 name which may be the name of a built-in type, a user-defined type or a standard  SQL type
     *                 supported by this database. This is the value returned by <code>Array.getBaseTypeName</code>
     * @param elements the elements that populate the returned object
     * @return an Array object whose elements map to the specified SQL type
     * @throws java.sql.SQLException if a database error occurs, the JDBC type is not appropriate for the typeName and
     *                               the conversion is not supported, the typeName is null or this method is called on a
     *                               closed connection
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this data type
     * @since 1.6
     */
    public Array createArrayOf(final String typeName, final Object[] elements) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Not yet supported");
    }

    /**
     * Factory method for creating Struct objects.
     *
     * @param typeName   the SQL type name of the SQL structured type that this <code>Struct</code> object maps to. The
     *                   typeName is the name of  a user-defined type that has been defined for this database. It is the
     *                   value returned by <code>Struct.getSQLTypeName</code>.
     * @param attributes the attributes that populate the returned object
     * @return a Struct object that maps to the given SQL type and is populated with the given attributes
     * @throws java.sql.SQLException if a database error occurs, the typeName is null or this method is called on a
     *                               closed connection
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this data type
     * @since 1.6
     */
    public Struct createStruct(final String typeName, final Object[] attributes) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Not yet supported");
    }

    /**
     * Returns an object that implements the given interface to allow access to non-standard methods, or standard
     * methods not exposed by the proxy.
     * <p/>
     * If the receiver implements the interface then the result is the receiver or a proxy for the receiver. If the
     * receiver is a wrapper and the wrapped object implements the interface then the result is the wrapped object or a
     * proxy for the wrapped object. Otherwise return the the result of calling <code>unwrap</code> recursively on the
     * wrapped object or a proxy for that result. If the receiver is not a wrapper and does not implement the interface,
     * then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException If no object found that implements the interface
     * @since 1.6
     */
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return iface.cast(this);
    }

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper for an
     * object that does. Returns false otherwise. If this implements the interface then return true, else if this is a
     * wrapper then return the result of recursively calling <code>isWrapperFor</code> on the wrapped object. If this
     * does not implement the interface and is not a wrapper, return false. This method should be implemented as a
     * low-cost operation compared to <code>unwrap</code> so that callers can use this method to avoid expensive
     * <code>unwrap</code> calls that may fail. If this method returns true then calling <code>unwrap</code> with the
     * same argument should succeed.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws java.sql.SQLException if an error occurs while determining whether this is a wrapper for an object with
     *                               the given interface.
     * @since 1.6
     */
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    /**
     * returns the username for the connection.
     *
     * @return the username.
     */
    public String getUsername() {
        return protocol.getUsername();
    }

    /**
     * returns the password for the connection.
     *
     * @return the password.
     */
    public String getPassword() {
        return protocol.getPassword();
    }

    /**
     * returns the hostname for the connection.
     *
     * @return the hostname.
     */
    public String getHostname() {
        return protocol.getHost();
    }

    /**
     * returns the port for the connection.
     *
     * @return the port
     */
    public int getPort() {
        return protocol.getPort();
    }

    /**
     * returns the database.
     *
     * @return the database
     */
    public String getDatabase() {
        return protocol.getDatabase();
    }

   
    public void setHostFailed() {
        protocol.setHostFailed();
    }

    volatile int lowercaseTableNames = -1;
    public int getLowercaseTableNames() throws SQLException {
        if (lowercaseTableNames == -1) {
            Statement st = createStatement();
            ResultSet rs = st.executeQuery("select @@lower_case_table_names");
            rs.next();
            lowercaseTableNames  = rs.getInt(1);
        }
        return lowercaseTableNames;
    }
    
	/* (non-Javadoc)
	 * @see java.sql.Connection#abort(java.util.concurrent.Executor)
	 */
	public void abort(Executor executor) throws SQLException {
		if (this.isClosed()) {
			return;
		}
		SQLPermission sqlPermission = new SQLPermission("callAbort");
		SecurityManager securityManager = new SecurityManager();
		if (securityManager != null && sqlPermission != null) {
			securityManager.checkPermission(sqlPermission);
		}
		if (executor == null) {
			throw SQLExceptionMapper.getSQLException("Cannot abort the connection: null executor passed");
		}
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					close();
					pooledConnection = null;
				} catch (SQLException sqle) {
					throw new RuntimeException(sqle);
				}
			}
		});
	}

	public int getNetworkTimeout() throws SQLException {
		try {
			return this.protocol.getTimeout();
		} catch (SocketException se) {
			throw SQLExceptionMapper.getSQLException("Cannot retrieve the network timeout", se);
		}
	}

	public String getSchema() throws SQLException {
		  // We support only catalog 
		  return null;
	}

	/* (non-Javadoc)
	 * @see java.sql.Connection#setNetworkTimeout(java.util.concurrent.Executor, int)
	 */
	public void setNetworkTimeout(Executor executor, final int milliseconds) throws SQLException {
		if (this.isClosed()) {
			throw SQLExceptionMapper.getSQLException("Connection.setNetworkTimeout cannot be called on a closed connection");
		}
		if (milliseconds < 0) {
			throw SQLExceptionMapper.getSQLException("Connection.setNetworkTimeout cannot be called with a negative timeout");
		}
		SQLPermission sqlPermission = new SQLPermission("setNetworkTimeout");
		SecurityManager securityManager = new SecurityManager();
		if (securityManager != null && sqlPermission != null) {
			securityManager.checkPermission(sqlPermission);
		}
		if (executor == null) {
			throw SQLExceptionMapper.getSQLException("Cannot set the connection timeout: null executor passed");
		}
//		executor.execute(new Runnable() {
//			@Override
//			public void run() {
//				try {
//					protocol.setTimeout(milliseconds);
//				} catch (SocketException se) {
//					throw new RuntimeException(SQLExceptionMapper.getSQLException("Cannot set the network timeout", se));
//				}
//			}
//		});
		try {
			protocol.setTimeout(milliseconds);
		} catch (SocketException se) {
			throw SQLExceptionMapper.getSQLException("Cannot set the network timeout", se);
		}
	}

	public void setSchema(String arg0) throws SQLException {
		  // We support only catalog 
      throw SQLExceptionMapper.getFeatureNotSupportedException("Only catalogs are supported");
	}
}
