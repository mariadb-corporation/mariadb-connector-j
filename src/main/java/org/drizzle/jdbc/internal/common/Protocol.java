/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common;

import org.drizzle.jdbc.internal.common.packet.RawPacket;
import org.drizzle.jdbc.internal.common.query.Query;
import org.drizzle.jdbc.internal.common.queryresults.QueryResult;

import java.util.List;

/**
 * User: marcuse Date: Jan 14, 2009 Time: 4:05:47 PM
 */
public interface Protocol {
    /**
     * closes the connection to the server
     *
     * @throws QueryException if there is a communication problem with the server
     */
    void close() throws QueryException;

    /**
     * returns true if the connection has been closed
     *
     * @return true if the connection is closed
     */
    boolean isClosed();

    /**
     * selects what database to use
     *
     * @param database the database
     * @throws QueryException if there is a problem selecting the database
     */
    void selectDB(String database) throws QueryException;

    /**
     * returns the server version string
     *
     * @return
     */
    String getServerVersion();

    /**
     * sets whether this connection should be read only
     * <p/>
     * TODO: actually enforce this
     *
     * @param readOnly if the connection should be read only
     */
    void setReadonly(boolean readOnly);

    /**
     * check if it is possble to execute writing operations on this connection
     *
     * @return true if we can update etc
     */
    boolean getReadonly();

    /**
     * commits the current transaction
     *
     * @throws QueryException if there is a problem committing the txn
     */
    void commit() throws QueryException;

    /**
     * rolls back the current transaction
     *
     * @throws QueryException if there is a problem rolling back
     */
    void rollback() throws QueryException;

    /**
     * rolls back to the given save point
     *
     * @param savepoint the save point to roll back to
     * @throws QueryException if there is a problem rolling back
     */
    void rollback(String savepoint) throws QueryException;

    /**
     * sets a save point
     *
     * @param savepoint the save point name
     * @throws QueryException if there is a problem setting the save point
     */
    void setSavepoint(String savepoint) throws QueryException;

    /**
     * releases the savepoint
     *
     * @param savepoint the name of the savepoint to release
     * @throws QueryException if there is a problem releasing the save point
     */
    void releaseSavepoint(String savepoint) throws QueryException;

    /**
     * sets whether statements should be autocommited
     *
     * @param autoCommit true if they should be autocommitted
     * @throws QueryException if there is a problem setting auto commit
     */
    void setAutoCommit(boolean autoCommit) throws QueryException;

    /**
     * returns true if we are in auto commit mode
     *
     * @return true if we are in auto commit mode
     */
    boolean getAutoCommit();

    /**
     * returns the host
     *
     * @return the host we are connected to
     */
    public String getHost();

    /**
     * returns the port we are connected to
     *
     * @return the port
     */
    public int getPort();

    /**
     * returns the current used database
     *
     * @return
     */
    public String getDatabase();

    /**
     * returns the current connected username
     *
     * @return the username
     */
    public String getUsername();

    /**
     * the current password
     *
     * @return the password
     */
    public String getPassword();

    /**
     * checks whether the connectiion is still valid
     *
     * @return true if it is valid
     * @throws QueryException if there is a problem communicating
     */
    boolean ping() throws QueryException;

    /**
     * executes a query
     *
     * @param dQuery the query to execute
     * @return a query result.
     * @throws QueryException if there is a problem with the query
     */
    QueryResult executeQuery(Query dQuery) throws QueryException;

    /**
     * adds a query to the batch
     *
     * @param dQuery the query to add
     */
    void addToBatch(Query dQuery);

    /**
     * executes the batch of queries
     *
     * @return a list of query results
     * @throws QueryException if there is a problem
     */
    public List<QueryResult> executeBatch() throws QueryException;

    /**
     * clears the current batch
     */
    void clearBatch();

    List<RawPacket> startBinlogDump(int startPos, String filename) throws BinlogDumpException;

    SupportedDatabases getDatabaseType();
}
