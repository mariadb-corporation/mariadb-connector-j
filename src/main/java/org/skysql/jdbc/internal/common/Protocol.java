/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.skysql.jdbc.internal.common;

import org.skysql.jdbc.internal.common.packet.RawPacket;
import org.skysql.jdbc.internal.common.query.Query;
import org.skysql.jdbc.internal.common.queryresults.QueryResult;

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
     * @param streaming - if true, the result is streaming (forward-only, non-cached)
     * if false - the result is cached and can be scrolled
     * @return a query result.
     * @throws QueryException if there is a problem with the query
     */
    QueryResult executeQuery(Query dQuery, boolean streaming) throws QueryException;

    /* Execute query that returns scrollable result set */
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

    boolean supportsPBMS();

    String getServerVariable(String s) throws QueryException;

    /**
     * should a database be created if it does not exist ?
     */
    boolean createDB();

    void cancelCurrentQuery() throws QueryException;
    void timeOut() throws QueryException;

    boolean hasMoreResults();
    QueryResult getMoreResults(boolean streaming) throws QueryException;
    boolean hasUnreadData();
}
