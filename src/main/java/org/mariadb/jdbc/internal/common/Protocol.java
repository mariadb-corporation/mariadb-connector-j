/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab. All Rights Reserved.

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

All rights reserved.

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

package org.mariadb.jdbc.internal.common;

import org.mariadb.jdbc.internal.common.packet.RawPacket;
import org.mariadb.jdbc.internal.common.query.Query;
import org.mariadb.jdbc.internal.common.queryresults.QueryResult;

import java.io.IOException;
import java.util.List;


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


    String getServerVariable(String s) throws QueryException;

    /**
     * should a database be created if it does not exist ?
     */
    boolean createDB();

    void cancelCurrentQuery() throws QueryException,IOException;


    boolean hasMoreResults();
    QueryResult getMoreResults(boolean streaming) throws QueryException;
    boolean hasUnreadData();
    void setMaxRows(int max) throws QueryException;
}
