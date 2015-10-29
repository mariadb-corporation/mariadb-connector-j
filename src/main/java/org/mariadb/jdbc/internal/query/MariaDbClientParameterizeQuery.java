package org.mariadb.jdbc.internal.query;
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

import org.mariadb.jdbc.internal.util.ExceptionMapper;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.List;

import static org.mariadb.jdbc.internal.util.Utils.createQueryParts;

/**
 * Client Parameterize query implementation.
 *
 * Example part :
 * INSERT INTO MultiTestt3_dupp(col1, pkey,col2,col3,col4) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY UPDATE pkey=pkey+10
 * <ol>
 * <li>queryParts array : [
 * "INSERT INTO MultiTestt3_dupp(col1, pkey,col2,col3,col4) VALUES (9, ",
 * ", 5, ",
 * ", 8) ON DUPLICATE KEY UPDATE pkey=pkey+10"]</li>
 * <li>rewriteFirstPart : "9, "</li>
 * <li>rewriteRepeatLastPart : ", 8"</li>
 * <li>rewriteNotRepeatLastPart : " ON DUPLICATE KEY UPDATE pkey=pkey+10"</li>
 * <li></li>
 * </ol>
 * <p>if "allowMultiQueries" option active :
 * Query will be rewritten as :
 * queryParts[0]+parameter[0]+...queryParts[x]+parameter[x]+queryParts[x+1] + ";" + new query...
 * "INSERT INTO MultiTestt3_dupp(col1, pkey,col2,col3,col4) VALUES (9, "+parameter0+", 5, "+parameter1+", 8) ON DUPLICATE KEY UPDATE pkey=pkey+10"</p>
 * <p>if "rewriteBatchedStatements" option active :
 * Query will be rewritten as :
 * - queryParts[0]+parameter[0]+...queryParts[x]+parameter[x] + ... "("+rewriteFirstPart+parameter[0]+...queryParts[x]
 * +parameter[x]-rewriteNotRepeatLastPart+")"+rewriteNotRepeatLastPart
 * for the example
 * "INSERT INTO MultiTestt3_dupp(col1, pkey,col2,col3,col4) VALUES (9, "+parameter0+", 5, "+parameter1+", 8)"+"("+"9, "+
 * secondParameter0+", 5, "+secondParameter1"+", 8"+")"+" ON DUPLICATE KEY UPDATE pkey=pkey+10"
 */
public class MariaDbClientParameterizeQuery implements ParameterizeQuery {

    private ParameterHolder[] parameters;
    private int paramCount;
    private byte[][] queryPartsArray;

    private byte[] rewriteFirstPart = null;
    private byte[] rewriteRepeatLastPart = null;
    private byte[] rewriteNotRepeatLastPart = null;

    /**
     * Return estimated query length.
     * @return length
     * @throws IOException if parameter approximated length launched exception (stream reading)
     */
    public int getQuerySize() throws IOException {
        long size = queryPartsArray[0].length;
        for (int i = 1; i < queryPartsArray.length; i++) {
            size += parameters[i - 1].getApproximateTextProtocolLength();
            if (queryPartsArray[i].length != 0) {
                size += queryPartsArray[i].length;
            }
        }
        return (int) size;
    }

    /**
     * Return first identical length of all queries.
     * @return length
     */
    public byte[] getRewriteFirstPart() {
        return rewriteFirstPart;
    }

    /**
     * Constructor.
     * @param query query string
     * @param noBackslashEscapes must backSlash be escaped
     * @param rewriteOffset first common part index
     */
    public MariaDbClientParameterizeQuery(String query, boolean noBackslashEscapes, int rewriteOffset) {
        try {
            List<String> queryParts = createQueryParts(query, noBackslashEscapes);
            if (rewriteOffset != -1) {
                rewriteFirstPart = queryParts.get(0).substring(rewriteOffset + 1).getBytes("UTF-8");
                String lastPart = queryParts.get(queryParts.size() - 1);
                if (lastPart.indexOf(")") != -1) {
                    rewriteRepeatLastPart = lastPart.substring(0, lastPart.indexOf(")")).getBytes("UTF-8");
                    rewriteNotRepeatLastPart = lastPart.substring(lastPart.indexOf(")") + 1).getBytes("UTF-8");
                } else {
                    rewriteRepeatLastPart = lastPart.getBytes("UTF-8");
                    rewriteNotRepeatLastPart = new byte[0];
                }
            }
            queryPartsArray = new byte[queryParts.size()][];
            for (int i = 0; i < queryParts.size(); i++) {
                queryPartsArray[i] = queryParts.get(i).getBytes("UTF-8");
            }
            paramCount = queryParts.size() - 1;
            parameters = new ParameterHolder[paramCount];
        } catch (UnsupportedEncodingException u) {
        }
    }

    private MariaDbClientParameterizeQuery() {

    }

    /**
     * Clone query to avoid recreating parts.
     * @return a clone version.
     */
    public MariaDbClientParameterizeQuery cloneQuery() {
        MariaDbClientParameterizeQuery clientQuery = new MariaDbClientParameterizeQuery();
        clientQuery.parameters = new ParameterHolder[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            clientQuery.parameters[i] = parameters[i];
        }
        clientQuery.paramCount = paramCount;
        clientQuery.queryPartsArray = queryPartsArray;
        clientQuery.rewriteFirstPart = rewriteFirstPart;
        clientQuery.rewriteRepeatLastPart = rewriteRepeatLastPart;
        clientQuery.rewriteNotRepeatLastPart = rewriteNotRepeatLastPart;
        return clientQuery;
    }

    /**
     * Set a parameter to query.
     * @param position  the position to set it at
     * @param parameter the parameter to set
     * @throws SQLException if position is incorrect
     */
    public void setParameter(final int position, final ParameterHolder parameter) throws SQLException {
        if (position >= 0 && position < paramCount) {
            parameters[position] = parameter;
        } else {
            throw ExceptionMapper.getSqlException("Could not set parameter");
        }
    }

    public ParameterHolder[] getParameters() {
        return parameters;
    }

    public void clearParameters() {
        this.parameters = new ParameterHolder[paramCount];
    }

    /**
     * Validate that all parameters are set.
     * @throws QueryException if any parameter is missing
     */
    public void validate() throws QueryException {
        if (containsNull(parameters)) {
            throw new QueryException("You need to set exactly " + paramCount + " parameters on the prepared statement");
        }
    }

    /**
     * Write whole query to buffer.
     * @param os outputStream
     * @throws IOException if any error occur during buffer writing
     */
    public void writeTo(final OutputStream os) throws IOException {
        if (queryPartsArray.length == 0) {
            throw new AssertionError("Invalid query, queryParts was empty");
        }
        os.write(queryPartsArray[0]);
        for (int i = 1; i < queryPartsArray.length; i++) {
            parameters[i - 1].writeTo(os);
            if (queryPartsArray[i].length != 0) {
                os.write(queryPartsArray[i]);
            }
        }
    }


    /**
     * Write first common part into buffer.
     * @param os outputStream
     * @throws IOException if any error occur during buffer writing
     */
    public void writeFirstRewritePart(final OutputStream os) throws IOException {
        if (queryPartsArray.length == 0) {
            throw new AssertionError("Invalid query, queryParts was empty");
        }

        for (int i = 0; i < queryPartsArray.length - 1; i++) {
            os.write(queryPartsArray[i]);
            parameters[i].writeTo(os);
        }
        if (rewriteRepeatLastPart != null) {
            os.write(rewriteRepeatLastPart);
        }
        os.write(41); // ")" in UTF-8
    }

    /**
     * Write last common part into buffer.
     * @param os outputStream
     * @throws IOException if any error occur during buffer writing
     */
    public void writeLastRewritePart(final OutputStream os) throws IOException {
        if (rewriteNotRepeatLastPart != null) {
            os.write(rewriteNotRepeatLastPart);
        }
    }

    /**
     * Will return identical last part length.
     * @return length
     */
    public int writeLastRewritePartLength() {
        if (rewriteNotRepeatLastPart != null) {
            return rewriteNotRepeatLastPart.length;
        }
        return 0;
    }


    /**
     * Write rewritable specific part of the query.
     * @param os outputStream
     * @param rewriteOffset for compatibility (not used)
     * @throws IOException if any error occur during writing into buffer.
     */
    public void writeToRewritablePart(final OutputStream os, int rewriteOffset) throws IOException {
        if (queryPartsArray.length == 0) {
            throw new AssertionError("Invalid query, queryParts was empty");
        }

        os.write(new byte[]{44, 40}); //",(" in UTF-8
        os.write(rewriteFirstPart);
        for (int i = 0; i < parameters.length; i++) {
            parameters[i].writeTo(os);
            if (i < parameters.length - 1) {
                os.write(queryPartsArray[i + 1]);
            } else {
                os.write(rewriteRepeatLastPart);
            }
        }
        os.write(41); // ")" in UTF-8
    }

    /**
     * Will return rewrite part length.
     * @param rewriteOffset the index position of content change between queries.
     * @return length
     * @throws IOException if parameter approximate length return exception
     */
    public int writeToRewritablePartLength(int rewriteOffset) throws IOException {
        long length = 3 + rewriteFirstPart.length;
        for (int i = 0; i < parameters.length; i++) {
            length += parameters[i].getApproximateTextProtocolLength();
            if (i < parameters.length - 1) {
                length += queryPartsArray[i + 1].length;
            } else {
                length += rewriteRepeatLastPart.length;
            }
        }
        return (int) length;
    }

    private boolean containsNull(ParameterHolder[] parameters) {
        for (ParameterHolder ph : parameters) {
            if (ph == null) {
                return true;
            }
        }
        return false;
    }

    public byte[][] getQueryPartsArray() {
        return queryPartsArray;
    }

    public int getParamCount() {
        return paramCount;
    }

    /**
     * toString implementation. Display current sql string.
     * @return current sql String.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();

        sb.append(new String(queryPartsArray[0]));
        for (int i = 1; i < queryPartsArray.length; i++) {
            sb.append("?");
            if (queryPartsArray[i].length != 0) {
                sb.append(new String(queryPartsArray[i]));
            }
        }

        if (parameters.length > 0) {
            sb.append(", parameters : [");
            for (int i = 0; i < parameters.length; i++) {
                if (parameters[i] == null) {
                    sb.append("null");
                } else {
                    sb.append(parameters[i].toString());
                }
                if (i != parameters.length - 1) {
                    sb.append(",");
                }
            }
            sb.append("]");
        }
        return sb.toString();
    }


}