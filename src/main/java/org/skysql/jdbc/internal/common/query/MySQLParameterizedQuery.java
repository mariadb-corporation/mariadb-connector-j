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

package org.skysql.jdbc.internal.common.query;

import org.skysql.jdbc.internal.common.QueryException;
import org.skysql.jdbc.internal.common.query.parameters.ParameterHolder;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import static org.skysql.jdbc.internal.common.Utils.createQueryParts;

/**
 * . User: marcuse Date: Feb 18, 2009 Time: 10:13:42 PM
 */
public class MySQLParameterizedQuery implements ParameterizedQuery {

    private ParameterHolder[] parameters;
    private final int paramCount;
    private final String query;
    private final byte[][] queryPartsArray;

    public MySQLParameterizedQuery(final String query) {
        this.query = query;
        List<String> queryParts = createQueryParts(query);
        queryPartsArray = new byte[queryParts.size()][];
        for(int i=0;i < queryParts.size(); i++) {
            try {
                queryPartsArray[i] = queryParts.get(i).getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("UTF-8 not supported", e);
            }
        }
        paramCount = queryParts.size() - 1;
        parameters = new ParameterHolder[paramCount];
    }

    public MySQLParameterizedQuery(final ParameterizedQuery paramQuery) {
        this.query = paramQuery.getQuery();
        this.queryPartsArray = paramQuery.getQueryPartsArray();
        paramCount = queryPartsArray.length - 1;
        parameters = new ParameterHolder[paramCount];
    }

    public void setParameter(final int position, final ParameterHolder parameter) throws IllegalParameterException {
        if (position >= 0 && position < paramCount) {
            parameters[position] = parameter;
        } else {
            throw new IllegalParameterException("No '?' on that position");
        }
    }

    public ParameterHolder[] getParameters() {
        return parameters;
    }

    public void clearParameters() {
        this.parameters = new ParameterHolder[paramCount];
    }

    public void validate() throws QueryException{
        if(containsNull(parameters)) {
            throw new QueryException("You need to set exactly " + paramCount + " parameters on the prepared statement");
        }
    }

    public int length() throws QueryException {
        if(containsNull(parameters)) {
            throw new QueryException("You need to set exactly " + paramCount + " parameters on the prepared statement");
        }        
        int length = 0;
        for(byte[] s : queryPartsArray) {
            length += s.length;
        }

        for(ParameterHolder ph : parameters) {
            try {
                length += ph.length();
            } catch (IOException e) {
                throw new QueryException("Could not calculate length of parameter: "+e.getMessage());
            }
        }
        return length;
    }

    public void writeTo(final OutputStream os) throws IOException, QueryException {

        if(queryPartsArray.length == 0) {
            throw new AssertionError("Invalid query, queryParts was empty");
        }
        os.write(queryPartsArray[0]);
        for(int i = 1; i<queryPartsArray.length; i++) {
            parameters[i-1].writeTo(os, 0, Integer.MAX_VALUE);
            if(queryPartsArray[i].length != 0)
                os.write(queryPartsArray[i]);
        }
    }


    public void writeTo(OutputStream ostream, int offset, int packLength)
            throws IOException, QueryException {

        /*if(containsNull(parameters)) {
            throw new QueryException("You need to set exactly " + paramCount + " parameters on the prepared statement");
        }
        */
        if(queryPartsArray.length == 0) {
            throw new AssertionError("Invalid query, queryParts was empty");
        }
        int skipped = 0;
        int sendCounter = 0;
        if(queryPartsArray[0].length > offset) {
            ostream.write(queryPartsArray[0], offset, queryPartsArray[0].length);
            sendCounter = queryPartsArray[0].length;
        } else {
            skipped = queryPartsArray[0].length;
        }

        for(int i = 1; i<queryPartsArray.length; i++) {
            ParameterHolder ph = parameters[i-1];

            if(skipped < offset && skipped + ph.length() > offset) {
                // offset is in the middle of this param
                int written = ph.writeTo(ostream, offset - skipped, packLength);
                skipped += offset - skipped; 
                sendCounter += written;
            } else if(ph.length()+skipped + sendCounter > offset) {
                sendCounter += ph.writeTo(ostream, 0, packLength - sendCounter);
            } else {
                skipped += ph.length();
            }
            if(sendCounter + skipped >=offset + packLength) return;
            if(queryPartsArray[i].length + sendCounter + skipped > offset) {
                ostream.write(queryPartsArray[i]);
                sendCounter += queryPartsArray[i].length;
            } else {
                skipped += queryPartsArray[i].length;
            }
            if(sendCounter >= packLength) return;
        }
        //System.out.println(offset+currentPos);


    }
    private boolean containsNull(ParameterHolder[] parameters) {
        for(ParameterHolder ph : parameters) {
            if(ph == null) {
                return true;
            }
        }
        return false;
    }

    public String getQuery() {
        return query;
    }

    public byte[][] getQueryPartsArray() {
        return queryPartsArray;
    }

    public QueryType getQueryType() {
        return QueryType.classifyQuery(query);
    }

    public int getParamCount() {
        return paramCount;
    }



}
