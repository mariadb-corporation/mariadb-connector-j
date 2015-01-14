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
package org.mariadb.jdbc.internal.common.query;

import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.parameters.ParameterHolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.List;

import static org.mariadb.jdbc.internal.common.Utils.createQueryParts;


public class MySQLParameterizedQuery implements ParameterizedQuery {

    private ParameterHolder[] parameters;
    private int paramCount;
    private String query;
    private byte[][] queryPartsArray;

    public MySQLParameterizedQuery(String query, boolean noBackslashEscapes) {
        this.query = query;
        List<String> queryParts = createQueryParts(query, noBackslashEscapes);
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

    private  MySQLParameterizedQuery() {

    }
    public MySQLParameterizedQuery cloneQuery() {
        MySQLParameterizedQuery q = new  MySQLParameterizedQuery();
        q.parameters = new ParameterHolder[parameters.length];
        for (int i = 0; i < parameters.length;i++) {
            q.parameters[i] = parameters[i];
        }
        q.paramCount = paramCount;
        q.query = query;
        q.queryPartsArray = queryPartsArray;
        return q;
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


    public void writeTo(final OutputStream os) throws IOException, QueryException {

        if(queryPartsArray.length == 0) {
            throw new AssertionError("Invalid query, queryParts was empty");
        }
        os.write(queryPartsArray[0]);
        for(int i = 1; i<queryPartsArray.length; i++) {
            parameters[i-1].writeTo(os);
            if(queryPartsArray[i].length != 0)
                os.write(queryPartsArray[i]);
        }
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

    public String toString() {
        StringBuffer sb  = new StringBuffer ("sql : '" + query + "'");
        if (parameters.length > 0) {
            sb.append(", parameters : [");
            for(int i = 0; i < parameters.length; i++) {
              if (parameters[i] == null)  {
                sb.append("null");
              }  else {
                sb.append(parameters[i].toString());
              }
              if (i != parameters.length -1) {
                sb.append(",");
              }
            }
            sb.append("]");
        }
        return sb.toString();
    }
    
    /**
     * Returns a string representing the SQL of the query.
     * @return
     */
	public String toSQL() {
		try {
			final ByteArrayOutputStream os = new ByteArrayOutputStream();
			writeTo(os);
			String sql = new String(os.toByteArray(), Charset.forName("UTF8"));
			return sql;
		} catch (QueryException qe) {
			return "";
		} catch (IOException e) {
			return "";
		}
	}

	private String toSQL2() throws UnsupportedEncodingException {
        if(queryPartsArray.length == 0) {
            return "";
        }
        String result;
        result = new String(queryPartsArray[0], "UTF-8");
        for(int i = 1; i<queryPartsArray.length; i++) {
            result += parameters[i-1];
            if(queryPartsArray[i].length != 0)
                result += new String(queryPartsArray[i], "UTF-8");
        }
		return result;
    }
	
	@Override
	public int getPacketLength() {
		try {
			return toSQL2().getBytes("UTF-8").length;
		} catch (UnsupportedEncodingException e) {
			return -1;
		}
	}

}
