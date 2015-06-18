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


Copyright (c) 2009-2011, Marcus Eriksson, Jay Pipes
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
import  org.mariadb.jdbc.internal.common.QueryException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;



public class MySQLQuery implements Query {

    private final String query;

    public MySQLQuery(final String query) {
        this.query = query;
    }

    public void writeTo(final OutputStream os) throws IOException {
        try {
            byte[] queryToSend =  query.getBytes("UTF-8");
            os.write(queryToSend, 0, queryToSend.length);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unsupported encoding: " + e.getMessage(), e);
        }
    }

    public void writeToRewritablePart(final OutputStream os, int rewriteOffset) throws IOException, QueryException {
        try {
            byte[] queryToSend = query.substring(rewriteOffset).getBytes("UTF-8");
            os.write(',');
            os.write(queryToSend, 0, queryToSend.length);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unsupported encoding: " + e.getMessage(), e);
        }
    }

    public String getQuery() {
        return query;
    }

    public QueryType getQueryType() {
        return QueryType.classifyQuery(query);
    }

    @Override
    public boolean equals(final Object otherObj) {
        return otherObj instanceof MySQLQuery && (((MySQLQuery) otherObj).query).equals(query);
    }

    public void writeTo(OutputStream ostream, int offset, int packLength) throws IOException {
        try {
            byte[] queryToSend =  query.getBytes("UTF-8");
            ostream.write(queryToSend, offset, packLength);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unsupported encoding: " + e.getMessage(), e);
        }


    }

    public void validate() throws QueryException {

    }

    public String toString() {
        return query;
    }


}
