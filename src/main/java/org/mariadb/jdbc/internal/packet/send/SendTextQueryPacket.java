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

Copyright (c) 2009-2011, Marcus Eriksson, Stephane Giron

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

package org.mariadb.jdbc.internal.packet.send;

import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;
import org.mariadb.jdbc.internal.query.Query;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;


public class SendTextQueryPacket implements InterfaceSendPacket {

    private Query query;
    private List<Query> queries;
    private boolean isRewritable;
    private int rewriteOffset;

    /**
     * Initialize data.
     * @param queries list of queries to send to server
     * @param isRewritable are queries rewritable.
     * @param rewriteOffset initial common offset
     */
    public SendTextQueryPacket(final List<Query> queries, boolean isRewritable, int rewriteOffset) {
        this.queries = queries;
        this.query = null;
        this.isRewritable = isRewritable;
        this.rewriteOffset = rewriteOffset;
    }

    /**
     * Initialize datas.
     * @param query query to send to server.
     */
    public SendTextQueryPacket(final Query query) {
        this.query = query;
        this.queries = null;
        this.isRewritable = false;
        this.rewriteOffset = 0;
    }

    /**
     * Send queries to server.
     * @param stream write socket to server
     * @return number of send queries
     * @throws IOException if connection error occur
     * @throws QueryException when query rewrite error.
     */
    public int send(final OutputStream stream) throws IOException, QueryException {
        PacketOutputStream pos = (PacketOutputStream) stream;
        pos.startPacket(0);
        pos.write(0x03);
        int queryNumberSend = 1;
        if (query != null) {
            query.writeTo(stream);
        } else {
            if (queries.size() == 1) {
                queries.get(0).writeTo(stream);
            } else {
                if (!isRewritable) {
                    queries.get(0).writeTo(stream);
                    for (int i = 1; i < queries.size(); i++) {
                        if (pos.checkRewritableLength(queries.get(i).getQuerySize())) {
                            pos.write(';');
                            queries.get(i).writeTo(stream);
                            queryNumberSend++;
                        }
                    }
                } else {
                    queries.get(0).writeFirstRewritePart(stream);
                    int lastPartLength = queries.get(0).writeLastRewritePartLength();
                    for (int i = 1; i < queries.size(); i++) {
                        if (pos.checkRewritableLength(queries.get(i).writeToRewritablePartLength(rewriteOffset) + lastPartLength)) {
                            queries.get(i).writeToRewritablePart(stream, rewriteOffset);
                            queryNumberSend++;
                        }
                    }
                    queries.get(0).writeLastRewritePart(stream);
                }
            }
        }

        pos.finishPacket();
        return queryNumberSend;
    }

}