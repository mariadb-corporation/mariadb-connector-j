/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.util;

import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;


public final class ServerPrepareStatementCache extends LinkedHashMap<String, ServerPrepareResult> {
    private final int maxSize;
    private final Protocol protocol;

    private ServerPrepareStatementCache(int size, Protocol protocol) {
        super(size, .75f, true);
        this.maxSize = size;
        this.protocol = protocol;
    }

    public static ServerPrepareStatementCache newInstance(int size, Protocol protocol) {
        return new ServerPrepareStatementCache(size, protocol);
    }

    /**
     * Remove eldestEntry.
     *
     * @param eldest eldest entry
     * @return true if eldest entry must be removed
     */
    @Override
    public boolean removeEldestEntry(Map.Entry eldest) {
        boolean mustBeRemoved = this.size() > maxSize;

        if (mustBeRemoved) {
            ServerPrepareResult serverPrepareResult = ((ServerPrepareResult) eldest.getValue());
            serverPrepareResult.setRemoveFromCache();
            if (serverPrepareResult.canBeDeallocate()) {
                try {
                    protocol.forceReleasePrepareStatement(serverPrepareResult.getStatementId());
                } catch (SQLException e) {
                    //eat exception
                }
            }
        }
        return mustBeRemoved;
    }

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key,
     * the existing cached prepared result shared counter will be incremented.
     *
     * @param key    key
     * @param result new prepare result.
     * @return the previous value associated with key if not been deallocate, or null if there was no mapping for key.
     */
    public synchronized ServerPrepareResult put(String key, ServerPrepareResult result) {
        ServerPrepareResult cachedServerPrepareResult = super.get(key);
        //if there is already some cached data (and not been deallocate), return existing cached data
        if (cachedServerPrepareResult != null && cachedServerPrepareResult.incrementShareCounter()) {
            return cachedServerPrepareResult;
        }
        //if no cache data, or been deallocate, put new result in cache
        result.setAddToCache();
        super.put(key, result);
        return null;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("ServerPrepareStatementCache.map[");
        for (Map.Entry<String, ServerPrepareResult> entry : this.entrySet()) {
            stringBuilder.append("\n").append(entry.getKey()).append("-").append(entry.getValue().getShareCounter());
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }
}
