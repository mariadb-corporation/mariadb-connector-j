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

package org.mariadb.jdbc.internal.io;

import org.mariadb.jdbc.internal.util.Utils;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class LruTraceCache extends LinkedHashMap<Instant, TraceObject> {

    public LruTraceCache() {
        super(16, 1.0f, false);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<Instant, TraceObject> eldest) {
        return size() > 10;
    }

    /**
     * Value of trace cache in a readable format.
     *
     * @return trace cache value
     */
    public String printStack() {
        SimpleDateFormat dateFormat = new SimpleDateFormat();
        StringBuilder sb = new StringBuilder();
        Set<Map.Entry<Instant, TraceObject>> set = entrySet();
        for (Map.Entry<Instant, TraceObject> entry : set) {
            TraceObject traceObj = entry.getValue();
            Instant instant = entry.getKey();
            String indicator = "";

            switch (traceObj.getIndicatorFlag()) {
                case TraceObject.NOT_COMPRESSED:
                    break;

                case TraceObject.COMPRESSED_PROTOCOL_NOT_COMPRESSED_PACKET:
                    indicator = " (compressed protocol - packet not compressed)";
                    break;

                case TraceObject.COMPRESSED_PROTOCOL_COMPRESSED_PACKET:
                    indicator = " (compressed protocol - packet compressed)";
                    break;

                default:
                    break;
            }

            if (traceObj.isSend()) {
                sb.append("\nsend at ");
            } else {
                sb.append("\nread at ");
            }
            DateTimeFormatter.ISO_INSTANT.formatTo(instant, sb);
            sb.append(indicator)
                    .append(Utils.hexdump(traceObj.getBuf()));

            traceObj.remove();
        }
        this.clear();
        return sb.toString();
    }

    /**
     * Permit to clear array's of array, to help garbage.
     */
    public void clearMemory() {
        Set<Map.Entry<Instant, TraceObject>> set = entrySet();
        for (Map.Entry<Instant, TraceObject> entry : set) {
            entry.getValue().remove();
        }
        this.clear();
    }
}
