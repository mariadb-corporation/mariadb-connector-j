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

package org.mariadb.jdbc.internal.common;

import java.util.EnumSet;
import java.util.Set;


public enum ServerStatus {
    IN_TRANSACTION((short) 1),
    AUTOCOMMIT((short) 2),
    MORE_RESULTS_EXISTS((short) 8),
    QUERY_NO_GOOD_INDEX_USED((short) 16),
    QUERY_NO_INDEX_USED((short) 32),
    CURSOR_EXISTS((short) 64),
    LAST_ROW_SENT((short) 128),
    DB_DROPPED((short) 256),
    NO_BACKSLASH_ESCAPES((short) 512),
    QUERY_WAS_SLOW((short) 1024);


    private final short bitmapFlag;

    ServerStatus(final short i) {
        this.bitmapFlag = i;
    }

    /**
     * returns the bit map flag
     *
     * @return the bitmap flag
     */
    public short getBitmapFlag() {
        return bitmapFlag;
    }

    /**
     * creates an enum set of the bitmasked field i
     *
     * @param i the bitmasked field
     * @return an enum set with the flags defined by i
     */
    public static Set<ServerStatus> getServerStatusSet(final short i) {
        final Set<ServerStatus> statusSet = EnumSet.noneOf(ServerStatus.class);
        for (final ServerStatus value : ServerStatus.values()) {
            if ((i & value.getBitmapFlag()) == value.getBitmapFlag()) {
                statusSet.add(value);
            }
        }
        return statusSet;
    }
}
