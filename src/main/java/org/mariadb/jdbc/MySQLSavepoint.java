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

package org.mariadb.jdbc;

import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * . User: marcuse Date: Feb 6, 2009 Time: 9:56:55 PM
 */
public class MySQLSavepoint implements Savepoint {
    private final int savepointId;
    private final String name;

    public MySQLSavepoint(final String name, final int savepointId) {
        this.savepointId = savepointId;
        this.name = name;
    }

    /**
     * Retrieves the generated ID for the savepoint that this <code>Savepoint</code> object represents.
     *
     * @return the numeric ID of this savepoint
     * @throws java.sql.SQLException if this is a named savepoint
     * @since 1.4
     */
    public int getSavepointId() throws SQLException {
        return savepointId;
    }

    /**
     * Retrieves the name of the savepoint that this <code>Savepoint</code> object represents.
     *
     * @return the name of this savepoint
     * @throws java.sql.SQLException if this is an un-named savepoint
     * @since 1.4
     */
    public String getSavepointName() throws SQLException {
        return name;
    }

    @Override
    public String toString() {
        return name + savepointId;
    }
}
