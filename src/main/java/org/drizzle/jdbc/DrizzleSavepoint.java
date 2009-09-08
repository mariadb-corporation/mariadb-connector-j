/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc;

import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * . User: marcuse Date: Feb 6, 2009 Time: 9:56:55 PM
 */
public class DrizzleSavepoint implements Savepoint {
    private final int savepointId;
    private final String name;

    public DrizzleSavepoint(final String name, final int savepointId) {
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
