package org.drizzle.jdbc;

import java.sql.Savepoint;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 6, 2009
 * Time: 9:56:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class DrizzleSavepoint implements Savepoint {
    private final int savepointId;
    private final String name;

    public DrizzleSavepoint(String name, int savepointId) {
        this.savepointId=savepointId;
        this.name=name;
    }

    /**
     * Retrieves the generated ID for the savepoint that this
     * <code>Savepoint</code> object represents.
     *
     * @return the numeric ID of this savepoint
     * @throws java.sql.SQLException if this is a named savepoint
     * @since 1.4
     */
    public int getSavepointId() throws SQLException {
        return savepointId;
    }

    /**
     * Retrieves the name of the savepoint that this <code>Savepoint</code>
     * object represents.
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
        return name+savepointId;
    }
}
