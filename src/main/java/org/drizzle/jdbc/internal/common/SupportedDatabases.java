package org.drizzle.jdbc.internal.common;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jun 13, 2009
 * Time: 12:49:55 PM
 * To change this template use File | Settings | File Templates.
 */
public enum SupportedDatabases {
    MYSQL("MySQL"), DRIZZLE("Drizzle");
    private final String databaseName;

    SupportedDatabases(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }
}
