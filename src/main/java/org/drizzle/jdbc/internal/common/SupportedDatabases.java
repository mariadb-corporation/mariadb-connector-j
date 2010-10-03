package org.drizzle.jdbc.internal.common;

import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Jun 13, 2009 Time: 12:49:55 PM To change this template use File |
 * Settings | File Templates.
 */
public enum SupportedDatabases {
    MYSQL("MySQL"), DRIZZLE("Drizzle");
    private final String databaseName;
    private static final Pattern drizzlePattern = Pattern.compile("^201\\d\\..*"); //will work for 9 years atleast!

    SupportedDatabases(final String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public static SupportedDatabases fromVersionString(String version) {
        if(drizzlePattern.matcher(version).matches())
            return SupportedDatabases.DRIZZLE;
        return SupportedDatabases.MYSQL;
    }
}
