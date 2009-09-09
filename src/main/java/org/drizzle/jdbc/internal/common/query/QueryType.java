package org.drizzle.jdbc.internal.common.query;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Aug 13, 2009 Time: 8:58:08 PM To change this template use File |
 * Settings | File Templates.
 */
public enum QueryType {
    REPLACE, INSERT, SELECT, UPDATE, DELETE, ALTER, UNCLASSIFIABLE;

    public static QueryType classifyQuery(final String query) {
        final String lowerCaseQuery = query.toLowerCase();
        if (lowerCaseQuery.startsWith("select")) {
            return QueryType.SELECT;
        } else if (lowerCaseQuery.startsWith("update")) {
            return QueryType.UPDATE;
        } else if (lowerCaseQuery.startsWith("insert")) {
            return QueryType.INSERT;
        } else if (lowerCaseQuery.startsWith("alter")) {
            return QueryType.ALTER;
        } else if (lowerCaseQuery.startsWith("delete")) {
            return QueryType.DELETE;
        } else if (lowerCaseQuery.startsWith("replace")) {
            return QueryType.REPLACE;
        } else {
            return QueryType.UNCLASSIFIABLE;
        }
    }
}
