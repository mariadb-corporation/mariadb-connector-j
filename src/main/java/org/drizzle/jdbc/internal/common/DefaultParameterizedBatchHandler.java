package org.drizzle.jdbc.internal.common;

import org.drizzle.jdbc.internal.SQLExceptionMapper;
import org.drizzle.jdbc.internal.common.query.ParameterizedQuery;
import org.drizzle.jdbc.internal.common.queryresults.ModifyQueryResult;
import org.drizzle.jdbc.internal.common.queryresults.QueryResult;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: May 31, 2009 Time: 4:14:56 PM To change this template use File |
 * Settings | File Templates.
 */
public final class DefaultParameterizedBatchHandler implements ParameterizedBatchHandler {
    private final List<ParameterizedQuery> queries = new LinkedList<ParameterizedQuery>();
    private final Protocol protocol;

    public DefaultParameterizedBatchHandler(final Protocol protocol) {
        this.protocol = protocol;
    }

    public void addToBatch(final ParameterizedQuery query) {
        queries.add(query);
    }

    public int[] executeBatch() throws QueryException {
        final int[] retArray = new int[queries.size()];
        int i = 0;
        for (final ParameterizedQuery query : queries) {
            final QueryResult qr = protocol.executeQuery(query);
            if (qr instanceof ModifyQueryResult) {
                retArray[i++] = (int) ((ModifyQueryResult) qr).getUpdateCount();
            } else {
                throw new QueryException("One of the queries in the batch returned a result set",
                        (short) -1,
                        SQLExceptionMapper.SQLStates.UNDEFINED_SQLSTATE.getSqlState());
            }
        }
        queries.clear();
        return retArray;
    }
}
