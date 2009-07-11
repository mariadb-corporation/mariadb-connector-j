package org.drizzle.jdbc;

import org.drizzle.jdbc.internal.common.ParameterizedBatchHandler;
import org.drizzle.jdbc.internal.common.Protocol;
import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.query.ParameterizedQuery;
import org.junit.Ignore;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: May 31, 2009
 * Time: 7:54:49 PM
 * To change this template use File | Settings | File Templates.
 */

@Ignore
public class TestNoopBatchHandler implements ParameterizedBatchHandler {
    public void addToBatch(ParameterizedQuery query) {
    }

    public int[] executeBatch() throws QueryException {
        return null;
    }
}
