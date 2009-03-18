package org.drizzle.jdbc.internal.query.drizzle;

import org.drizzle.jdbc.internal.query.QueryFactory;
import org.drizzle.jdbc.internal.query.Query;
import org.drizzle.jdbc.internal.query.ParameterizedQuery;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 18, 2009
 * Time: 10:14:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class DrizzleQueryFactory implements QueryFactory {
    public Query createQuery(String query) {
        return new DrizzleQuery(query);
    }

    public ParameterizedQuery createParameterizedQuery(String query) {
        return new DrizzleParameterizedQuery(query);
    }

    public ParameterizedQuery createParameterizedQuery(ParameterizedQuery dQuery) {
        return new DrizzleParameterizedQuery(dQuery);
    }
}
