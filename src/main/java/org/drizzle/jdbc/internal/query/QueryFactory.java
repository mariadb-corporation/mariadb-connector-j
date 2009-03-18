package org.drizzle.jdbc.internal.query;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 18, 2009
 * Time: 10:06:11 PM
 * To change this template use File | Settings | File Templates.
 */
public interface QueryFactory {
    Query createQuery(String query);
    ParameterizedQuery createParameterizedQuery(String query);

    ParameterizedQuery createParameterizedQuery(ParameterizedQuery dQuery);
}
