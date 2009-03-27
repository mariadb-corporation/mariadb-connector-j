package org.drizzle.jdbc.internal.query;

/**
 .
 * User: marcuse
 * Date: Mar 18, 2009
 * Time: 10:06:11 PM

 */
public interface QueryFactory {
    Query createQuery(String query);
    ParameterizedQuery createParameterizedQuery(String query);
    ParameterizedQuery createParameterizedQuery(ParameterizedQuery dQuery);
}
