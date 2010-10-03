/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */
                         
package org.drizzle.jdbc.internal.common.query;

import java.util.concurrent.ConcurrentHashMap;

/**
 * . User: marcuse Date: Mar 18, 2009 Time: 10:14:27 PM
 */
public class DrizzleQueryFactory implements QueryFactory {
    private static final ConcurrentHashMap<String, ParameterizedQuery> PREPARED_CACHE = new ConcurrentHashMap<String, ParameterizedQuery>();
    public Query createQuery(final String query) {
        return new DrizzleQuery(query);
    }
    
    public DrizzleQuery createQuery(byte[] query)
    {
        return new DrizzleQuery(query);
    }
    public ParameterizedQuery createParameterizedQuery(final String query) {
        ParameterizedQuery pq = DrizzleQueryFactory.PREPARED_CACHE.get(query);
        

        if(pq == null) {
            pq = new DrizzleParameterizedQuery(query);
            DrizzleQueryFactory.PREPARED_CACHE.put(query, pq);
            return pq;
        } else {
            return new DrizzleParameterizedQuery(pq);
        }
    }

    public ParameterizedQuery createParameterizedQuery(final ParameterizedQuery dQuery) {
        return new DrizzleParameterizedQuery(dQuery);
    }
}
