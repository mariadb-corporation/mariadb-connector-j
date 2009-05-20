package org.drizzle.jdbc.internal;

import org.drizzle.jdbc.internal.common.QueryException;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: May 20, 2009
 * Time: 5:06:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class SQLExceptionMapper {
    public enum SQLStates {
        INTEGRITY("23");
        private String sqlStateGroup;

        SQLStates(String sqlStateGroup) {
            this.sqlStateGroup=sqlStateGroup;
        }

        public static SQLStates fromString(String group) {
            if(group.startsWith("23"))
                return INTEGRITY;
            return INTEGRITY;
        }
    }
    public static SQLException get(QueryException e) {
        String sqlState = e.getSqlState();
        SQLStates state = SQLStates.fromString(sqlState);
        switch(state) {
            case INTEGRITY:
                return new SQLIntegrityConstraintViolationException(e.getMessage(),e);
        }
        return new SQLException(e.getMessage(),e);
    }
}
