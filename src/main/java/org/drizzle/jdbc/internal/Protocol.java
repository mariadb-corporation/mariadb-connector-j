package org.drizzle.jdbc.internal;

import java.io.IOException;
import java.sql.SQLException;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 4:05:47 PM
 */
public interface Protocol {

    void close() throws QueryException;

    boolean isClosed();
/* TODO: use these methods for queries instead
   if you don't know what kind of query is being passed, classify it using
   queryIsSelect(..) first, then execute the correct method*/
    /*
    boolean queryIsSelect(String query);
    DrizzleSelectResult executeQuery(String query);
    DrizzleModifyResult executeUpdate(String query);      */

    QueryResult executeQuery(String s) throws QueryException;



    void selectDB(String database) throws QueryException;

    String getVersion();

    void setReadonly(boolean readOnly);

    boolean getReadonly();

    void commit() throws QueryException;

    void rollback() throws QueryException;
    void rollback(String savepoint) throws QueryException;
    void setSavepoint(String savepoint) throws QueryException;
    void releaseSavepoint(String savepoint) throws QueryException;

    void setAutoCommit(boolean autoCommit) throws QueryException;

    boolean getAutoCommit();

    public String getHost();

    public int getPort();

    public String getDatabase();

    public String getUsername();
    public String getPassword();
}
