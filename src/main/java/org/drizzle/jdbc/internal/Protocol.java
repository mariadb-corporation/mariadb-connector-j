package org.drizzle.jdbc.internal;

import java.io.IOException;
import java.sql.SQLException;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 4:05:47 PM
 */
public interface Protocol {

    void close() throws IOException;

    boolean isClosed();
/* TODO: use these methods for queries instead
   if you don't know what kind of query is being passed, classify it using
   queryIsSelect(..) first, then execute the correct method*/
    /*
    boolean queryIsSelect(String query);
    DrizzleSelectResult executeQuery(String query);
    DrizzleModifyResult executeUpdate(String query);      */

    QueryResult executeQuery(String s) throws IOException, SQLException;



    void selectDB(String database) throws IOException;

    String getVersion();

    void setReadonly(boolean readOnly);

    boolean getReadonly();

    void commit() throws IOException, SQLException;

    void rollback() throws IOException, SQLException;

    void setAutoCommit(boolean autoCommit);

    boolean getAutoCommit();


}
