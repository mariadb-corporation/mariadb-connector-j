package org.drizzle.jdbc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

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

    DrizzleQueryResult executeQuery(String s) throws IOException, SQLException;



    void selectDB(String database) throws IOException;

    public void clearInputStream() throws IOException;

    String getVersion();

    void setReadonly(boolean readOnly);

    boolean getReadonly();
}
