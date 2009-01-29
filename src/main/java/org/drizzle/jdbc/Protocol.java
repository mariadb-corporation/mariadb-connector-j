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

    DrizzleQueryResult executeQuery(String s) throws IOException, SQLException;

    void selectDB(String database) throws IOException;

    public void clearInputStream() throws IOException;
}
