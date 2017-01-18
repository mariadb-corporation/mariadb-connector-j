package org.mariadb.jdbc;

import com.fasterxml.jackson.databind.deser.Deserializers;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static org.junit.Assert.*;



public class MariaDbDatabaseMetaDataTest extends BaseTest {


    @Test
    public void testColumnTypeClauseTinyInt1IsBitTrue() throws Exception {
        MariaDbConnection connection = (MariaDbConnection)setConnection();
        MariaDbDatabaseMetaData dbMetaData = new MariaDbDatabaseMetaData(connection, "", "");
        String result = dbMetaData.columnTypeClause("column_name");
        assertEquals(" UCASE(IF( IF(column_name='tinyint(1)','BIT',column_name)  LIKE '%(%)%', CONCAT(SUBSTRING( "
                + "IF(column_name='tinyint(1)','BIT',column_name) ,1, LOCATE('(',IF(column_name='tinyint(1)','BIT',column_name) ) - 1 ), "
                + "SUBSTRING(IF(column_name='tinyint(1)','BIT',column_name) ,1+locate(')',IF(column_name='tinyint(1)','BIT',column_name) ))), "
                + "IF(column_name='tinyint(1)','BIT',column_name) ))", result);
    }

    @Test
    public void testColumnTypeClauseTinyInt1IsBitFalse() throws Exception {
        Properties props = new Properties();
        props.setProperty("tinyInt1isBit", "false");
        MariaDbConnection connection = (MariaDbConnection)openNewConnection(connUri, props);
        MariaDbDatabaseMetaData dbMetaData = new MariaDbDatabaseMetaData(connection, "", "");
        String result = dbMetaData.columnTypeClause("column_name");
        assertEquals(" UCASE(IF( column_name LIKE '%(%)%', CONCAT(SUBSTRING( column_name,1, LOCATE('(',column_name) - 1 ), "
                + "SUBSTRING(column_name,1+locate(')',column_name))), column_name))", result);
    }

}