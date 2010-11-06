package org.drizzle.jdbc;

import org.drizzle.jdbc.internal.common.RewriteParameterizedBatchHandlerFactory;
import org.drizzle.jdbc.internal.common.Utils;
import org.drizzle.jdbc.internal.common.packet.RawPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 7:58:11 AM
 */
@Ignore
public class BlobStreamingTest {
    public static String host = "10.100.100.50";
    private Connection connection;
    static { Logger.getLogger("").setLevel(Level.OFF); }

    public BlobStreamingTest() throws SQLException {
        //connection = DriverManager.getConnection("jdbc:mysql:thin://10.100.100.50:3306/test_units_jdbc");
       connection = DriverManager.getConnection("jdbc:drizzle://"+host+":3307/test_units_jdbc?enableBlobStreaming=true");
       //connection = DriverManager.getConnection("jdbc:mysql://10.100.100.50:3306/test_units_jdbc");
    }
  //  @After
    public void close() throws SQLException {
        connection.close();
    }
    public Connection getConnection() {
        return connection;
    }
    
   // @Test
    public void doQuery() throws SQLException, IOException {
        Statement stmt = getConnection().createStatement();
        stmt.execute("drop table  if exists bstreaming1");
        stmt.execute("create table bstreaming1 (id int not null primary key auto_increment, test longblob)");
        PreparedStatement ps = getConnection().prepareStatement("insert into bstreaming1 values (null, ?)");
        ByteArrayInputStream bais = new ByteArrayInputStream("HEJHEJHEJ".getBytes());
        ps.setBinaryStream(1, bais);
        ps.executeUpdate();
        
        stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("select * from bstreaming1");
        assertEquals(rs.next(), true);
        byte[] b = new byte[100];
        int l = rs.getBinaryStream("test").read(b);
        assertEquals("HEJHEJHEJ",new String(b,0,l));
    }

}