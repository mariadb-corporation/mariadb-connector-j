package org.mariadb.jdbc;

import org.junit.Test;

import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;

import static org.junit.Assert.*;

public class DataNTypeTest extends BaseTest {

    @Test
    public void testSetNClob() throws Exception {

        createTable("testSetNClob", "id int not null primary key, strm text", "CHARSET utf8");
        PreparedStatement stmt = sharedConnection.prepareStatement("insert into testSetNClob (id, strm) values (?,?)");
        NClob nclob = sharedConnection.createNClob();
        OutputStream stream = nclob.setAsciiStream(1);
        byte[] bytes = "hello".getBytes();
        stream.write(bytes);

        stmt.setInt(1, 1);
        stmt.setNClob(2, nclob);
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from testSetNClob");
        assertTrue(rs.next());
        assertTrue(rs.getObject(2) instanceof String);
        assertTrue(rs.getString(2).equals("hello"));
        NClob resultNClob = rs.getNClob(2);
        assertNotNull(resultNClob);
        assertEquals(5, resultNClob.getAsciiStream().available());

    }

    @Test
    public void testSetObjectNClob() throws Exception {

        createTable("testSetObjectNClob", "id int not null primary key, strm text, strm2 text", "CHARSET utf8");
        PreparedStatement stmt = sharedConnection.prepareStatement("insert into testSetObjectNClob (id, strm, strm2) values (?,?,?)");
        NClob nclob = sharedConnection.createNClob();
        OutputStream stream = nclob.setAsciiStream(1);
        byte[] bytes = "hello".getBytes();
        stream.write(bytes);

        stmt.setInt(1, 2);
        stmt.setObject(2, nclob);
        stmt.setObject(3, nclob, Types.NCLOB);
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from testSetObjectNClob");
        assertTrue(rs.next());
        assertTrue(rs.getObject(2) instanceof String);
        assertTrue(rs.getString(2).equals("hello"));
        assertEquals(5, rs.getNClob(2).getAsciiStream().available());
        assertTrue(rs.getObject(3) instanceof String);
        assertTrue(rs.getString(3).equals("hello"));
        assertEquals(5, rs.getNClob(3).getAsciiStream().available());
    }


    @Test
    public void testSetNString() throws Exception {

        createTable("testSetNString", "id int not null primary key, strm varchar(10)", "CHARSET utf8");
        PreparedStatement stmt = sharedConnection.prepareStatement("insert into testSetNString (id, strm) values (?,?)");
        stmt.setInt(1, 1);
        stmt.setNString(2, "hello");
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from testSetNString");
        assertTrue(rs.next());
        assertTrue(rs.getObject(2) instanceof String);
        assertTrue(rs.getNString(2).equals("hello"));

    }

    @Test
    public void testSetObjectNString() throws Exception {

        createTable("testSetObjectNString", "id int not null primary key, strm varchar(10), strm2 varchar(10)", "CHARSET utf8");
        PreparedStatement stmt = sharedConnection.prepareStatement("insert into testSetObjectNString (id, strm, strm2) values (?, ?, ?)");
        stmt.setInt(1, 2);
        stmt.setObject(2, "hello");
        stmt.setObject(3, "hello", Types.NCLOB);
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from testSetObjectNString");
        assertTrue(rs.next());
        assertTrue(rs.getObject(2) instanceof String);
        assertTrue(rs.getString(2).equals("hello"));
        assertTrue(rs.getObject(3) instanceof String);
        assertTrue(rs.getString(3).equals("hello"));
        assertEquals(5, rs.getNClob(2).getAsciiStream().available());
        assertEquals(5, rs.getNClob(3).getAsciiStream().available());
    }


    @Test
    public void testSetNCharacter() throws Exception {

        createTable("testSetNCharacter", "id int not null primary key, strm text", "CHARSET utf8");
        PreparedStatement stmt = sharedConnection.prepareStatement("insert into testSetNCharacter (id, strm) values (?,?)");
        String toInsert = "\u00D8abcdefgh\njklmn\"";

        stmt.setInt(1, 1);
        stmt.setNCharacterStream(2, new StringReader(toInsert));
        stmt.execute();

        stmt.setInt(1, 2);
        stmt.setNCharacterStream(2, new StringReader(toInsert), 3);
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from testSetNCharacter");
        assertTrue(rs.next());
        assertTrue(rs.getObject(2) instanceof String);
        assertTrue(rs.getCharacterStream(2) instanceof Reader);
        checkCharStream(rs.getCharacterStream(2), toInsert);

        assertTrue(rs.next());
        checkCharStream(rs.getCharacterStream(2), toInsert.substring(0, 3));
    }

    @Test
    public void testSetObjectNCharacter() throws Exception {

        createTable("testSetObjectNCharacter", "id int not null primary key, strm text", "CHARSET utf8");
        PreparedStatement stmt = sharedConnection.prepareStatement("insert into testSetObjectNCharacter (id, strm) values (?,?)");
        String toInsert = "\u00D8abcdefgh\njklmn\"";
        ;

        stmt.setInt(1, 1);
        stmt.setObject(2, new StringReader(toInsert));
        stmt.execute();

        stmt.setInt(1, 2);
        stmt.setObject(2, new StringReader(toInsert), Types.LONGNVARCHAR);
        stmt.execute();

        stmt.setInt(1, 3);
        stmt.setObject(2, new StringReader(toInsert), Types.LONGNVARCHAR, 3);
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from testSetObjectNCharacter");
        assertTrue(rs.next());
        Reader reader1 = rs.getObject(2, Reader.class);
        assertNotNull(reader1);
        assertTrue(rs.getObject(2) instanceof String);
        assertTrue(rs.getCharacterStream(2) instanceof Reader);
        checkCharStream(rs.getCharacterStream(2), toInsert);

        assertTrue(rs.next());
        assertTrue(rs.getObject(2) instanceof String);
        checkCharStream(rs.getCharacterStream(2), toInsert);

        assertTrue(rs.next());
        checkCharStream(rs.getCharacterStream(2), toInsert.substring(0, 3));
    }

    private void checkCharStream(Reader reader, String comparedValue) throws Exception {

        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = reader.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(comparedValue, sb.toString());
    }
}
