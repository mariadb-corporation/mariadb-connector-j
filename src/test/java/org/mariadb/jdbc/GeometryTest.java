package org.mariadb.jdbc;

import org.junit.After;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GeometryTest extends BaseTest {

    private void geometryTest(String geometryString, String geometryBinary) throws SQLException {
        Statement stmt = connection.createStatement();
        createTestTable("geom_test", "g geometry");
        ResultSet rs;
        if (geometryBinary == null) {
            rs = stmt.executeQuery("SELECT AsWKB(GeomFromText('" + geometryString + "'))");
            rs.next();
            geometryBinary = DatatypeConverter.printHexBinary(rs.getBytes(1));
        }
        BigInteger geometryNumber = new BigInteger(geometryBinary, 16);
        String sql = "INSERT INTO geom_test VALUES (GeomFromText('" + geometryString + "'))";
        stmt.execute(sql);
        rs = stmt.executeQuery("SELECT AsText(g), AsBinary(g), g FROM geom_test");
        rs.next();
        // as text
        assertEquals(geometryString, rs.getString(1));
        // as binary
        String returnWKB = DatatypeConverter.printHexBinary((byte[]) rs.getObject(2));
        assertEquals(geometryBinary, returnWKB);
        // as object
        Object geometry = null;
        try {
            geometry = rs.getObject(3);
        } catch (Exception e) {
            assertTrue(false);
        }
        String returnGeometry = DatatypeConverter.printHexBinary((byte[]) geometry);
        BigInteger returnNumber = new BigInteger(returnGeometry, 16);
        assertEquals(geometryNumber, returnNumber);
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
    }

    @Test
    public void pointTest() throws SQLException {
        String pointString = "POINT(1 1)";
        String pointWKB = "0101000000000000000000F03F000000000000F03F";
        geometryTest(pointString, pointWKB);
    }

    @Test
    public void lineStringTest() throws SQLException {
        String lineString = "LINESTRING(0 0,1 1,2 2)";
        geometryTest(lineString, null);
    }

    @Test
    public void polygonTest() throws SQLException {
        String polygonString = "POLYGON((0 0,10 0,0 10,0 0))";
        geometryTest(polygonString, null);
    }

}
