package org.mariadb.jdbc.unit.client;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.unit.type.GeometryTest;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

public class ColumnDecoderTest {

    @Test
    public void testWrongEncoding() throws SQLException {
        /*
       +--------------------------------------------------+
       |  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |
+------+--------------------------------------------------+------------------+
|000000| 35 00 00 02 03 64 65 66  05 74 65 73 74 6A 08 42 | 5....defg.testj.B |
|000010| 69 74 43 6F 64 65 63 08  42 69 74 43 6F 64 65 63 | itCodec.BitCodec |
|000020| 07 74 31 61 6C 69 61 73  02 74 31 00 0C 3F 00 01 | .t1alias.t1..?.. |
|000030| 00 00 00 10 20 00 00 00  00                      | .... ....        |
+------+--------------------------------------------------+------------------+
         */

        byte[] ptBytes = GeometryTest.hexStringToByteArray("04 64 65 66 67 05 74 65 73 74 6A 08 42" +
                "69 74 43 6F 64 65 63 08  42 69 74 43 6F 64 65 63" +
                "07 74 31 61 6C 69 61 73  02 74 31 00 0C 3F 00 01" +
                "00 00 00 10 20 00 00 00  00");
        ReadableByteBuf readBuf = new StandardReadableByteBuf(ptBytes, ptBytes.length);
        ColumnDecoder columnDecoder = ColumnDecoder.decodeStd(readBuf);
        assertEquals("t1", columnDecoder.getColumnName());
        assertEquals("t1alias", columnDecoder.getColumnAlias());
        assertEquals("BitCodec", columnDecoder.getTable());
        assertEquals("BitCodec", columnDecoder.getTableAlias());
        assertEquals("defg", columnDecoder.getCatalog());
        assertEquals("testj", columnDecoder.getSchema());

        columnDecoder = ColumnDecoder.create("db", "test", DataType.GEOMETRY, 0);
        assertEquals("test", columnDecoder.getColumnName());
        assertEquals("test", columnDecoder.getColumnAlias());
        assertEquals("db", columnDecoder.getCatalog());
    }
}
