// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.integration.resultset;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.result.SequentialResult;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.util.ClosableLock;
import org.mariadb.jdbc.client.util.MutableByte;
import org.mariadb.jdbc.integration.Common;

/** Sequential result-set fed with forged packets, as a malicious proxy could send. */
public class SequentialResultProtocolTest extends Common {

  private static final ColumnDecoder[] META = {
    ColumnDecoder.create("db", "col", DataType.VARCHAR, 0)
  };

  private static byte[] packet(byte[] payload) {
    byte[] p = new byte[4 + payload.length];
    p[0] = (byte) payload.length;
    p[1] = (byte) (payload.length >>> 8);
    p[2] = (byte) (payload.length >>> 16);
    p[3] = 1;
    System.arraycopy(payload, 0, p, 4, payload.length);
    return p;
  }

  private static byte[] eofPacket() {
    // OK packet with 0xFE header: affected rows, insert id, status, warnings
    return packet(new byte[] {(byte) 0xFE, 0, 0, 2, 0, 0, 0});
  }

  private SequentialResult result(Connection con, Statement stmt, byte[]... packets)
      throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    for (byte[] p : packets) stream.write(p);
    Reader reader =
        new Reader(
            new ByteArrayInputStream(stream.toByteArray()),
            con.getContext().getConf(),
            new MutableByte());
    reader.setMaxAllowedPacket(40 * 1024 * 1024);
    return new SequentialResult(
        stmt,
        false,
        0,
        META,
        reader,
        con.getContext(),
        new ClosableLock(),
        ResultSet.TYPE_FORWARD_ONLY,
        false,
        false);
  }

  @Test
  public void fieldLengthExceedingPacket() throws Exception {
    // field announces 5 bytes, packet only holds 2: buffered getter
    try (Connection con = createCon()) {
      Statement stmt = con.createStatement();
      SequentialResult rs = result(con, stmt, packet(new byte[] {5, 'a', 'b'}), eofPacket());
      assertTrue(rs.next());
      assertThrowsContains(
          SQLNonTransientConnectionException.class, () -> rs.getString(1), "exceeds the 2 bytes");
      assertTrue(con.isClosed());
      assertThrowsContains(SQLException.class, rs::next, "closed");
    }
    // same with a stream getter
    try (Connection con = createCon()) {
      Statement stmt = con.createStatement();
      SequentialResult rs = result(con, stmt, packet(new byte[] {5, 'a', 'b'}), eofPacket());
      assertTrue(rs.next());
      assertThrowsContains(
          SQLNonTransientConnectionException.class,
          () -> rs.getBinaryStream(1),
          "exceeds the 2 bytes");
      assertTrue(con.isClosed());
    }
    // valid field is fine, and connection stays open
    try (Connection con = createCon()) {
      Statement stmt = con.createStatement();
      SequentialResult rs = result(con, stmt, packet(new byte[] {2, 'a', 'b'}), eofPacket());
      assertTrue(rs.next());
      assertEquals("ab", rs.getString(1));
      assertFalse(rs.next());
      assertFalse(con.isClosed());
    }
  }

  @Test
  public void fieldLengthExceedingMaxAllowedPacket() throws Exception {
    // row split in packets, so the row end is unknown: a huge announced length must be rejected
    // before any allocation, from the client maxAllowedPacket limit
    for (boolean buffered : new boolean[] {true, false}) {
      try (Connection con = createCon()) {
        Statement stmt = con.createStatement();
        byte[] first = new byte[0xFFFFFF];
        long declared = Integer.MAX_VALUE - 10;
        first[0] = (byte) 0xFE;
        for (int i = 0; i < 8; i++) first[1 + i] = (byte) (declared >>> (8 * i));
        SequentialResult rs = result(con, stmt, packet(first), packet(new byte[10]), eofPacket());
        assertTrue(rs.next());
        assertThrowsContains(
            SQLNonTransientConnectionException.class,
            () -> {
              if (buffered) rs.getBytes(1);
              else rs.getBinaryStream(1);
            },
            "exceeds maxAllowedPacket");
        assertTrue(con.isClosed());
      }
    }
    // row bigger than maxAllowedPacket, whatever the field lengths
    try (Connection con = createCon()) {
      Statement stmt = con.createStatement();
      byte[] fragment = new byte[0xFFFFFF];
      java.util.Arrays.fill(fragment, (byte) 'a');
      fragment[0] = 5;
      SequentialResult rs =
          result(con, stmt, packet(fragment), packet(fragment), packet(fragment), eofPacket());
      assertTrue(rs.next());
      assertEquals("aaaaa", rs.getString(1));
      // skipping the rest of the row reads the following fragments
      assertThrowsContains(
          SQLNonTransientConnectionException.class, rs::next, "Error while streaming");
      assertTrue(con.isClosed());
    }
  }

  @Test
  public void fieldLengthExceedingFollowingPacket() throws Exception {
    // row split in packets: the field announces more bytes than the following packet brings
    try (Connection con = createCon()) {
      Statement stmt = con.createStatement();
      byte[] first = new byte[0xFFFFFF];
      long declared = 0xFFFFFFL - 9 + 100; // needs 100 bytes in the next packet
      first[0] = (byte) 0xFE;
      for (int i = 0; i < 8; i++) first[1 + i] = (byte) (declared >>> (8 * i));
      for (int i = 9; i < first.length; i++) first[i] = 'a';
      byte[] second = new byte[10];
      java.util.Arrays.fill(second, (byte) 'a');
      SequentialResult rs = result(con, stmt, packet(first), packet(second), eofPacket());
      assertTrue(rs.next());
      InputStream is = rs.getBinaryStream(1);
      byte[] buf = new byte[65536];
      long total = 0;
      IOException ioe =
          assertThrows(
              IOException.class,
              () -> {
                int len;
                while ((len = is.read(buf)) > 0) {}
              });
      assertTrue(ioe.getMessage().contains("ended before the end"), ioe.getMessage());
      assertTrue(con.isClosed());
    }
  }
}
