// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

package com.singlestore.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.Statement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.TimeZone;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the {@code preserveInstants} connection option introduced for {@link
 * com.singlestore.jdbc.plugin.codec.OffsetDateTimeCodec#encodeText}.
 *
 * <p>With {@code rewriteBatchedStatements=true}, batched {@link OffsetDateTime} parameters flow
 * through {@code encodeText}, which converts them to a wall-clock string via the JVM default time
 * zone. Across DST and historical timezone boundaries (e.g. 1987-1988 KDT in {@code Asia/Seoul})
 * the JVM IANA tzdata and the server's {@code @@session.time_zone} interpretation can disagree,
 * drifting the stored UTC instant by one hour. When {@code preserveInstants=true}, the codec
 * instead emits {@code FROM_UNIXTIME(epoch[.us])}, which the server evaluates as a deterministic
 * UTC instant regardless of its session timezone.
 *
 * <p>Each test pins the JVM default time zone to {@code Asia/Seoul} for the duration of the body so
 * that the codec's wall-clock conversion path exercises tzdata with DST history. The {@code
 * FROM_UNIXTIME} path is JVM-TZ independent and must round-trip exactly regardless.
 */
public class OffsetDateTimePreserveInstantsTest extends CommonCodecTest {

  private TimeZone originalDefaultTimeZone;

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE OffsetDateTimePreserveInstants ("
            + "id INT NOT NULL PRIMARY KEY, ts TIMESTAMP(6) NULL DEFAULT NULL)");
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS OffsetDateTimePreserveInstants");
  }

  @BeforeEach
  public void pinJvmTimeZone() throws SQLException {
    originalDefaultTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Seoul"));
    try (Statement stmt = sharedConn.createStatement()) {
      stmt.execute("TRUNCATE TABLE OffsetDateTimePreserveInstants");
    }
  }

  @AfterEach
  public void restoreJvmTimeZone() {
    TimeZone.setDefault(originalDefaultTimeZone);
  }

  private static OffsetDateTime atUtcEpoch(long epochSec, int nanos) {
    return OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSec, nanos), ZoneOffset.UTC);
  }

  private static long unixTimestamp(Connection con, int id) throws SQLException {
    try (PreparedStatement ps =
        con.prepareStatement(
            "SELECT UNIX_TIMESTAMP(ts) FROM OffsetDateTimePreserveInstants WHERE id = ?")) {
      ps.setInt(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next(), "no row for id=" + id);
        return rs.getLong(1);
      }
    }
  }

  private static String microFraction(Connection con, int id) throws SQLException {
    try (PreparedStatement ps =
        con.prepareStatement(
            "SELECT DATE_FORMAT(ts, '%f') FROM OffsetDateTimePreserveInstants WHERE id = ?")) {
      ps.setInt(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next(), "no row for id=" + id);
        return rs.getString(1);
      }
    }
  }

  /**
   * With {@code preserveInstants=true} and {@code rewriteBatchedStatements=true} (which forces
   * every batched parameter through {@code encodeText}), batched {@link OffsetDateTime} parameters
   * must round-trip the absolute UTC instant exactly, including across historical KDT (1988-07-15
   * in {@code Asia/Seoul}) and at the {@code FROM_UNIXTIME} accepted boundaries.
   */
  @Test
  public void preserveInstantsRoundTripsBatchedOffsetDateTime() throws SQLException {
    try (Connection con = createCon("preserveInstants=true&rewriteBatchedStatements=true");
        PreparedStatement ps =
            con.prepareStatement(
                "INSERT INTO OffsetDateTimePreserveInstants (id, ts) VALUES (?, ?)")) {
      // 1988-07-15T04:00:00Z = KDT 14:00 in Asia/Seoul.
      long kdt = 584_942_400L;
      // 2025-07-15T05:00:00Z = modern KST 14:00.
      long modern = 1_752_555_600L;
      // FROM_UNIXTIME accepts [0, INT32_MAX].
      long lowerBound = 1L;
      long upperBound = (long) Integer.MAX_VALUE;

      long[][] cases = {{1, kdt}, {2, modern}, {3, lowerBound}, {4, upperBound}};
      for (long[] c : cases) {
        ps.setInt(1, (int) c[0]);
        ps.setObject(2, atUtcEpoch(c[1], 0), Types.TIMESTAMP);
        ps.addBatch();
      }
      ps.executeBatch();

      for (long[] c : cases) {
        assertEquals(
            c[1],
            unixTimestamp(con, (int) c[0]),
            "preserveInstants must preserve the UTC instant exactly for id=" + c[0]);
      }
    }
  }

  /**
   * The {@code FROM_UNIXTIME(epoch.NNNNNN)} branch must preserve microsecond precision and zero-pad
   * fractional components correctly. In particular: 1µs must serialize as {@code .000001}, not
   * {@code .1}. Sub-microsecond nanoseconds are not representable in TIMESTAMP(6) and are truncated
   * server-side.
   */
  @Test
  public void preserveInstantsPreservesMicrosecondPrecision() throws SQLException {
    long base = 1_752_555_600L; // 2025-07-15T05:00:00Z

    try (Connection con = createCon("preserveInstants=true&rewriteBatchedStatements=true");
        PreparedStatement ps =
            con.prepareStatement(
                "INSERT INTO OffsetDateTimePreserveInstants (id, ts) VALUES (?, ?)")) {
      // (id, nanos, expected stored micros)
      Object[][] cases = {
        {10, 123_456_000, "123456"}, // typical 6-digit
        {11, 1_000, "000001"}, // 1 µs: requires six-digit zero-pad
        {12, 100_000_000, "100000"}, // 100 ms
        {13, 500, "000000"}, // sub-µs truncated by TIMESTAMP(6)
      };
      for (Object[] c : cases) {
        ps.setInt(1, (int) c[0]);
        ps.setObject(2, atUtcEpoch(base, (int) c[1]), Types.TIMESTAMP);
        ps.addBatch();
      }
      ps.executeBatch();

      for (Object[] c : cases) {
        assertEquals(
            c[2],
            microFraction(con, (int) c[0]),
            "preserveInstants must preserve microsecond precision for id=" + c[0]);
      }
    }
  }

  /**
   * With {@code preserveInstants=false} (the default), the codec must continue to emit the legacy
   * wall-clock literal so existing applications see no behavior change. Verifies via a delta check
   * across two adjacent UTC instants (60 s apart) — this anchors on relative correctness without
   * depending on the server's session timezone for absolute epoch interpretation.
   */
  @Test
  public void preserveInstantsDisabledIsBackwardsCompatible() throws SQLException {
    long base = 1_752_555_600L; // 2025-07-15T05:00:00Z

    try (Connection con = createCon("preserveInstants=false&rewriteBatchedStatements=true");
        PreparedStatement ps =
            con.prepareStatement(
                "INSERT INTO OffsetDateTimePreserveInstants (id, ts) VALUES (?, ?)")) {
      ps.setInt(1, 30);
      ps.setObject(2, atUtcEpoch(base, 0), Types.TIMESTAMP);
      ps.addBatch();
      ps.setInt(1, 31);
      ps.setObject(2, atUtcEpoch(base + 60L, 0), Types.TIMESTAMP);
      ps.addBatch();
      ps.executeBatch();

      assertEquals(
          60L,
          unixTimestamp(con, 31) - unixTimestamp(con, 30),
          "legacy path must preserve relative 60s delta");
    }
  }
}
