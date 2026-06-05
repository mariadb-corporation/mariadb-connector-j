// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.client.context;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.RedoContext;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.message.client.QueryPacket;
import org.mariadb.jdbc.message.server.InitialHandshakePacket;
import org.mariadb.jdbc.util.constants.ServerStatus;

/**
 * Unit tests for {@link RedoContext} redo-buffer lifecycle, in particular that a statement is only
 * kept for transaction replay while a transaction is actually open. This guarantees that an
 * autocommit statement (e.g. an INSERT) is never replayed after a failover.
 */
public class RedoContextTest {

  /** Server status mirroring the OK packet of a statement run inside an open transaction. */
  private static final int IN_TX = ServerStatus.IN_TRANSACTION | ServerStatus.AUTOCOMMIT;

  /** Server status mirroring the OK packet of an autocommit statement (no transaction open). */
  private static final int NO_TX = ServerStatus.AUTOCOMMIT;

  private static RedoContext newRedoContext() throws SQLException {
    InitialHandshakePacket handshake =
        InitialHandshakePacket.decode(new ReadableByteBuf(buildHandshake()));
    Configuration conf = Configuration.parse("jdbc:mariadb://localhost:3306/test");
    ExceptionFactory exceptionFactory = new ExceptionFactory(conf, null);
    return new RedoContext(
        null, handshake, 0L, conf, exceptionFactory, null, Boolean.FALSE, () -> {});
  }

  /**
   * Builds a minimal MariaDB initial-handshake packet. All capability flags are 0 so that
   * SECURE_CONNECTION / PLUGIN_AUTH branches are skipped, keeping the layout minimal.
   */
  private static byte[] buildHandshake() {
    ByteBuffer buf = ByteBuffer.allocate(128);
    buf.put((byte) 0x0a); // protocol version
    buf.put("11.4.0-MariaDB".getBytes(StandardCharsets.US_ASCII));
    buf.put((byte) 0x00); // server version null terminator
    buf.put(new byte[] {1, 0, 0, 0}); // thread id (4 bytes)
    buf.put(new byte[8]); // seed part 1 (8 bytes)
    buf.put((byte) 0x00); // filler
    buf.put(new byte[] {0, 0}); // capabilities lower 2 bytes = 0
    buf.put((byte) 0x00); // default collation
    buf.put(new byte[] {0, 0}); // server status = 0
    buf.put(new byte[] {0, 0}); // capabilities upper 2 bytes = 0
    buf.put((byte) 0x00); // (no PLUGIN_AUTH) skipped salt-length byte
    buf.put(new byte[6]); // reserved skip(6)
    buf.put(new byte[4]); // mariadb additional capabilities
    buf.put((byte) 0x00); // trailing skip
    return Arrays.copyOf(buf.array(), buf.position());
  }

  private static QueryPacket query(String sql) {
    return new QueryPacket(sql);
  }

  @Test
  public void autocommitStatementNotBuffered() throws SQLException {
    RedoContext ctx = newRedoContext();

    // mirror ReplayClient.execute order: response read (setServerStatus) THEN saveRedo
    ctx.setServerStatus(NO_TX);
    ctx.saveRedo(query("INSERT INTO t VALUES (1)"));

    assertEquals(
        0,
        ctx.getTransactionSaver().getIdx(),
        "autocommit statement must not be retained for replay");
  }

  @Test
  public void transactionalStatementBuffered() throws SQLException {
    RedoContext ctx = newRedoContext();

    ctx.setServerStatus(IN_TX);
    ctx.saveRedo(query("INSERT INTO t VALUES (1)"));

    assertEquals(
        1, ctx.getTransactionSaver().getIdx(), "in-transaction statement must be retained");
  }

  /**
   * Regression test for the clear-then-re-add ordering bug: an autocommit INSERT executed before an
   * explicitly started transaction must not linger in the redo buffer and get replayed as part of
   * that later transaction.
   */
  @Test
  public void autocommitStatementNotCarriedIntoLaterTransaction() throws SQLException {
    RedoContext ctx = newRedoContext();

    // 1. autocommit INSERT - commits immediately, nothing to replay
    ctx.setServerStatus(NO_TX);
    ctx.saveRedo(query("INSERT INTO t VALUES (1)"));

    // 2. explicit BEGIN while autocommit is on - opens a transaction
    ctx.setServerStatus(IN_TX);
    ctx.saveRedo(query("BEGIN"));

    // 3. INSERT inside the transaction
    ctx.setServerStatus(IN_TX);
    ctx.saveRedo(query("INSERT INTO t VALUES (2)"));

    assertEquals(
        2,
        ctx.getTransactionSaver().getIdx(),
        "only the BEGIN and the in-transaction INSERT must be replayable");
    assertEquals("BEGIN", ctx.getTransactionSaver().getBuffers()[0].description());
    assertEquals(
        "INSERT INTO t VALUES (2)", ctx.getTransactionSaver().getBuffers()[1].description());
  }

  /** Ending a transaction (COMMIT / ROLLBACK / implicit commit) must empty the redo buffer. */
  @Test
  public void transactionEndClearsBuffer() throws SQLException {
    RedoContext ctx = newRedoContext();

    ctx.setServerStatus(IN_TX);
    ctx.saveRedo(query("INSERT INTO t VALUES (1)"));
    ctx.setServerStatus(IN_TX);
    ctx.saveRedo(query("INSERT INTO t VALUES (2)"));
    assertEquals(2, ctx.getTransactionSaver().getIdx());

    // COMMIT: server reports no transaction in progress
    ctx.setServerStatus(NO_TX);
    ctx.saveRedo(query("COMMIT"));

    assertEquals(
        0,
        ctx.getTransactionSaver().getIdx(),
        "committing a transaction must clear the redo buffer");
  }
}
