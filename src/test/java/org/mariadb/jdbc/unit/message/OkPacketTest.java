// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.message;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.OkPacket;
import org.mariadb.jdbc.util.constants.Capabilities;
import org.mariadb.jdbc.util.constants.StateChange;

public class OkPacketTest {

  /** Records the database the OK packet parser tracks, with CLIENT_SESSION_TRACK enabled. */
  private static class DbRecorder {
    String database;
  }

  private static Context context(DbRecorder rec) {
    return (Context)
        Proxy.newProxyInstance(
            Context.class.getClassLoader(),
            new Class[] {Context.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "hasClientCapability":
                  return ((Long) args[0] & Capabilities.CLIENT_SESSION_TRACK) != 0;
                case "setDatabase":
                  rec.database = (String) args[0];
                  return null;
                case "getDatabase":
                  return rec.database;
                default:
                  Class<?> rt = method.getReturnType();
                  if (rt == boolean.class) return false;
                  if (rt == int.class) return 0;
                  if (rt == long.class) return 0L;
                  return null;
              }
            });
  }

  /**
   * Builds an OK packet whose session-state info holds an unhandled state-change type (here
   * SESSION_TRACK_STATE_CHANGE) immediately followed by a SESSION_TRACK_SCHEMA entry switching the
   * current database to {@code db}.
   */
  private static byte[] okPacketWithUnknownThenSchema(String db) throws Exception {
    byte[] dbBytes = db.getBytes(StandardCharsets.UTF_8);
    byte[] unknown =
        new byte[] {(byte) StateChange.SESSION_TRACK_STATE_CHANGE, 0x01, 0x01}; // type, len, data
    ByteArrayOutputStream schema = new ByteArrayOutputStream();
    schema.write(StateChange.SESSION_TRACK_SCHEMA);
    schema.write(dbBytes.length + 1); // data length (db length-encoded prefix + bytes)
    schema.write(dbBytes.length); // db name length
    schema.write(dbBytes);

    ByteArrayOutputStream block = new ByteArrayOutputStream();
    block.write(unknown);
    block.write(schema.toByteArray());
    byte[] blockBytes = block.toByteArray();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0x00); // ok header
    out.write(0x00); // affected rows
    out.write(0x00); // last insert id
    out.write(0x02);
    out.write(0x00); // server status
    out.write(0x00);
    out.write(0x00); // warnings
    out.write(0x00); // info (empty)
    out.write(blockBytes.length); // session state info length
    out.write(blockBytes);
    return out.toByteArray();
  }

  @Test
  public void unknownSessionStateTypeKeepsParsingSchema() throws Exception {
    byte[] packet = okPacketWithUnknownThenSchema("newdb");
    // backing array larger than the packet, mirroring the reusable read buffer
    byte[] backing = Arrays.copyOf(packet, packet.length + 16);

    DbRecorder rec = new DbRecorder();
    OkPacket.parse(new ReadableByteBuf(backing, packet.length), context(rec));
    Assertions.assertEquals("newdb", rec.database);

    DbRecorder rec2 = new DbRecorder();
    OkPacket.parseWithInfo(new ReadableByteBuf(backing, packet.length), context(rec2));
    Assertions.assertEquals("newdb", rec2.database);
  }
}
