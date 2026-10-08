// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.message.server.OkPacket;
import org.mariadb.jdbc.util.constants.Capabilities;
import org.mariadb.jdbc.util.constants.StateChange;

/**
 * Lengths inside the session-state block of an OK packet are server-declared: a name, value or
 * schema length larger than what the block holds is malformed. The rest of the block is ignored,
 * nothing is read past it and nothing is allocated from the declared length.
 */
public class OkPacketTest {

  /** Records what the parser reports to the context, with CLIENT_SESSION_TRACK negotiated. */
  private static class Recorder {
    String database;
    Long autoIncrement;
  }

  private static Context context(Recorder rec) {
    return (Context)
        Proxy.newProxyInstance(
            Context.class.getClassLoader(),
            new Class<?>[] {Context.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "hasClientCapability":
                  return ((Long) args[0] & Capabilities.CLIENT_SESSION_TRACK) != 0;
                case "setDatabase":
                  rec.database = (String) args[0];
                  return null;
                case "setAutoIncrement":
                  rec.autoIncrement = (Long) args[0];
                  return null;
                default:
                  Class<?> rt = method.getReturnType();
                  if (rt == boolean.class) return false;
                  if (rt == int.class) return 0;
                  if (rt == long.class) return 0L;
                  return null;
              }
            });
  }

  /** OK packet header with no rows, no insert id, no info, followed by one session-state block. */
  private static StandardReadableByteBuf okPacket(byte[] stateBlock) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0x00); // OK header
    out.write(0x00); // affected rows
    out.write(0x00); // last insert id
    out.write(new byte[] {0x00, 0x40}); // status: SERVER_SESSION_STATE_CHANGED
    out.write(new byte[] {0x00, 0x00}); // warnings
    out.write(0x00); // info: empty
    out.write(stateBlock.length);
    out.write(stateBlock);
    byte[] packet = out.toByteArray();
    return new StandardReadableByteBuf(packet, packet.length);
  }

  /** SESSION_TRACK_SYSTEM_VARIABLES entry with the given raw name and value fields. */
  private static byte[] systemVariable(byte[] nameField, byte[] valueField) throws Exception {
    ByteArrayOutputStream entry = new ByteArrayOutputStream();
    entry.write(StateChange.SESSION_TRACK_SYSTEM_VARIABLES);
    entry.write(nameField.length + valueField.length);
    entry.write(nameField);
    entry.write(valueField);
    return entry.toByteArray();
  }

  private static byte[] lengthEncoded(String s) {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    byte[] field = new byte[bytes.length + 1];
    field[0] = (byte) bytes.length;
    System.arraycopy(bytes, 0, field, 1, bytes.length);
    return field;
  }

  @Test
  public void systemVariableChangeApplied() throws Exception {
    Recorder rec = new Recorder();
    new OkPacket(
        okPacket(systemVariable(lengthEncoded("auto_increment_increment"), lengthEncoded("3"))),
        context(rec));
    assertEquals(3L, rec.autoIncrement);
  }

  @Test
  public void variableNameLengthBeyondBlockIgnored() throws Exception {
    // name declares 20 bytes, the entry holds 4: the block is ignored, no exception, no effect
    byte[] name = {20, 'a', 'b', 'c'};
    Recorder rec = new Recorder();
    new OkPacket(okPacket(systemVariable(name, new byte[0])), context(rec));
    assertNull(rec.autoIncrement);
  }

  @Test
  public void variableValueLengthBeyondBlockIgnored() throws Exception {
    // value declares 9 bytes, the entry holds 1
    byte[] value = {9, '3'};
    Recorder rec = new Recorder();
    new OkPacket(
        okPacket(systemVariable(lengthEncoded("auto_increment_increment"), value)), context(rec));
    assertNull(rec.autoIncrement);
  }

  @Test
  public void unknownStateTypeThenSchemaChange() throws Exception {
    // an unhandled state-change type must be skipped within the block, so that the schema entry
    // following it is still applied
    ByteArrayOutputStream block = new ByteArrayOutputStream();
    block.write(StateChange.SESSION_TRACK_STATE_CHANGE);
    block.write(1); // entry length
    block.write(1); // entry data
    block.write(StateChange.SESSION_TRACK_SCHEMA);
    block.write(3);
    block.write(lengthEncoded("db"));
    Recorder rec = new Recorder();
    new OkPacket(okPacket(block.toByteArray()), context(rec));
    assertEquals("db", rec.database);
  }

  @Test
  public void schemaChangeAppliedAndMalformedIgnored() throws Exception {
    ByteArrayOutputStream schema = new ByteArrayOutputStream();
    schema.write(StateChange.SESSION_TRACK_SCHEMA);
    schema.write(3); // entry length: length-encoded prefix + "db"
    schema.write(lengthEncoded("db"));
    Recorder rec = new Recorder();
    new OkPacket(okPacket(schema.toByteArray()), context(rec));
    assertEquals("db", rec.database);

    // database name declares 9 bytes, the entry holds 2
    ByteArrayOutputStream bad = new ByteArrayOutputStream();
    bad.write(StateChange.SESSION_TRACK_SCHEMA);
    bad.write(3);
    bad.write(new byte[] {9, 'd', 'b'});
    Recorder rec2 = new Recorder();
    new OkPacket(okPacket(bad.toByteArray()), context(rec2));
    assertNull(rec2.database);
  }
}
