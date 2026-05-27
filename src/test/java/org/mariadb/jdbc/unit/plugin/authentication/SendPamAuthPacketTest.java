// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.plugin.authentication;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableByte;
import org.mariadb.jdbc.integration.util.RecordingAuthDialogCallback;
import org.mariadb.jdbc.plugin.authentication.standard.SendPamAuthPacket;

/**
 * Verifies that {@link SendPamAuthPacket} routes server-issued dialog prompts to the
 * application-registered {@link org.mariadb.jdbc.plugin.AuthDialogCallback}, mirroring the C
 * connector's {@code mariadb_auth_dialog} dlsym hook.
 */
public class SendPamAuthPacketTest {

  @BeforeEach
  void resetRecorder() {
    RecordingAuthDialogCallback.responses.clear();
    RecordingAuthDialogCallback.calls.clear();
  }

  @Test
  void process_invokesAuthDialogCallbackForLaterRounds() throws Exception {
    RecordingAuthDialogCallback.responses.add("1234"); // round 2 answer

    Configuration conf = Configuration.parse("jdbc:mariadb://localhost/");

    // Inbound packets the fake server "sends":
    //   1) dialog prompt: 1 byte flags + UTF-8 prompt text
    //   2) OK packet: 0x00 (single byte sufficient — process() only inspects the first byte)
    ByteArrayOutputStream serverStream = new ByteArrayOutputStream();
    byte[] promptText = "Enter PIN: ".getBytes(StandardCharsets.UTF_8);
    byte[] promptPacket = new byte[1 + promptText.length];
    // flag 0x05 = MariaDB server's PASSWORD_QUESTION (bit 0 set, echo bit cleared). We avoid
    // 0x00/0xfe/0xff which the protocol reserves for OK/Switch/ERR — those would short-circuit
    // process() before it reaches the prompt.
    promptPacket[0] = 0x05;
    System.arraycopy(promptText, 0, promptPacket, 1, promptText.length);
    writePacket(serverStream, (byte) 1, promptPacket);
    writePacket(serverStream, (byte) 2, new byte[] {0x00});

    Reader reader =
        new Reader(new ByteArrayInputStream(serverStream.toByteArray()), conf, new MutableByte());

    ByteArrayOutputStream clientStream = new ByteArrayOutputStream();
    Writer writer = new Writer(clientStream, 1024, 0xffffff, new MutableByte(), new MutableByte());

    SendPamAuthPacket pam = new SendPamAuthPacket("first", conf);
    pam.process(writer, reader, null, false);

    assertEquals(1, RecordingAuthDialogCallback.calls.size());
    RecordingAuthDialogCallback.Call call = RecordingAuthDialogCallback.calls.get(0);
    assertEquals(2, call.round);
    assertEquals("Enter PIN: ", call.prompt);
    assertFalse(call.echo, "password-style prompt: echo must be false");

    // Client should have written two packets: "first\0" then "1234\0".
    assertEquals("first", payloadString(clientStream.toByteArray(), 0));
    assertEquals(
        "1234",
        payloadString(clientStream.toByteArray(), packetEnd(clientStream.toByteArray(), 0)));
  }

  @Test
  void echoFlagBitSetSurfacesAsTrueToCallback() throws Exception {
    RecordingAuthDialogCallback.responses.add("answer");
    Configuration conf = Configuration.parse("jdbc:mariadb://localhost/");

    ByteArrayOutputStream serverStream = new ByteArrayOutputStream();
    byte[] promptText = "Your name? ".getBytes(StandardCharsets.UTF_8);
    byte[] promptPacket = new byte[1 + promptText.length];
    promptPacket[0] = 0x02; // flags: echo on (info / normal input)
    System.arraycopy(promptText, 0, promptPacket, 1, promptText.length);
    writePacket(serverStream, (byte) 1, promptPacket);
    writePacket(serverStream, (byte) 2, new byte[] {0x00});

    Reader reader =
        new Reader(new ByteArrayInputStream(serverStream.toByteArray()), conf, new MutableByte());
    Writer writer =
        new Writer(
            new ByteArrayOutputStream(), 1024, 0xffffff, new MutableByte(), new MutableByte());

    new SendPamAuthPacket("first", conf).process(writer, reader, null, false);

    assertEquals(1, RecordingAuthDialogCallback.calls.size());
    assertTrue(RecordingAuthDialogCallback.calls.get(0).echo);
  }

  /** Writes a single MariaDB-protocol packet ({@code [len-LE 3 bytes][seq][payload]}). */
  private static void writePacket(ByteArrayOutputStream out, byte sequence, byte[] payload) {
    int len = payload.length;
    out.write(len & 0xff);
    out.write((len >> 8) & 0xff);
    out.write((len >> 16) & 0xff);
    out.write(sequence);
    out.write(payload, 0, payload.length);
  }

  /** Read a null-terminated UTF-8 string from the payload of the packet starting at {@code off}. */
  private static String payloadString(byte[] stream, int off) {
    int payloadStart = off + 4; // skip 3 length + 1 sequence
    int len =
        (stream[off] & 0xff) | ((stream[off + 1] & 0xff) << 8) | ((stream[off + 2] & 0xff) << 16);
    int end = payloadStart;
    while (end < payloadStart + len && stream[end] != 0) end++;
    return new String(stream, payloadStart, end - payloadStart, StandardCharsets.UTF_8);
  }

  private static int packetEnd(byte[] stream, int off) {
    int len =
        (stream[off] & 0xff) | ((stream[off + 1] & 0xff) << 8) | ((stream[off + 2] & 0xff) << 16);
    return off + 4 + len;
  }
}
