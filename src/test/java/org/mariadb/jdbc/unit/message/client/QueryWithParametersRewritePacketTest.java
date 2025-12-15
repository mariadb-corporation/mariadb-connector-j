// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.message.client;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mariadb.jdbc.client.socket.impl.PacketWriter;
import org.mariadb.jdbc.client.util.MutableByte;
import org.mariadb.jdbc.client.util.Parameter;
import org.mariadb.jdbc.client.util.Parameters;
import org.mariadb.jdbc.codec.NonNullParameter;
import org.mariadb.jdbc.message.client.QueryWithParametersRewritePacket;
import org.mariadb.jdbc.plugin.codec.IntCodec;
import org.mariadb.jdbc.util.ClientParser;
import org.mariadb.jdbc.util.ParameterList;

public class QueryWithParametersRewritePacketTest {

  static Stream<Arguments> rewriteTestData() {
    return Stream.of(
        Arguments.of(
            "INSERT INTO b VALUES (?)",
            Arrays.asList(
                new ParameterList(new Parameter[] {new NonNullParameter<>(IntCodec.INSTANCE, 1)}),
                new ParameterList(new Parameter[] {new NonNullParameter<>(IntCodec.INSTANCE, 2)})),
            "INSERT INTO b VALUES (1),(2)"),
        Arguments.of(
            "INSERT INTO b VALUES (?,?)",
            Arrays.asList(
                new ParameterList(
                    new Parameter[] {
                      new NonNullParameter<>(IntCodec.INSTANCE, 1),
                      new NonNullParameter<>(IntCodec.INSTANCE, 2)
                    }),
                new ParameterList(
                    new Parameter[] {
                      new NonNullParameter<>(IntCodec.INSTANCE, 3),
                      new NonNullParameter<>(IntCodec.INSTANCE, 4)
                    })),
            "INSERT INTO b VALUES (1,2),(3,4)"),
        Arguments.of(
            "INSERT INTO b VALUES (?,?) AS v ON DUPLICATE KEY UPDATE b.a=(v.a)",
            Arrays.asList(
                new ParameterList(
                    new Parameter[] {
                      new NonNullParameter<>(IntCodec.INSTANCE, 1),
                      new NonNullParameter<>(IntCodec.INSTANCE, 2)
                    }),
                new ParameterList(
                    new Parameter[] {
                      new NonNullParameter<>(IntCodec.INSTANCE, 3),
                      new NonNullParameter<>(IntCodec.INSTANCE, 4)
                    })),
            "INSERT INTO b VALUES (1,2),(3,4) AS v ON DUPLICATE KEY UPDATE b.a=(v.a)"),
        Arguments.of(
                "INSERT INTO b VALUES (?,?), (?,?) AS v ON DUPLICATE KEY UPDATE b.a=(v.a)",
                Arrays.asList(
                        new ParameterList(
                                new Parameter[] {
                                        new NonNullParameter<>(IntCodec.INSTANCE, 1),
                                        new NonNullParameter<>(IntCodec.INSTANCE, 2),
                                        new NonNullParameter<>(IntCodec.INSTANCE, 3),
                                        new NonNullParameter<>(IntCodec.INSTANCE, 4)
                                }),
                        new ParameterList(
                                new Parameter[] {
                                        new NonNullParameter<>(IntCodec.INSTANCE, 11),
                                        new NonNullParameter<>(IntCodec.INSTANCE, 12),
                                        new NonNullParameter<>(IntCodec.INSTANCE, 13),
                                        new NonNullParameter<>(IntCodec.INSTANCE, 14)
                                })),
                "INSERT INTO b VALUES (1,2), (3,4),(11,12), (13,14) AS v ON DUPLICATE KEY UPDATE b.a=(v.a)"),
        Arguments.of(
            "INSERT INTO b VALUES (?, ?) # comment",
            Arrays.asList(
                new ParameterList(
                    new Parameter[] {
                      new NonNullParameter<>(IntCodec.INSTANCE, 1),
                      new NonNullParameter<>(IntCodec.INSTANCE, 2)
                    }),
                new ParameterList(
                    new Parameter[] {
                      new NonNullParameter<>(IntCodec.INSTANCE, 3),
                      new NonNullParameter<>(IntCodec.INSTANCE, 4)
                    }),
                new ParameterList(
                    new Parameter[] {
                      new NonNullParameter<>(IntCodec.INSTANCE, 5),
                      new NonNullParameter<>(IntCodec.INSTANCE, 6)
                    })),
            "INSERT INTO b VALUES (1, 2),(3, 4),(5, 6) # comment"));
  }

  @ParameterizedTest()
  @MethodSource("rewriteTestData")
  public void testQueryWithParametersRewritePacket(
      String sql, List<Parameters> parameters, String expected) throws Exception {
    ClientParser parser = ClientParser.rewritableParts(sql, false);

    QueryWithParametersRewritePacket packet =
        new QueryWithParametersRewritePacket(null, parser, parameters);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PacketWriter writer =
        new PacketWriter(out, 0, 0x00ffffff, new MutableByte(), new MutableByte());

    packet.encode(writer, null);

    String sent = out.toString("UTF-8").substring(5);
    Assertions.assertEquals(sent, expected);
  }
}
