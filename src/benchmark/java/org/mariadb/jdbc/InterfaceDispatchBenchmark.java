// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc;

import java.util.concurrent.TimeUnit;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.openjdk.jmh.annotations.*;

/**
 * Benchmark: interface dispatch (ReadableByteBuf) vs concrete dispatch (ReadableByteBuf).
 *
 * <p>Simulates the per-row decode hot path where ColumnDecoder methods receive ReadableByteBuf
 * (interface) but the only implementation is ReadableByteBuf.
 *
 * <p>Run with: mvn clean package -P bench -DskipTests java -Duser.country=US -Duser.language=en
 * -jar target/benchmarks.jar "InterfaceDispatchBenchmark"
 */
@State(Scope.Benchmark)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 3)
@Threads(value = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class InterfaceDispatchBenchmark {

  // 16-byte LE buffer: enough for readInt + readLong + readInt
  private byte[] data;

  @Setup(Level.Iteration)
  public void setup() {
    // Fill with non-zero bytes to make the result non-trivial
    data =
        new byte[] {
          1, 2, 3, 4, // readInt (4 bytes)
          5, 6, 7, 8, 9, 10, 11, 12, // readLong (8 bytes)
          13, 14, 15, 16, // readInt (4 bytes)
          // atoll data: "12345678" (8 bytes)
          '1', '2', '3', '4', '5', '6', '7', '8',
          // readShort (2 bytes)
          17, 18,
          // readByte (1 byte)
          19,
          // padding
          0, 0, 0, 0, 0
        };
  }

  // ============================================================
  // 1. Single readInt: interface vs concrete
  // ============================================================

  @Benchmark
  public int readInt_interface() {
    ReadableByteBuf buf = new ReadableByteBuf(data);
    return buf.readInt();
  }

  @Benchmark
  public int readInt_concrete() {
    ReadableByteBuf buf = new ReadableByteBuf(data);
    return buf.readInt();
  }

  // ============================================================
  // 2. Single readLong: interface vs concrete
  // ============================================================

  @Benchmark
  public long readLong_interface() {
    ReadableByteBuf buf = new ReadableByteBuf(data);
    return buf.readLong();
  }

  @Benchmark
  public long readLong_concrete() {
    ReadableByteBuf buf = new ReadableByteBuf(data);
    return buf.readLong();
  }

  // ============================================================
  // 3. atoll (8 digits): interface vs concrete
  // ============================================================

  @Benchmark
  public long atoll_interface() {
    ReadableByteBuf buf = new ReadableByteBuf(data);
    buf.pos(16); // skip to atoll data
    return buf.atoll(8);
  }

  @Benchmark
  public long atoll_concrete() {
    ReadableByteBuf buf = new ReadableByteBuf(data);
    buf.pos(16); // skip to atoll data
    return buf.atoll(8);
  }

  // ============================================================
  // 4. Simulated row decode: readInt + readLong + atoll(8)
  //    This simulates decoding 3 columns from one row
  // ============================================================

  @Benchmark
  public long decodeRow_interface() {
    ReadableByteBuf buf = new ReadableByteBuf(data);
    int col1 = buf.readInt();
    long col2 = buf.readLong();
    buf.skip(4); // skip 4 bytes
    long col3 = buf.atoll(8);
    return col1 + col2 + col3;
  }

  @Benchmark
  public long decodeRow_concrete() {
    ReadableByteBuf buf = new ReadableByteBuf(data);
    int col1 = buf.readInt();
    long col2 = buf.readLong();
    buf.skip(4); // skip 4 bytes
    long col3 = buf.atoll(8);
    return col1 + col2 + col3;
  }

  // ============================================================
  // 5. Multi-column decode: readInt x5 (common for int-heavy tables)
  // ============================================================

  @Benchmark
  public long fiveReadInt_interface() {
    ReadableByteBuf buf = new ReadableByteBuf(data);
    return (long) buf.readInt() + buf.readInt() + buf.readInt() + buf.readInt();
  }

  @Benchmark
  public long fiveReadInt_concrete() {
    ReadableByteBuf buf = new ReadableByteBuf(data);
    return (long) buf.readInt() + buf.readInt() + buf.readInt() + buf.readInt();
  }

  // ============================================================
  // 6. Mixed reads: readByte + readShort + readInt + readLong
  //    Simulates a row with varied column types
  // ============================================================

  @Benchmark
  public long mixedReads_interface() {
    ReadableByteBuf buf = new ReadableByteBuf(data);
    byte b = buf.readByte();
    short s = buf.readShort();
    int i = buf.readInt();
    long l = buf.readLong();
    return b + s + i + l;
  }

  @Benchmark
  public long mixedReads_concrete() {
    ReadableByteBuf buf = new ReadableByteBuf(data);
    byte b = buf.readByte();
    short s = buf.readShort();
    int i = buf.readInt();
    long l = buf.readLong();
    return b + s + i + l;
  }
}
