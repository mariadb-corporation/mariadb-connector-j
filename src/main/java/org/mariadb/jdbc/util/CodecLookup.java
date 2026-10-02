// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.util;

import java.util.concurrent.ConcurrentHashMap;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.plugin.Codec;

/**
 * Resolution of the codec decoding a column to a java type ({@code getObject(index, Class)}) or
 * encoding a java object ({@code setObject}), without looping on the whole codec list for every
 * value: resolved codecs are remembered in static tables.
 *
 * <p>A decoder is remembered by requested java type, column {@link DataType} and column binary
 * flag: this is all the driver codecs look at in {@code canDecode} (the binary flag being needed
 * to tell text from binary columns sharing a data type, i.e. TEXT / BLOB or VARCHAR / VARBINARY).
 *
 * <p>The tables are only used for the codec list shared by all connections (option {@code
 * cacheCodecs}, the default), and only if that list contains the driver codecs alone: a custom
 * codec can rely on any column attribute, so its selection cannot be remembered. Other lists
 * ({@code cacheCodecs=false}, reloaded for each connection) are scanned on every call, as before.
 *
 * <p>Only JDK classes (bootstrap or platform class loader) and classes of the driver class loader
 * are used as keys, so that the tables never keep an application class loader reachable.
 */
public final class CodecLookup {

  /** one slot per {@link DataType}, for non-binary then for binary columns */
  private static final int DECODER_SLOTS = DataType.values().length * 2;

  private static final String DRIVER_CODEC_PACKAGE = "org.mariadb.jdbc.plugin.codec";
  private static final ClassLoader PLATFORM_CLASS_LOADER = ClassLoader.getPlatformClassLoader();

  /** codec list the tables are valid for, null if none (not loaded yet, or having custom codecs) */
  private static volatile Codec<?>[] owner;

  /** requested java type → codec by {@link DataType} ordinal * 2 (+ 1 for a binary column) */
  private static final ConcurrentHashMap<Class<?>, Codec<?>[]> DECODERS = new ConcurrentHashMap<>();

  /** java type of the value → codec */
  private static final ConcurrentHashMap<Class<?>, Codec<?>> ENCODERS = new ConcurrentHashMap<>();

  private CodecLookup() {}

  private static boolean useTables(Codec<?>[] codecs, Class<?> key) {
    if (codecs != owner) return false;
    // JDK classes (bootstrap loader, or platform loader for java.sql) and driver classes
    ClassLoader loader = key.getClassLoader();
    return loader == null
        || loader == PLATFORM_CLASS_LOADER
        || loader == CodecLookup.class.getClassLoader();
  }

  /**
   * Declare the codec list shared by all connections of the JVM (option {@code cacheCodecs}, the
   * default): the tables are used for this list, if it contains the driver codecs alone.
   * Connections not using the shared list ({@code cacheCodecs=false}) scan their own list.
   *
   * @param sharedCodecs JVM-wide codec list
   */
  public static void register(Codec<?>[] sharedCodecs) {
    for (Codec<?> codec : sharedCodecs) {
      if (!DRIVER_CODEC_PACKAGE.equals(codec.getClass().getPackageName())) return;
    }
    owner = sharedCodecs;
  }

  /**
   * Find the codec decoding a column into the requested java type.
   *
   * @param codecs codec list of the connection
   * @param column column metadata
   * @param type requested java type
   * @param <T> requested java type
   * @return the codec, or null when no codec can decode this column to this type
   */
  @SuppressWarnings("unchecked")
  public static <T> Codec<T> decoder(Codec<?>[] codecs, ColumnDecoder column, Class<T> type) {
    boolean useTables = useTables(codecs, type);
    int typeIndex = column.getType().ordinal() * 2 + (column.isBinary() ? 1 : 0);
    if (useTables) {
      Codec<?>[] byDataType = DECODERS.get(type);
      if (byDataType != null) {
        Codec<?> known = byDataType[typeIndex];
        if (known != null) return (Codec<T>) known;
      }
    }
    for (Codec<?> codec : codecs) {
      if (codec.canDecode(column, type)) {
        if (useTables) {
          DECODERS.computeIfAbsent(type, k -> new Codec<?>[DECODER_SLOTS])[typeIndex] = codec;
        }
        return (Codec<T>) codec;
      }
    }
    return null;
  }

  /**
   * Find the codec encoding a java object.
   *
   * @param codecs codec list of the connection
   * @param value object to encode, not null
   * @return the codec, or null when no codec can encode this object
   */
  public static Codec<?> encoder(Codec<?>[] codecs, Object value) {
    Class<?> type = value.getClass();
    boolean useTables = useTables(codecs, type);
    if (useTables) {
      Codec<?> known = ENCODERS.get(type);
      if (known != null) return known;
    }
    for (Codec<?> codec : codecs) {
      if (codec.canEncode(value)) {
        if (useTables) ENCODERS.put(type, codec);
        return codec;
      }
    }
    return null;
  }
}
