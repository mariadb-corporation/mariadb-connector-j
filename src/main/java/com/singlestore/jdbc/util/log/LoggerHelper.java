// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.util.log;

public final class LoggerHelper {

  private static final char[] hexArray = "0123456789ABCDEF".toCharArray();

  /**
   * Write bytes/hexadecimal value of a byte array to a StringBuilder.
   *
   * <p>String output example :
   *
   * <pre>{@code
   * +--------------------------------------------------+
   * |  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |
   * +--------------------------------------------------+------------------+
   * | 5F 00 00 00 03 73 65 74  20 61 75 74 6F 63 6F 6D | _....set autocom |
   * | 6D 69 74 3D 31 2C 20 73  65 73 73 69 6F 6E 5F 74 | mit=1, session_t |
   * | 72 61 63 6B 5F 73 63 68  65 6D 61 3D 31 2C 20 73 | rack_schema=1, s |
   * | 71 6C 5F 6D 6F 64 65 20  3D 20 63 6F 6E 63 61 74 | ql_mode = concat |
   * | 28 40 40 73 71 6C 5F 6D  6F 64 65 2C 27 2C 53 54 | (@@sql_mode,',ST |
   * | 52 49 43 54 5F 54 52 41  4E 53 5F 54 41 42 4C 45 | RICT_TRANS_TABLE |
   * | 53 27 29                                         | S')              |
   * +--------------------------------------------------+------------------+
   * }</pre>
   *
   * @param bytes byte array
   * @param offset offset
   * @param dataLength byte length to write
   * @return formated hexa
   */
  public static String hex(byte[] bytes, int offset, int dataLength) {
    return hex(bytes, offset, dataLength, Integer.MAX_VALUE);
  }

  public static String hex(byte[] bytes, int offset, int dataLength, int trunkLength) {

    if (bytes == null || bytes.length == 0) {
      return "";
    }

    char[] hexaValue = new char[16];
    hexaValue[8] = ' ';

    int pos = offset;
    int posHexa = 0;
    int logLength = Math.min(dataLength, trunkLength);
    StringBuilder sb = new StringBuilder(logLength * 3);
    sb.append(
        "+--------------------------------------------------+\n"
            + "|  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |\n"
            + "+--------------------------------------------------+------------------+\n| ");

    while (pos < logLength + offset) {
      int byteValue = bytes[pos] & 0xFF;
      sb.append(hexArray[byteValue >>> 4]).append(hexArray[byteValue & 0x0F]).append(" ");

      hexaValue[posHexa++] = (byteValue > 31 && byteValue < 127) ? (char) byteValue : '.';

      if (posHexa == 8) {
        sb.append(" ");
      }
      if (posHexa == 16) {
        sb.append("| ").append(hexaValue).append(" |\n");
        if (pos + 1 != logLength + offset) sb.append("| ");
        posHexa = 0;
      }
      pos++;
    }

    int remaining = posHexa;
    if (remaining > 0) {
      if (remaining < 8) {
        for (; remaining < 8; remaining++) {
          sb.append("   ");
        }
        sb.append(" ");
      }

      for (; remaining < 16; remaining++) {
        sb.append("   ");
      }

      for (; posHexa < 16; posHexa++) {
        hexaValue[posHexa] = ' ';
      }

      sb.append("| ").append(hexaValue).append(" |\n");
    }
    if (dataLength > trunkLength) {
      sb.append("+-------------------truncated----------------------+------------------+\n");
    } else {
      sb.append("+--------------------------------------------------+------------------+\n");
    }
    return sb.toString();
  }

  public static String hex(
      byte[] header, byte[] bytes, int offset, int dataLength, int trunkLength) {
    byte[] complete = new byte[dataLength + header.length];
    System.arraycopy(header, 0, complete, 0, header.length);
    System.arraycopy(bytes, offset, complete, header.length, dataLength);
    return hex(complete, 0, dataLength + header.length, trunkLength);
  }
}
