package org.mariadb.jdbc.internal.util.string;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

public class StringUtils {
  private StringUtils() {}

  public static String newString(byte[] bytes, int offset, int length, Charset charset) {
    try {
      return new String(bytes, offset, length, charset.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(charset.name() + ": " + e);
    }
  }

  public static byte[] getBytes(String string, Charset charset) {
    try {
      return string.getBytes(charset.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(charset.name() + ": " + e);
    }
  }
}
