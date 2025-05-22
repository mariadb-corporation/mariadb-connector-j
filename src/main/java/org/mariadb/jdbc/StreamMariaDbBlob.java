// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc;

import java.io.*;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * MariaDB Blob implementation that uses InputStream to avoid loading the whole data in memory. This
 * implementation ensures that data can only be read once.
 */
public class StreamMariaDbBlob implements Blob {

  private final InputStream inputStream;
  private byte[] initialBuffer;
  private int initialBufferLength;
  private final int remainingLength;
  private boolean consumed = false;
  private static final int BUFFER_SIZE = 8192;

  /**
   * Creates a new StreamMariaDbBlob with the given input stream and initial buffer.
   *
   * @param inputStream the input stream containing the blob data
   * @param initialBuffer the initial buffer containing the first part of the blob data
   * @param initialBufferLength the length of valid data in the initial buffer
   * @param remainingLength the length of remaining data in the input stream
   */
  public StreamMariaDbBlob(
      byte[] initialBuffer, int initialBufferLength, InputStream inputStream, int remainingLength) {
    if (inputStream == null) {
      throw new IllegalArgumentException("inputStream cannot be null");
    }
    if (initialBuffer == null) {
      throw new IllegalArgumentException("initialBuffer cannot be null");
    }
    if (initialBufferLength < 0 || initialBufferLength > initialBuffer.length) {
      throw new IllegalArgumentException("initialBufferLength is invalid");
    }
    if (remainingLength < 0) {
      throw new IllegalArgumentException("remainingLength cannot be negative");
    }
    this.inputStream = inputStream;
    this.initialBuffer = initialBuffer;
    this.initialBufferLength = initialBufferLength;
    this.remainingLength = remainingLength;
  }

  @Override
  public long length() throws SQLException {
    return initialBufferLength + remainingLength;
  }

  public void load() throws SQLException {
    if (!consumed) {
      this.initialBuffer = getBytes(1, initialBufferLength + remainingLength);
      this.initialBufferLength = initialBufferLength + remainingLength;
    }
  }

  @Override
  public byte[] getBytes(long pos, int length) throws SQLException {
    if (consumed) {
      throw new SQLException("Blob data has already been consumed");
    }
    if (pos < 1) {
      throw new SQLException("Position must be greater than 0");
    }
    if (length < 0) {
      throw new SQLException("Length cannot be negative");
    }
    if (pos > length()) {
      throw new SQLException("Position is greater than blob length");
    }

    try {
      byte[] result = new byte[length];
      int bytesRead = 0;

      // If position is within initial buffer
      if (pos <= initialBufferLength) {
        int bufferPos = (int) (pos - 1);
        int bytesFromBuffer = Math.min(length, initialBufferLength - bufferPos);
        System.arraycopy(initialBuffer, bufferPos, result, 0, bytesFromBuffer);
        bytesRead = bytesFromBuffer;

        // If we need more bytes than what's in the buffer
        if (bytesFromBuffer < length) {
          int remainingBytes = length - bytesFromBuffer;
          int streamBytesRead = inputStream.read(result, bytesFromBuffer, remainingBytes);
          if (streamBytesRead > 0) {
            bytesRead += streamBytesRead;
          }
        }
      } else {
        // Skip to the correct position in the stream
        long skip = inputStream.skip(pos - initialBufferLength - 1);
        if (skip != pos - initialBufferLength - 1) {
          throw new SQLException("Failed to skip to requested position");
        }
        bytesRead = inputStream.read(result, 0, length);
      }

      if (bytesRead == -1) {
        return new byte[0];
      }
      if (bytesRead < length) {
        byte[] trimmed = new byte[bytesRead];
        System.arraycopy(result, 0, trimmed, 0, bytesRead);
        result = trimmed;
      }
      consumed = true;
      return result;
    } catch (IOException e) {
      throw new SQLException("Error reading blob data", e);
    }
  }

  @Override
  public InputStream getBinaryStream() throws SQLException {
    if (consumed) {
      throw new SQLException("Blob data has already been consumed");
    }
    consumed = true;
    return new SequenceInputStream(
        new ByteArrayInputStream(initialBuffer, 0, initialBufferLength), inputStream);
  }

  @Override
  public long position(byte[] pattern, long start) throws SQLException {
    if (consumed) {
      throw new SQLException("Blob data has already been consumed");
    }
    if (pattern == null || pattern.length == 0) {
      return 0;
    }
    if (start < 1) {
      throw new SQLException("Start position must be greater than 0");
    }
    if (start > length()) {
      throw new SQLException("Start position is greater than blob length");
    }

    try {
      // If start position is within initial buffer
      if (start <= initialBufferLength) {
        int bufferPos = (int) (start - 1);
        int patternIndex = 0;

        // Search in initial buffer
        for (int i = bufferPos; i < initialBufferLength; i++) {
          if (initialBuffer[i] == pattern[patternIndex]) {
            patternIndex++;
            if (patternIndex == pattern.length) {
              consumed = true;
              return i
                  - pattern.length
                  + 2; // +2 because position is 1-based and we need to include the first byte
            }
          } else {
            patternIndex = 0;
          }
        }

        // If pattern wasn't found in initial buffer, continue with stream
        if (patternIndex > 0) {
          // We have a partial match at the end of the buffer
          byte[] buffer = new byte[BUFFER_SIZE];
          int bytesRead;
          long currentPos = initialBufferLength + 1;

          while ((bytesRead = inputStream.read(buffer)) != -1) {
            for (int i = 0; i < bytesRead; i++) {
              if (buffer[i] == pattern[patternIndex]) {
                patternIndex++;
                if (patternIndex == pattern.length) {
                  consumed = true;
                  return currentPos - pattern.length + 1;
                }
              } else {
                patternIndex = 0;
              }
              currentPos++;
            }
          }
        }
      } else {
        // Skip to the start position in the stream
        long skip = inputStream.skip(start - initialBufferLength - 1);
        if (skip != start - initialBufferLength - 1) {
          throw new SQLException("Failed to skip to requested position");
        }

        // Search in stream
        byte[] buffer = new byte[BUFFER_SIZE];
        int bytesRead;
        long currentPos = start;
        int patternIndex = 0;

        while ((bytesRead = inputStream.read(buffer)) != -1) {
          for (int i = 0; i < bytesRead; i++) {
            if (buffer[i] == pattern[patternIndex]) {
              patternIndex++;
              if (patternIndex == pattern.length) {
                consumed = true;
                return currentPos - pattern.length + 1;
              }
            } else {
              patternIndex = 0;
            }
            currentPos++;
          }
        }
      }
      consumed = true;
      return -1;
    } catch (IOException e) {
      throw new SQLException("Error searching in blob data", e);
    }
  }

  @Override
  public long position(Blob pattern, long start) throws SQLException {
    if (consumed) {
      throw new SQLException("Blob data has already been consumed");
    }
    byte[] patternBytes = pattern.getBytes(1, (int) pattern.length());
    return position(patternBytes, start);
  }

  @Override
  public int setBytes(long pos, byte[] bytes) throws SQLException {
    throw new SQLException("StreamMariaDbBlob is read-only");
  }

  @Override
  public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
    throw new SQLException("StreamMariaDbBlob is read-only");
  }

  @Override
  public OutputStream setBinaryStream(long pos) throws SQLException {
    throw new SQLException("StreamMariaDbBlob is read-only");
  }

  @Override
  public void truncate(long len) throws SQLException {
    throw new SQLException("StreamMariaDbBlob is read-only");
  }

  @Override
  public void free() throws SQLException {
    try {
      inputStream.close();
    } catch (IOException e) {
      throw new SQLException("Error closing input stream", e);
    }
  }

  @Override
  public InputStream getBinaryStream(long pos, long length) throws SQLException {
    if (consumed) {
      throw new SQLException("Blob data has already been consumed");
    }
    if (pos < 1) {
      throw new SQLException("Position must be greater than 0");
    }
    if (length < 0) {
      throw new SQLException("Length cannot be negative");
    }
    if (pos > length()) {
      throw new SQLException("Position is greater than blob length");
    }
    if (pos + length - 1 > length()) {
      throw new SQLException("Position + length exceeds blob length");
    }

    try {
      consumed = true;

      // If position is within initial buffer
      if (pos <= initialBufferLength) {
        int bufferPos = (int) (pos - 1);
        int bytesFromBuffer = Math.min((int) length, initialBufferLength - bufferPos);

        if (bytesFromBuffer == length) {
          // All data is in the initial buffer
          return new ByteArrayInputStream(initialBuffer, bufferPos, bytesFromBuffer);
        } else {
          // Need to combine buffer and stream
          return new SequenceInputStream(
              new ByteArrayInputStream(initialBuffer, bufferPos, bytesFromBuffer),
              new LimitedInputStream(inputStream, length - bytesFromBuffer));
        }
      } else {
        // Skip to the correct position in the stream
        long skip = inputStream.skip(pos - initialBufferLength - 1);
        if (skip != pos - initialBufferLength - 1) {
          throw new SQLException("Failed to skip to requested position");
        }
        return new LimitedInputStream(inputStream, length);
      }
    } catch (IOException e) {
      throw new SQLException("Error creating binary stream", e);
    }
  }

  /** A wrapper InputStream that limits the number of bytes that can be read. */
  private static class LimitedInputStream extends InputStream {
    private final InputStream inputStream;
    private long remaining;

    public LimitedInputStream(InputStream inputStream, long length) {
      this.inputStream = inputStream;
      this.remaining = length;
    }

    @Override
    public int read() throws IOException {
      if (remaining <= 0) {
        return -1;
      }
      int b = inputStream.read();
      if (b != -1) {
        remaining--;
      }
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (remaining <= 0) {
        return -1;
      }
      int bytesToRead = (int) Math.min(len, remaining);
      int bytesRead = inputStream.read(b, off, bytesToRead);
      if (bytesRead != -1) {
        remaining -= bytesRead;
      }
      return bytesRead;
    }
  }
}
