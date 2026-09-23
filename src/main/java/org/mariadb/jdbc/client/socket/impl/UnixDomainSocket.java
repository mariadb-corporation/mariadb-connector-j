// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.client.socket.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

/** Unix domain socket, built on the JDK unix domain socket support of {@link SocketChannel}. */
public class UnixDomainSocket extends Socket {

  private static final boolean IS_WINDOWS =
      System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");

  private final AtomicBoolean closeLock = new AtomicBoolean();
  private final UnixDomainSocketAddress address;
  private final SocketChannel channel;
  private InputStream is;
  private OutputStream os;
  private boolean connected;

  /**
   * Constructor
   *
   * @param path unix socket path
   * @throws IOException if any error occurs
   */
  public UnixDomainSocket(String path) throws IOException {
    if (IS_WINDOWS) {
      throw new IOException("Unix domain sockets are not supported on Windows");
    }
    address = UnixDomainSocketAddress.of(path);
    channel = SocketChannel.open(StandardProtocolFamily.UNIX);
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  @Override
  public boolean isClosed() {
    return closeLock.get();
  }

  @Override
  public void close() throws IOException {
    if (!closeLock.getAndSet(true)) {
      connected = false;
      channel.close();
    }
  }

  /**
   * Connect to the unix domain socket. The timeout is ignored: connecting to a local socket
   * completes immediately, either succeeding or failing.
   *
   * @param endpoint ignored, the path given to the constructor is used
   * @param timeout ignored
   * @throws IOException if the connection fails
   */
  @Override
  public void connect(SocketAddress endpoint, int timeout) throws IOException {
    try {
      channel.connect(address);
    } catch (IOException ioe) {
      try {
        close();
      } catch (IOException e) {
        // eat
      }
      throw new IOException("unix domain socket connect() failed : " + ioe.getMessage(), ioe);
    }
    connected = true;
    is = new UnixSocketInputStream();
    os = new UnixSocketOutputStream();
  }

  @Override
  public InputStream getInputStream() {
    return is;
  }

  @Override
  public OutputStream getOutputStream() {
    return os;
  }

  @Override
  public void setTcpNoDelay(boolean b) {
    // do nothing
  }

  @Override
  public void setKeepAlive(boolean b) {
    // do nothing
  }

  @Override
  public void setSoLinger(boolean b, int i) {
    // do nothing
  }

  @Override
  public void setSoTimeout(int timeout) {
    // do nothing: blocking channel reads have no read timeout
  }

  @Override
  public void shutdownInput() {
    // do nothing
  }

  @Override
  public void shutdownOutput() {
    // do nothing
  }

  class UnixSocketInputStream extends InputStream {

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      return channel.read(ByteBuffer.wrap(bytes, off, len));
    }

    @Override
    public int read() throws IOException {
      byte[] bytes = new byte[1];
      int bytesRead = read(bytes, 0, 1);
      if (bytesRead <= 0) {
        return -1;
      }
      return bytes[0] & 0xff;
    }

    @Override
    public int read(byte[] bytes) throws IOException {
      return read(bytes, 0, bytes.length);
    }
  }

  class UnixSocketOutputStream extends OutputStream {

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
      ByteBuffer buffer = ByteBuffer.wrap(bytes, off, len);
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
    }

    @Override
    public void write(int value) throws IOException {
      write(new byte[] {(byte) value}, 0, 1);
    }
  }
}
