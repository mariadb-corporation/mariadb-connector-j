/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.io.socket;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.platform.win32.BaseTSD.SIZE_T;
import com.sun.jna.platform.win32.WinNT.HANDLE;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;
import com.sun.jna.win32.StdCallLibrary;
import com.sun.jna.win32.W32APIFunctionMapper;
import com.sun.jna.win32.W32APITypeMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SharedMemorySocket extends Socket {

  //SDDL string for mutex security flags (Everyone group has SYNCHRONIZE right)
  private static final String EVERYONE_SYNCHRONIZE_SDDL = "D:(A;;0x100000;;;WD)";
  private static final Map<String, Object> WIN32API_OPTIONS = new HashMap<String, Object>();
  // Size of memory mapped region
  private static final int BUFFERLEN = 16004;

  static {
    WIN32API_OPTIONS.put(Library.OPTION_FUNCTION_MAPPER, W32APIFunctionMapper.UNICODE);
    WIN32API_OPTIONS.put(Library.OPTION_TYPE_MAPPER, W32APITypeMapper.UNICODE);
  }

  private InputStream is;
  private OutputStream os;
  private String memoryName;
  private HANDLE serverRead;
  private HANDLE serverWrote;
  private HANDLE clientRead;
  private HANDLE clientWrote;
  private HANDLE connectionClosed;
  private Pointer view;
  private int bytesLeft;
  private int position;
  private int timeout = Kernel32.INFINITE;

  /**
   * Create ShareMemorySocket.
   *
   * @param name name
   * @throws IOException exception
   */
  public SharedMemorySocket(String name) throws IOException {
    if (!Platform.isWindows()) {
      throw new IOException("shared memory connections are only supported on Windows");
    }
    memoryName = name;
  }

  private static HANDLE openEvent(String name) {
    return Kernel32.INSTANCE.OpenEvent(Kernel32.EVENT_MODIFY_STATE | Kernel32.SYNCHRONIZE, false, name);
  }

  /**
   * Map memory
   *
   * @param mapName map name
   * @param mode    mode
   * @param size    size
   * @return Pointer
   */
  private static Pointer mapMemory(String mapName, int mode, int size) {
    HANDLE mapping = Kernel32.INSTANCE.OpenFileMapping(mode, false, mapName);
    Pointer v = Kernel32.INSTANCE.MapViewOfFile(mapping, mode, 0, 0, new SIZE_T(size));
    Kernel32.INSTANCE.CloseHandle(mapping);
    return v;
  }

  @Override
  public void connect(SocketAddress endpoint) throws IOException {
    connect(endpoint, 0);
  }

  /*
  Create a mutex to synchronize login. Without mutex, different connections that are created at about the same
  time, could get the same connection number. Note, that this mutex, or any synchronization does not exist in
  in either C or .NET connectors (i.e they are racy)
  */
  private HANDLE lockMutex() throws IOException {
    PointerByReference securityDescriptor = new PointerByReference();
    Advapi32.INSTANCE.ConvertStringSecurityDescriptorToSecurityDescriptor(EVERYONE_SYNCHRONIZE_SDDL, 1, securityDescriptor, null);
    Advapi32.SECURITY_ATTRIBUTES sa = new Advapi32.SECURITY_ATTRIBUTES();
    sa.nLength = sa.size();
    sa.lpSecurityDescriptor = securityDescriptor.getValue();
    sa.bInheritHandle = false;
    HANDLE mutex = Kernel32.INSTANCE.CreateMutex(sa, false, memoryName + "_CONNECT_MUTEX");
    Kernel32.INSTANCE.LocalFree(securityDescriptor.getValue());
    if (Kernel32.INSTANCE.WaitForSingleObject(mutex, timeout) == -1) {
      Kernel32.INSTANCE.CloseHandle(mutex);
      throw new IOException("wait failed (timeout, last error =  " + Kernel32.INSTANCE.GetLastError());
    }
    return mutex;
  }

  private int getConnectNumber() throws IOException {
    HANDLE connectRequest;
    try {
      connectRequest = openEvent(memoryName + "_CONNECT_REQUEST");
    } catch (LastErrorException lee) {
      try {
        connectRequest = openEvent("Global\\" + memoryName + "_CONNECT_REQUEST");
        memoryName = "Global\\" + memoryName;
      } catch (LastErrorException lee2) {
        throw new IOException("getConnectNumber() fails : " + lee2.getMessage() + " " + memoryName);
      }
    }

    HANDLE connectAnswer = openEvent(memoryName + "_CONNECT_ANSWER");


    HANDLE mutex = lockMutex();
    Pointer connectData = null;
    try {
      Kernel32.INSTANCE.SetEvent(connectRequest);
      connectData = mapMemory(memoryName + "_CONNECT_DATA", Kernel32.FILE_MAP_READ, 4);
      int ret = Kernel32.INSTANCE.WaitForSingleObject(connectAnswer, timeout);
      if (ret != 0) {
        throw new IOException("WaitForSingleObject returned " + ret + ", last error " + Kernel32.INSTANCE.GetLastError());
      }
      return connectData.getInt(0);
    } finally {
      Kernel32.INSTANCE.ReleaseMutex(mutex);
      Kernel32.INSTANCE.CloseHandle(mutex);
      if (connectData != null) {
        Kernel32.INSTANCE.UnmapViewOfFile(connectData);
      }
      Kernel32.INSTANCE.CloseHandle(connectRequest);
      Kernel32.INSTANCE.CloseHandle(connectAnswer);
    }
  }

  public void connect(SocketAddress endpoint, int timeout) throws IOException {
    try {
      is = new SharedMemoryInputStream();
      os = new SharedMemoryOutputStream();
      String prefix = memoryName + "_" + getConnectNumber();
      clientRead = openEvent(prefix + "_CLIENT_READ");
      serverRead = openEvent(prefix + "_SERVER_READ");
      serverWrote = openEvent(prefix + "_SERVER_WROTE");
      clientWrote = openEvent(prefix + "_CLIENT_WROTE");
      connectionClosed = openEvent(prefix + "_CONNECTION_CLOSED");
      view = mapMemory(prefix + "_DATA", Kernel32.FILE_MAP_WRITE, BUFFERLEN);
      Kernel32.INSTANCE.SetEvent(serverRead);
    } catch (LastErrorException lee) {
      throw new IOException(lee.getMessage(), lee.getCause());
    }
  }

  public InputStream getInputStream() {
    return is;
  }

  public OutputStream getOutputStream() {
    return os;
  }

  public void setTcpNoDelay(boolean b) {
    //do nothing
  }

  public void setKeepAlive(boolean b) {
    //do nothing
  }

  public void setReceiveBufferSize(int size) {
    //do nothing
  }

  public void setSendBufferSize(int size) {
    //do nothing
  }

  public void setSoLinger(boolean b, int i) {
    //do nothing
  }

  public void setSoTimeout(int t) {
    if (t == 0) {
      timeout = Kernel32.INFINITE;
    } else {
      timeout = t;
    }
  }

  public void shutdownInput() {
    //do nothing
  }

  public void shutdownOutput() {
    //do nothing
  }

  @Override
  public void close() {

    if (connectionClosed != null && Kernel32.INSTANCE.WaitForSingleObject(connectionClosed, 0) != 0) {
      /* Set close event if not yet set */
      Kernel32.INSTANCE.SetEvent(connectionClosed);
    }
    HANDLE[] handles = {serverRead, serverWrote, clientRead, clientWrote, connectionClosed};
    for (HANDLE h : handles) {
      if (h != null) {
        Kernel32.INSTANCE.CloseHandle(h);
      }
    }
    if (view != null) {
      Kernel32.INSTANCE.UnmapViewOfFile(view);
    }
    serverRead = null;
    serverWrote = null;
    clientRead = null;
    clientWrote = null;
    connectionClosed = null;
    view = null;
  }

  public interface Kernel32 extends StdCallLibrary {
    Kernel32 INSTANCE = (Kernel32) Native.loadLibrary("Kernel32", Kernel32.class, WIN32API_OPTIONS);
    int FILE_MAP_WRITE = 0x0002;
    int FILE_MAP_READ = 0x0004;
    int EVENT_MODIFY_STATE = 0x0002;
    int SYNCHRONIZE = 0x00100000;
    int INFINITE = -1;

    HANDLE OpenEvent(int dwDesiredAccess, boolean bInheritHandle, String name) throws LastErrorException;

    HANDLE OpenFileMapping(int dwDesiredAccess, boolean bInheritHandle, String name) throws LastErrorException;

    Pointer MapViewOfFile(HANDLE hFileMappingObject, int dwDesiredAccess, int dwFileOffsetHigh, int dwFileOffsetLow,
                          SIZE_T dwNumberOfBytesToMap) throws LastErrorException;

    boolean UnmapViewOfFile(Pointer view) throws LastErrorException;

    boolean SetEvent(HANDLE handle) throws LastErrorException;

    boolean CloseHandle(HANDLE handle) throws LastErrorException;

    int WaitForSingleObject(HANDLE handle, int timeout) throws LastErrorException;

    int WaitForMultipleObjects(int count, HANDLE[] handles, boolean waitAll, int millis) throws LastErrorException;

    int GetLastError() throws LastErrorException;

    HANDLE CreateMutex(Advapi32.SECURITY_ATTRIBUTES sa, boolean initialOwner, String name);

    boolean ReleaseMutex(HANDLE hMutex);

    Pointer LocalFree(Pointer p);


  }

  public interface Advapi32 extends StdCallLibrary {
    Advapi32 INSTANCE = (Advapi32) Native.loadLibrary("advapi32", Advapi32.class, WIN32API_OPTIONS);

    boolean ConvertStringSecurityDescriptorToSecurityDescriptor(String sddl, int sddlVersion, PointerByReference psd,
                                                                IntByReference length);

    class SECURITY_ATTRIBUTES extends Structure {
      public int nLength;
      public Pointer lpSecurityDescriptor;
      public boolean bInheritHandle;

      protected java.util.List<String> getFieldOrder() {
        return Arrays.asList("nLength", "lpSecurityDescriptor", "bInheritHandle");
      }
    }

  }

  class SharedMemoryInputStream extends InputStream {
    @Override
    public int read(byte[] bytes, int off, int count) throws IOException {
      HANDLE[] handles = {serverWrote, connectionClosed};
      if (bytesLeft == 0) {
        int index = Kernel32.INSTANCE.WaitForMultipleObjects(2, handles, false, timeout);
        if (index == -1) {
          throw new IOException("wait failed, timeout");
        } else if (index == 1) {
          // Connection closed
          throw new IOException("Server closed connection");
        } else if (index != 0) {
          throw new IOException("Unexpected return result from WaitForMultipleObjects : " + index);
        }
        bytesLeft = view.getInt(0);
        position = 4;
      }
      int len = Math.min(count, bytesLeft);
      view.read(position, bytes, off, len);
      position += len;
      bytesLeft -= len;
      if (bytesLeft == 0) {
        Kernel32.INSTANCE.SetEvent(clientRead);
      }
      return len;
    }

    @Override
    public int read() throws IOException {
      byte[] bit = new byte[1];
      int bytesRead = read(bit);
      if (bytesRead == 0) {
        return -1;
      }
      return bit[0] & 0xff;
    }

    @Override
    public int read(byte[] bytes) throws IOException {
      return read(bytes, 0, bytes.length);
    }
  }

  class SharedMemoryOutputStream extends OutputStream {
    @Override
    public void write(byte[] bytes, int off, int count) throws IOException {
      int bytesToWrite = count;
      int buffPos = off;
      HANDLE[] handles = {serverRead, connectionClosed};
      while (bytesToWrite > 0) {
        int index = Kernel32.INSTANCE.WaitForMultipleObjects(2, handles, false, timeout);
        if (index == -1) {
          // wait failed,  probably timeout
          throw new IOException("WaitForMultipleObjects() failed, timeout");
        } else if (index == 1) {
          // Connection closed
          throw new IOException("Server closed connection");
        } else if (index != 0) {
          throw new IOException("Unexpected return result from WaitForMultipleObjects : " + index);
        }

        int chunk = Math.min(bytesToWrite, BUFFERLEN);
        view.setInt(0, chunk);
        view.write(4, bytes, buffPos, chunk);
        buffPos += chunk;
        bytesToWrite -= chunk;
        if (!Kernel32.INSTANCE.SetEvent(clientWrote)) {
          throw new IOException("SetEvent failed");
        }
      }
    }

    @Override
    public void write(int value) throws IOException {
      write(new byte[]{(byte) value});
    }

    @Override
    public void write(byte[] bytes) throws IOException {
      write(bytes, 0, bytes.length);
    }
  }
}
