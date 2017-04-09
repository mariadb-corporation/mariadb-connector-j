/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.
Copyright (c) 2015-2016 MariaDB Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

package org.mariadb.jdbc.internal.io.socket;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

public class NamedPipeSocket extends Socket {
    String host;
    String name;

    RandomAccessFile file;
    InputStream is;
    OutputStream os;

    public NamedPipeSocket(String host, String name) {
        this.host = host;
        this.name = name;
    }

    @Override
    public void close() throws IOException {
        if (file != null) {
            file.close();
            file = null;
        }
    }

    @Override
    public void connect(SocketAddress endpoint) throws IOException {
        connect(endpoint, 0);
    }

    /**
     * Name pipe connection.
     *
     * @param endpoint  endPoint
     * @param timeout   timeout in milliseconds
     * @throws IOException exception
     */
    public void connect(SocketAddress endpoint, int timeout) throws IOException {
        String filename;
        if (host == null || host.equals("localhost")) {
            filename = "\\\\.\\pipe\\" + name;
        } else {
            filename = "\\\\" + host + "\\pipe\\" + name;
        }

        //use a default timeout of 100ms if no timeout set.
        int usedTimeout = timeout == 0 ? 100 : timeout;
        long initialNano = System.nanoTime();
        do {
            try {
                file = new RandomAccessFile(filename, "rw");
                break;
            } catch (FileNotFoundException fileNotFoundException) {
                try {
                    //using JNA if available
                    Class kernel32Class = Class.forName("com.sun.jna.platform.win32.Kernel32");
                    Field field = kernel32Class.getField("INSTANCE");
                    Object fieldInstance = field.get(kernel32Class);
                    Method waitNamedPipe = fieldInstance.getClass().getMethod("WaitNamedPipe");
                    waitNamedPipe.invoke(fieldInstance, filename, timeout);

                    //then retry connection
                    try {
                        file = new RandomAccessFile(filename, "rw");
                    } catch (FileNotFoundException secondException) {
                        throw secondException;
                    }

                } catch (Throwable cle) {
                    // in case JNA not on classpath, then wait 10ms before next try.
                    if (System.nanoTime() - initialNano > TimeUnit.MILLISECONDS.toNanos(usedTimeout)) {
                        if (timeout == 0) {
                            throw new FileNotFoundException(fileNotFoundException.getMessage()
                                    + "\nplease consider set connectTimeout option, so connection can retry having access to named pipe. "
                                    + "\n(Named pipe can throw ERROR_PIPE_BUSY error)");
                        }
                        throw fileNotFoundException;
                    }
                    try {
                        TimeUnit.MILLISECONDS.sleep(5);
                    } catch (InterruptedException interrupted) {
                        IOException ioException = new IOException("Interruption during connection to named pipe");
                        ioException.initCause(interrupted);
                        throw ioException;
                    }
                }
            }
        } while (true);

        is = new InputStream() {
            @Override
            public int read(byte[] bytes, int off, int len) throws IOException {
                return file.read(bytes, off, len);
            }

            @Override
            public int read() throws IOException {
                return file.read();
            }

            @Override
            public int read(byte[] bytes) throws IOException {
                return file.read(bytes);
            }
        };

        os = new OutputStream() {
            @Override
            public void write(byte[] bytes, int off, int len) throws IOException {
                file.write(bytes, off, len);
            }

            @Override
            public void write(int value) throws IOException {
                file.write(value);
            }

            @Override
            public void write(byte[] bytes) throws IOException {
                file.write(bytes);
            }
        };
    }

    public InputStream getInputStream() {
        return is;
    }

    public OutputStream getOutputStream() {
        return os;
    }

    public void setTcpNoDelay(boolean bool) {
        //do nothing
    }

    public void setKeepAlive(boolean bool) {
        //do nothing
    }

    public void setReceiveBufferSize(int size) {
        //do nothing
    }

    public void setSendBufferSize(int size) {
        //do nothing
    }

    public void setSoLinger(boolean bool, int value) {
        //do nothing
    }

    @Override
    public void setSoTimeout(int timeout) {
        //do nothing
    }

    public void shutdownInput() {
        //do nothing
    }

    public void shutdownOutput() {
        //do nothing
    }
}
