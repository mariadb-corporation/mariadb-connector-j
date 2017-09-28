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

package org.mariadb.jdbc.failover;

import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;

import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

public class TcpProxySocket implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TcpProxy.class);

    private final String host;
    private final int remoteport;
    private int localport;
    private boolean stop = false;
    private Socket client = null;
    private Socket server = null;
    private ServerSocket ss;
    private int delay;

    /**
     * Creation of proxy.
     *
     * @param host       database host
     * @param remoteport database port
     * @throws IOException exception
     */
    public TcpProxySocket(String host, int remoteport) throws IOException {
        this.host = host;
        this.remoteport = remoteport;
        ss = new ServerSocket(0);
        this.localport = ss.getLocalPort();
    }

    public int getLocalPort() {
        return ss.getLocalPort();
    }

    public boolean isClosed() {
        return ss.isClosed();
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }

    /**
     * Kill proxy.
     */
    public void kill() {
        stop = true;
        try {
            if (server != null) {
                server.close();
            }
        } catch (IOException e) {
            //eat Exception
        }
        try {
            if (client != null) {
                client.close();
            }
        } catch (IOException e) {
            //eat Exception
        }
        try {
            ss.close();
        } catch (IOException e) {
            //eat Exception
        }
    }

    @Override
    public void run() {

        logger.trace("host proxy port " + this.localport + " for " + host + " started");
        stop = false;
        try {
            try {
                if (ss.isClosed()) {
                    ss = new ServerSocket(localport);
                }
            } catch (BindException b) {
                //in case for testing crash and reopen too quickly
                try {
                    Thread.sleep(100);
                } catch (InterruptedException i) {
                    //eat Exception
                }
                if (ss.isClosed()) {
                    ss = new ServerSocket(localport);
                }
            }
            final byte[] request = new byte[1024];
            byte[] reply = new byte[4096];
            while (!stop) {
                try {
                    client = ss.accept();
                    final InputStream fromClient = client.getInputStream();
                    final OutputStream toClient = client.getOutputStream();
                    try {
                        server = new Socket(host, remoteport);
                    } catch (IOException e) {
                        PrintWriter out = new PrintWriter(new OutputStreamWriter(toClient));
                        out.println("Proxy server cannot connect to " + host + ":"
                                + remoteport + ":\n" + e);
                        out.flush();
                        client.close();
                        continue;
                    }
                    final InputStream fromServer = server.getInputStream();
                    final OutputStream toServer = server.getOutputStream();
                    new Thread() {
                        public void run() {
                            int bytesRead;
                            try {
                                while ((bytesRead = fromClient.read(request)) != -1) {
                                    if (delay > 0) {
                                        try {
                                            Thread.sleep(delay);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                    toServer.write(request, 0, bytesRead);
                                    toServer.flush();
                                }
                            } catch (IOException e) {
                                //eat exception
                            }
                            try {
                                toServer.close();
                            } catch (IOException e) {
                                //eat exception
                            }
                        }
                    }.start();
                    int bytesRead;
                    try {
                        while ((bytesRead = fromServer.read(reply)) != -1) {
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            toClient.write(reply, 0, bytesRead);
                            toClient.flush();
                        }
                    } catch (IOException e) {
                        //eat exception
                    }
                    toClient.close();
                } catch (IOException e) {
                    //System.err.println("ERROR socket : "+e);
                } finally {
                    try {
                        if (server != null) {
                            server.close();
                        }
                        if (client != null) {
                            client.close();
                        }
                    } catch (IOException e) {
                        //eat exception
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getLocalport() {
        return localport;
    }
}
