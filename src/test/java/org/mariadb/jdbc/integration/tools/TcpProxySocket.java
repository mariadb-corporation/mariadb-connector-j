// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.integration.tools;

import java.io.*;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

public class TcpProxySocket implements Runnable {
  private static final Logger logger = Loggers.getLogger(TcpProxySocket.class);

  private final String host;
  private final int remoteport;
  private final int localport;
  private volatile boolean stop = false;
  private volatile Socket client = null;
  private volatile Socket server = null;
  private volatile ServerSocket ss;
  private volatile Thread relayThread;
  private int delay;

  /**
   * Creation of proxy.
   *
   * @param host database host
   * @param remoteport database port
   * @throws IOException exception
   */
  public TcpProxySocket(String host, int remoteport) throws IOException {
    this.host = host;
    this.remoteport = remoteport;
    ss = newServerSocket(0);
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

  /** Kill proxy. */
  public void kill(boolean rst) {
    stop = true;
    Socket s = server;
    try {
      if (s != null) {
        if (rst) s.setSoLinger(true, 0);
        s.close();
      }
    } catch (IOException e) {
      // eat Exception
    }
    Socket c = client;
    try {
      if (c != null) {
        if (rst) c.setSoLinger(true, 0);
        c.close();
      }
    } catch (IOException e) {
      // eat Exception
    }
    try {
      ss.close();
    } catch (IOException e) {
      // eat Exception
    }
  }

  public void sendRst() {

    Socket c = client;
    try {
      if (c != null) {
        // send an RST, not FIN
        c.setSoLinger(true, 0);
        c.close();
      }
    } catch (IOException e) {
      // eat Exception
    }
    Socket s = server;
    try {
      if (s != null) {
        s.close();
      }
    } catch (IOException e) {
      // eat Exception
    }

    try {
      ss.close();
    } catch (IOException e) {
      // eat Exception
    }
  }

  /**
   * Wait for the current relay thread (if any) to terminate. Used on restart so that a relay thread
   * delayed past a restart cannot interfere with the freshly started one.
   *
   * @param millis maximum time to wait
   */
  public void awaitStop(long millis) {
    Thread t = relayThread;
    if (t != null && t != Thread.currentThread()) {
      try {
        t.join(millis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void run() {

    logger.trace("host proxy port " + this.localport + " for " + host + " started");
    relayThread = Thread.currentThread();
    stop = false;
    try {
      try {
        if (ss.isClosed()) {
          ss = newServerSocket(localport);
        }
      } catch (BindException b) {
        // in case for testing crash and reopen too quickly
        try {
          Thread.sleep(100);
        } catch (InterruptedException i) {
          // eat Exception
        }
        if (ss.isClosed()) {
          ss = newServerSocket(localport);
        }
      }
      while (!stop) {
        // Per-connection sockets are kept in local variables so that a stale relay thread (e.g.
        // one delayed past a restart under load) only ever closes its own connection in the
        // finally block, never a newer connection accepted by a subsequent run(). The instance
        // fields are updated only so that kill()/sendRst() can interrupt the active relay.
        Socket localClient = null;
        Socket localServer = null;
        final byte[] request = new byte[1024];
        final byte[] reply = new byte[4096];
        try {
          localClient = ss.accept();
          client = localClient;
          final InputStream fromClient = localClient.getInputStream();
          final OutputStream toClient = localClient.getOutputStream();
          try {
            localServer = new Socket(host, remoteport);
            server = localServer;
          } catch (IOException e) {
            PrintWriter out = new PrintWriter(new OutputStreamWriter(toClient));
            out.println("Proxy server cannot connect to " + host + ":" + remoteport + ":\n" + e);
            out.flush();
            localClient.close();
            continue;
          }
          final InputStream fromServer = localServer.getInputStream();
          final OutputStream toServer = localServer.getOutputStream();
          new Thread(
                  () -> {
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
                      // eat exception
                    }
                    try {
                      toServer.close();
                    } catch (IOException e) {
                      // eat exception
                    }
                  })
              .start();
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
            // eat exception
          }
          toClient.close();
        } catch (IOException e) {
          // System.err.println("ERROR socket : "+e);
        } finally {
          closeQuietly(localServer);
          closeQuietly(localClient);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private ServerSocket newServerSocket(int port) throws IOException {
    ServerSocket s = new ServerSocket();
    // allow rebinding the same port immediately after a restart, avoiding BindException windows
    s.setReuseAddress(true);
    s.bind(new InetSocketAddress(port));
    return s;
  }

  private static void closeQuietly(Socket s) {
    if (s != null) {
      try {
        s.close();
      } catch (IOException e) {
        // eat exception
      }
    }
  }

  public int getLocalport() {
    return localport;
  }
}
