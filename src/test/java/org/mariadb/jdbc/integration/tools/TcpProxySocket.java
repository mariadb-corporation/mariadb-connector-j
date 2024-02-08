// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.integration.tools;

import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

public class TcpProxySocket implements Runnable {
  private static final Logger logger = Loggers.getLogger(TcpProxySocket.class);

  private final String host;
  private final int remoteport;
  private final int localport;
  private boolean stop = false;
  private Socket client = null;
  private Socket server = null;
  private ServerSocket ss;
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

  /** Kill proxy. */
  public void kill() {
    stop = true;
    try {
      if (server != null) {
        server.close();
      }
    } catch (IOException e) {
      // eat Exception
    }
    try {
      if (client != null) {
        client.close();
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

    try {
      if (client != null) {
        // send an RST, not FIN
        client.setSoLinger(true, 0);
        client.close();
      }
    } catch (IOException e) {
      // eat Exception
    }
    try {
      if (server != null) {
        server.close();
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
        // in case for testing crash and reopen too quickly
        try {
          Thread.sleep(100);
        } catch (InterruptedException i) {
          // eat Exception
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
            out.println("Proxy server cannot connect to " + host + ":" + remoteport + ":\n" + e);
            out.flush();
            client.close();
            continue;
          }
          final InputStream fromServer = server.getInputStream();
          final OutputStream toServer = server.getOutputStream();
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
          try {
            if (server != null) {
              server.close();
            }
            if (client != null) {
              client.close();
            }
          } catch (IOException e) {
            // eat exception
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
