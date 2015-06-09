package org.mariadb.jdbc.multihost;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class TcpProxy {
    protected static Logger log = Logger.getLogger("org.maria.jdbc");

    String host;
    int remoteport;
    int localport;
    TcpProxySocket socket;

    public TcpProxy(String host, int remoteport, int localport) {
        this.host = host;
        this.remoteport = remoteport;
        this.localport = localport;
        socket = new TcpProxySocket(host, remoteport, localport);
        Executors.newSingleThreadScheduledExecutor().schedule(socket, 0, TimeUnit.MILLISECONDS);
    }

    public void restart(long sleepTime) {
        socket.kill();
        Executors.newSingleThreadScheduledExecutor().schedule(socket, sleepTime, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        socket.kill();
    }

}
