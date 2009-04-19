package org.drizzle.jdbc.internal.drizzle.packet;

import org.drizzle.jdbc.internal.drizzle.packet.RawPacket;
import org.drizzle.jdbc.internal.common.PacketFetcher;
import org.drizzle.jdbc.internal.drizzle.packet.ReadAheadInputStream;

import java.util.concurrent.*;
import java.io.InputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 31, 2009
 * Time: 2:01:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class AsyncPacketFetcher implements Runnable, PacketFetcher {
    private final BlockingQueue<RawPacket> packet = new LinkedBlockingQueue<RawPacket>();
    private final InputStream inputStream;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private volatile boolean shutDown=false;
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    public AsyncPacketFetcher(InputStream inputStream) {
        this.inputStream = new ReadAheadInputStream(inputStream);
        executorService.submit(this);
    }

    public void run() {
        while(!shutDown) {
            try {
                RawPacket rawPacket = new RawPacket(inputStream);
                packet.add(rawPacket);
            } catch (IOException e) {
                //TODO: how do i handle ioexceptions when async? shutdown?
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        try {
            executorService.shutdown();
            inputStream.close();
            shutdownLatch.countDown();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public RawPacket getRawPacket() {
        try {
            return packet.take();
        } catch (InterruptedException e) {
            throw new RuntimeException("doh",e); //Todo: fix
        }
    }

    public void close() {
        this.shutDown=true;
/*        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }*/
    }
}
