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
    private static final RawPacket IOEXCEPTION_PILL = new RawPacket();
    private final BlockingQueue<RawPacket> packet = new LinkedBlockingQueue<RawPacket>();
    private final InputStream inputStream;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private volatile boolean shutDown=false;

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
                // ok got an ioexception reading that packet, lets put the IOEXCEPTION pill on the queue
                packet.add(IOEXCEPTION_PILL);
            }
        }
        try {
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();  // we are closing down, ignore any exceptions 
        }
    }

    public RawPacket getRawPacket() throws IOException {
        try {
            RawPacket rawPacket = packet.take();
            if(rawPacket == IOEXCEPTION_PILL) throw new IOException();
            return rawPacket;
        } catch (InterruptedException e) {
            throw new RuntimeException("Got interrupted while waiting for a packet",e); //Todo: fix
        }
    }
    public void awaitTermination() {
        try {
            executorService.awaitTermination(1,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new IllegalStateException("executorService shutdown problem",e);
        }
    }
    public void close() {
        this.shutDown=true;
        executorService.shutdownNow();
    }
}