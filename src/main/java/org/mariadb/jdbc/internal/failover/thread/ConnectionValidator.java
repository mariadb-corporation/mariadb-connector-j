/*
MariaDB Client for Java

Copyright (c) 2015 MariaDB.

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

package org.mariadb.jdbc.internal.failover.thread;

import org.mariadb.jdbc.internal.failover.Listener;
import org.mariadb.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ConnectionValidator  {
    private static final ScheduledExecutorService fixedSizedScheduler = SchedulerServiceProviderHolder.getFixedSizeScheduler(1);
    private static final int MINIMUM_CHECK_DELAY_MILLIS = 100;

    private final ConcurrentLinkedQueue<Listener> queue = new ConcurrentLinkedQueue<>();
    private final AtomicLong currentScheduledFrequency = new AtomicLong(-1);
    private final ListenerChecker checker = new ListenerChecker();

    /**
     * Add listener to validation list.
     * @param listener listener
     * @param listenerCheckMillis schedule time
     */
    public void addListener(Listener listener, long listenerCheckMillis) {
        queue.add(listener);
        
        while (true) {
            long casFrequency = currentScheduledFrequency.get();
            if (casFrequency == listenerCheckMillis || (casFrequency != -1 && casFrequency <= MINIMUM_CHECK_DELAY_MILLIS)) {
                // common path...only one listener check frequency configured
                break;
            } else if (casFrequency == -1) {
                if (currentScheduledFrequency.compareAndSet(-1, listenerCheckMillis)) {
                    fixedSizedScheduler.schedule(checker, listenerCheckMillis, TimeUnit.MILLISECONDS);
                    break;
                }
            } else {
                BigInteger bi1 = BigInteger.valueOf(casFrequency);
                BigInteger bi2 = BigInteger.valueOf(listenerCheckMillis);
                int gcd = Math.max(MINIMUM_CHECK_DELAY_MILLIS, bi1.gcd(bi2).intValue());
                if (gcd < casFrequency) {
                    if (currentScheduledFrequency.compareAndSet(casFrequency, gcd)) {
                        fixedSizedScheduler.schedule(checker, gcd, TimeUnit.MILLISECONDS);
                        break;
                    }
                } else {
                    // already running at gcd frequency
                    break;
                }
            }
        }
    }

    /**
     * Remove listener to validation list.
     * @param listener listener
     */
    public void removeListener(Listener listener) {
        queue.remove(listener);
        
        long casFrequency;
        while ((casFrequency = currentScheduledFrequency.get()) > 0 
                && queue.isEmpty()) {   // must check queue count after casFrequency
            if (currentScheduledFrequency.compareAndSet(casFrequency, -1)) {
                break;
            }
        }
    }

    private class ListenerChecker implements Runnable {
        @Override
        public void run() {
            try {
                doRun();
            } finally {
                long delay = currentScheduledFrequency.get();
                if (delay > 0) {
                    fixedSizedScheduler.schedule(this, delay, TimeUnit.MILLISECONDS);
                }
            }
        }
        
        private void doRun() {
            Listener listener;
            Iterator<Listener> tmpQueue = queue.iterator();
            long now = -1;
            while (tmpQueue.hasNext()) {
                listener = tmpQueue.next();
                if (!listener.isExplicitClosed()) {
                    long durationNanos = (now == -1 ? now = System.nanoTime() : now) - listener.getLastQueryNanos();
                    long durationSeconds = TimeUnit.NANOSECONDS.toSeconds(durationNanos);
                    if (durationSeconds >= listener.getUrlParser().getOptions().validConnectionTimeout
                            && !listener.isMasterHostFail()) {
                        boolean masterFail = false;
                        if (listener.isMasterConnected()) {
                            listener.checkMasterStatus(null);
                        } else {
                            masterFail = true;
                        }

                        if (masterFail && listener.setMasterHostFail()) {
                            try {
                                listener.primaryFail(null, null);
                            } catch (Throwable t) {
                                //do nothing
                            }
                        }
                    }
                }
            }
        }
    }
}
