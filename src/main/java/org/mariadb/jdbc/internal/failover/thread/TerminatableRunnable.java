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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public abstract class TerminatableRunnable implements Runnable {
    private final AtomicInteger runState = new AtomicInteger(0); // -1 = removed, 0 = idle, 1 = active
    private final AtomicBoolean unschedule = new AtomicBoolean();
    private volatile ScheduledFuture<?> scheduledFuture = null;

    protected abstract void doRun();

    public TerminatableRunnable(ScheduledExecutorService scheduler,
                                long initialDelay,
                                long delay,
                                TimeUnit unit) {
        this.scheduledFuture = scheduler.scheduleWithFixedDelay(this, initialDelay, delay, unit);
    }

    @Override
    public final void run() {
        if (!runState.compareAndSet(0, 1)) {
            // task has somehow either started to run in parallel (should not be possible)
            // or more likely the task has now been set to terminate
            return;
        }
        try {
            doRun();
        } finally {
            runState.compareAndSet(1, 0);
        }
    }

    /**
     * Unschedule next launched, and wait for the current task to complete before closing it.
     */
    public void blockTillTerminated() {
        unscheduleTask();
        while (!runState.compareAndSet(0, -1)) {
            // wait and retry
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            if (Thread.currentThread().isInterrupted()) {
                runState.set(-1);
                return;
            }
        }
    }

    public boolean isUnschedule() {
        return unschedule.get();
    }

    /**
     * Unschedule task if active.
     */
    public void unscheduleTask() {
        if (unschedule.compareAndSet(false, true)) {
            scheduledFuture.cancel(false);
            scheduledFuture = null;
            return;
        }
    }

}

