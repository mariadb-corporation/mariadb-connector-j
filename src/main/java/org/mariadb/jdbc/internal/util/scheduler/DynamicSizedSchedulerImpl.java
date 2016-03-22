/*
MariaDB Client for Java

Copyright (c) 2015 Monty Program Ab.

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

package org.mariadb.jdbc.internal.util.scheduler;

import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DynamicSizedSchedulerImpl extends ScheduledThreadPoolExecutor implements DynamicSizedSchedulerInterface {
    /**
     * Initialize a scheduler with dynamic pool size.
     * @param corePoolSize initial Core pool size
     */
    public DynamicSizedSchedulerImpl(int corePoolSize) {
        super(corePoolSize, new MariaDbThreadFactory());
    }

    @Override
    public void setPoolSize(int newSize) {
        synchronized (this) {
            super.setCorePoolSize(newSize);
        }
    }

    @Override
    public void adjustPoolSize(int delta) {
        // locked to avoid check then act race condition
        synchronized (this) {
            super.setCorePoolSize(Math.max(0, super.getCorePoolSize() + delta));
        }
    }

    @Override
    public Future<?> addThreadAndExecute(final Runnable task) {
        return addThreadAndSchedule(task, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public Future<?> addThreadAndSchedule(final Runnable task, long delay, TimeUnit unit) {
        adjustPoolSize(1);
        
        FutureTask<?> result = new PoolSizeDecreaseFuture(task);
        if (delay == 0) {
            // execute is slightly better if we can, as it avoids wrapping the future in another future
            super.execute(result);
        } else {
            super.schedule(result, delay, unit);
            // can not return future from schedule above
            // we must return our decreasing future to handle Future.cancel case
        }
        return result;
    }
    
    private class PoolSizeDecreaseFuture extends FutureTask<Object> {
        public PoolSizeDecreaseFuture(Runnable runnable) {
            super(runnable, null);
        }
        
        @Override
        protected void done() {
            // invoked when task is complete or canceled
            adjustPoolSize(-1);
        }
    }
}
