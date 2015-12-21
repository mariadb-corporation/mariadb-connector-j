package org.mariadb.jdbc.internal.util;

/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

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

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provider for when ever an internal thread pool is needed.  This can allow library users to 
 * override our default pooling behavior with possibly better and faster options.
 */
public class SchedulerServiceProviderHolder {
    /**
     * The default provider will construct a new pool on every request.  Shutdown of pools returned
     * from this provider is restricted, forcing them to use 
     * {@link SchedulerProvider#shutdownScheduler(ScheduledExecutorService)}.
     */
    protected static SchedulerProvider DEFAULT_PROVIDER = new SchedulerProvider() {
        @Override
        public ScheduledExecutorService getScheduler(int minimumThreads) {
            return new ShutdownRestrictedScheduledExecutorService(minimumThreads);
        }

        @Override
        public void shutdownScheduler(ScheduledExecutorService scheduler) {
            ((ShutdownRestrictedScheduledExecutorService)scheduler).uncheckedShutdown();
        }
    };
    
    private static volatile SchedulerProvider currentProvider = null;
    
    /**
     * Change the current set scheduler provider.  This provider will be provided in future requests 
     * to {@link #getSchedulerProvider()}.
     * 
     * @param newProvider New provider to use, or {@code null} to use the default provider
     */
    public static void setSchedulerProvider(SchedulerProvider newProvider) {
        currentProvider = newProvider;
    }
    
    /**
     * Get the currently set {@link SchedulerProvider} from set invocations via 
     * {@link #setSchedulerProvider(SchedulerProvider)}.  If none has been set a default provider 
     * will be provided (never a {@code null} result).
     * 
     * @return Provider to get scheduler pools from
     */
    public static SchedulerProvider getSchedulerProvider() {
        SchedulerProvider result = currentProvider;
        if (result == null) {
            return DEFAULT_PROVIDER;
        } else {
            return result;
        }
    }
    
    /**
     * <p>Provider for thread pools which allow scheduling capabilities.  It is expected that the 
     * thread pools entire lifecycle (start to stop) is done through the same provider instance.</p>
     */
    public interface SchedulerProvider {
        /**
         * Request to get a scheduler with a minimum number of AVAILABLE threads.  The scheduler 
         * returned from this should not be shutdown directly, but instead 
         * {@link #shutdownScheduler(ScheduledExecutorService)} should be invoked once ready to be 
         * shutdown.
         * 
         * @param minimumThreads Minimum number of available threads for the returned scheduler
         * @return A new scheduler that is ready to accept tasks
         */
        public ScheduledExecutorService getScheduler(int minimumThreads);
        
        /**
         * Once a scheduler provided by this provider is ready to be shutdown, this should be 
         * invoked.  It is expected that ONLY schedulers produced by this provider will be accepted 
         * for shutdown, if a different scheduler is provided behavior is undefined.
         * 
         * @param scheduler Previously provided scheduler to be shutdown
         */
        public void shutdownScheduler(ScheduledExecutorService scheduler);
    }
    
    /**
     * Small class which restricts shutdown actions.  Attempts to shutdown will result in an 
     * {@link UnsupportedOperationException}.  Instead shutdown should be invoked through 
     * {@link ShutdownRestrictedScheduledExecutorService#uncheckedShutdown()}.
     */
    private static class ShutdownRestrictedScheduledExecutorService extends ScheduledThreadPoolExecutor {
        private static final AtomicInteger POOL_ID = new AtomicInteger();

        public ShutdownRestrictedScheduledExecutorService(int corePoolSize) {
            super(corePoolSize, new ThreadFactory() {
                private final int thisPoolId = POOL_ID.incrementAndGet();
                // start from DefaultThread factory to get security groups and what not
                private final ThreadFactory parentFactory = Executors.defaultThreadFactory();
                private final AtomicInteger threadId = new AtomicInteger();

                @Override
                public Thread newThread(Runnable runnable) {
                    Thread result = parentFactory.newThread(runnable);
                    result.setName("mariaDb-" + thisPoolId + "-" + threadId.incrementAndGet());
                    
                    return result;
                }
            });
        }
        
        protected void uncheckedShutdown() {
            super.shutdownNow();
        }
        
        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException("Pool must be shutdown through scheduler provider");
        }
        
        @Override
        public void shutdown() {
            throw new UnsupportedOperationException("Pool must be shutdown through scheduler provider");
        }
    }
}
