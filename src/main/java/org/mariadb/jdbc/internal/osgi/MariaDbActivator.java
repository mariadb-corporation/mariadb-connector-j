/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2019 Christoph Läubrich
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

package org.mariadb.jdbc.internal.osgi;

import java.sql.*;
import java.util.Dictionary;
import java.util.Hashtable;

import org.mariadb.jdbc.Driver;
import org.mariadb.jdbc.MariaDbDatabaseMetaData;
import org.mariadb.jdbc.internal.util.constant.Version;
import org.mariadb.jdbc.internal.util.scheduler.*;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.jdbc.*;

/**
 * The MariaDbActivator registers the JDBC Service with the OSGi-Framework.
 * 
 */
public class MariaDbActivator implements BundleActivator {

  private ServiceRegistration<DataSourceFactory> service;

  @Override
  public void start(BundleContext context) throws Exception {
    Dictionary<String, Object> properties = new Hashtable<>();
    properties.put(DataSourceFactory.OSGI_JDBC_DRIVER_CLASS, Driver.class.getName());
    properties.put(DataSourceFactory.OSGI_JDBC_DRIVER_NAME, MariaDbDatabaseMetaData.DRIVER_NAME);
    properties.put(DataSourceFactory.OSGI_JDBC_DRIVER_VERSION, Version.version);
    service = context.registerService(DataSourceFactory.class, new MariaDbDataSourceFactory(),
        properties);
  }

  @Override
  public void stop(BundleContext context) throws Exception {

    if (service != null) {
      service.unregister();
      service = null;
    }

    if (DriverManager.getDriver("jdbc:mariadb:") != null) {
      SchedulerServiceProviderHolder.close();
    }
  }

}
