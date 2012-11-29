/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab. All Rights Reserved.

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

All rights reserved.

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

package org.mariadb.jdbc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a jdbc url.
 * <p/>
 * User: marcuse Date: Apr 21, 2009 Time: 9:32:34 AM
 */
public class JDBCUrl {
    private String username;
    private String password;
    private String database;
    private HostAddress addresses[];


    private JDBCUrl( String username, String password, String database, HostAddress addresses[]) {
        this.username = username;
        this.password = password;
        this.database = database;
        this.addresses = addresses;
    }

    /*
    Parse ConnectorJ compatible urls
    jdbc:mysql://host:port/database

     */
    private static JDBCUrl parseConnectorJUrl(String url) {
        if (!url.startsWith("jdbc:mysql://")) {
            return null;
        }
        url = url.substring(13);
        String hostname;
        String database;
        String[] tokens = url.split("/");
        hostname=tokens[0];
        database=(tokens.length > 1)?tokens[1]:null;
        return new JDBCUrl("", "",  database, HostAddress.parse(hostname));
    }

    public static JDBCUrl parse(final String url) {

        final String username;
        final String password;
        final String hostname;
        final int port;
        final String database;        

        if(url.startsWith("jdbc:mysql://")) {
            return parseConnectorJUrl(url);
        }
        final Pattern p = Pattern.compile("^jdbc:(mysql:thin)://((\\w+)(:(\\w*))?@)?([^/:]+)(:(\\d+))?(/(\\w+))?");
        final Matcher m = p.matcher(url);
        if (m.find()) {


            username = (m.group(3) == null ? "" : m.group(3));
            password = (m.group(5) == null ? "" : m.group(5));
            hostname = (m.group(6) == null ? "" : m.group(6));
            if (m.group(8) != null) {
                port = Integer.parseInt(m.group(8));
            } else {
                port = 3306;
            }
            database = m.group(10);
            HostAddress addresses[] = new HostAddress[1];
            addresses[0] = new HostAddress();
            addresses[0].host = hostname;
            addresses[0].port = port;
            return new JDBCUrl(username, password, database, addresses);
        } else {
            return null;
        }
    }
    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getHostname() {
        return addresses[0].host;
    }

    public int getPort() {
        return addresses[0].port;
    }

    public String getDatabase() {
        return database;
    }


    public HostAddress[] getHostAddresses() {
	return this.addresses;
    }

    public String toString() {
        String s = "jdbc:mysql://";
        if (addresses != null)
            s += HostAddress.toString(addresses);
        if (database != null)
            s += "/" + database;
       return s;
    }

}
