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

package org.mariadb.jdbc;



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

    static boolean acceptsURL(String url) {
    	return (url != null) &&
    			( 
    		    // url.startsWith("jdbc:mysql:thin://") ||
    			url.startsWith("jdbc:mariadb://") ||
    			url.startsWith("jdbc:mysql://")
    			);
    	
    }
    public static JDBCUrl parse(final String url) {
        if(url.startsWith("jdbc:mysql://")) {
            return parseConnectorJUrl(url);
        }
        String[] arr = new String[] {"jdbc:mysql:thin://","jdbc:mariadb://"};
        for (String prefix : arr) {
        	if (url.startsWith(prefix)) {
        		return parseConnectorJUrl("jdbc:mysql://" + url.substring(prefix.length()));
        	}
        }
        return null;
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
