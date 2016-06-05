/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.
Copyright (c) 2015 Avaya Inc.
Copyright (c) 2015-2016 MariaDB Ab.

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

package org.mariadb.jdbc.internal.util;

/**
 * Created by krasaee on 2016-06-05.
 */
public class JavaVersion {
    private Integer discard;
    private Integer major;
    private Integer minor;
    private Integer update;
    private Integer build;

    private static JavaVersion _cached = null;

    /**
     * Looks at the java.runtime.version and returns an instance descripting the different java version levels.
     * @return a new intance of JavaVersion
     */
    public static JavaVersion version() {

        if (_cached != null) {
            return _cached;
        }

        String[] elements = System.getProperty("java.runtime.version").split("\\.|_|-b");

        JavaVersion version = new JavaVersion();
        if (elements != null && !"".equals(elements)) {
            version.discard = elements.length > 0 ? Integer.parseInt(elements[0]) : null;
            version.major = elements.length > 1 ? Integer.parseInt(elements[1]) : null;
            version.minor = elements.length > 2 ? Integer.parseInt(elements[2]) : null;
            version.update = elements.length > 3 ? Integer.parseInt(elements[3]) : null;
            version.build = elements.length > 4 ? Integer.parseInt(elements[4]) : null;
        }

        _cached = version;

        return version;
    }

    /**
     * Return the discard version, usually 1.
     *
     * @return return discoard version
     */
    public Integer getDiscard() {
        return discard;
    }

    /**
     * Return the discard version, i.e. 4,5,6,7,8 or higher.
     *
     * @return return major version
     */
    public Integer getMajor() {
        return major;
    }

    /**
     * Return the minor version, usually.
     *
     * @return return update version
     */
    public Integer getMinor() {
        return minor;
    }

    /**
     * Return the update version.
     *
     * @return return update version
     */
    public Integer getUpdate() {
        return update;
    }

    /**
     * Return the build version, i.e. b-15 (removes the b- and only returns 15).
     *
     * @return return build version
     */
    public Integer getBuild() {
        return build;
    }

    /**
     * Determines whether the runtime java version is at a minimum version (i.e greater or equal to version 1.7).
     *
     * @param discard the discard version, usually 1.
     * @param major the version to check against, greater or equal to a specific version such as 7.
     * @return returns <code>true</code> if minimum version is met, otherwise <code>false</code>
     */
    public boolean isMinimum(int discard, int major) {
        return (this.discard >= discard && this.major >= major);
    }

    /**
     * Determines whether the runtime java version is at a minimum version (i.e greater or equal to version 1.7).
     *
     * @param major  the version to check against, greater or equal to a specific version such as 7.
     * @return returns <code>true</code> if minimum version is met, otherwise <code>false</code>
     */
    public boolean isMinimum(int major) {
        return isMinimum(1, major);
    }
}
