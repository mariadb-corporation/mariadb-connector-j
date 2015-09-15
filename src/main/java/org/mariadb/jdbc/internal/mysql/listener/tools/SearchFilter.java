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
package org.mariadb.jdbc.internal.mysql.listener.tools;

public class SearchFilter {
    boolean searchForMaster;
    boolean searchForSlave;
    boolean fineIfFoundOnlyMaster;
    boolean fineIfFoundOnlySlave;
    boolean initialConnection;
    boolean uniqueLoop;

    public SearchFilter(boolean searchForMaster, boolean searchForSlave) {
        this.searchForMaster = searchForMaster;
        this.searchForSlave = searchForSlave;
    }

    public SearchFilter(boolean searchForMaster, boolean searchForSlave, boolean fineIfFoundOnlyMaster, boolean fineIfFoundOnlySlave) {
        this.searchForMaster = searchForMaster;
        this.searchForSlave = searchForSlave;
        this.fineIfFoundOnlyMaster = fineIfFoundOnlyMaster;
        this.fineIfFoundOnlySlave = fineIfFoundOnlySlave;
    }

    public SearchFilter(boolean searchForMaster, boolean searchForSlave, boolean initialConnection) {
        this.searchForMaster = searchForMaster;
        this.searchForSlave = searchForSlave;
        this.initialConnection = initialConnection;
    }

    public boolean isInitialConnection() {
        return initialConnection;
    }

    public void setInitialConnection(boolean initialConnection) {
        this.initialConnection = initialConnection;
    }

    public boolean isFineIfFoundOnlyMaster() {
        return fineIfFoundOnlyMaster;
    }

    public void setFineIfFoundOnlyMaster(boolean fineIfFoundOnlyMaster) {
        this.fineIfFoundOnlyMaster = fineIfFoundOnlyMaster;
    }

    public boolean isFineIfFoundOnlySlave() {
        return fineIfFoundOnlySlave;
    }

    public void setFineIfFoundOnlySlave(boolean fineIfFoundOnlySlave) {
        this.fineIfFoundOnlySlave = fineIfFoundOnlySlave;
    }

    public boolean isSearchForMaster() {
        return searchForMaster;
    }

    public void setSearchForMaster(boolean searchForMaster) {
        this.searchForMaster = searchForMaster;
    }

    public boolean isSearchForSlave() {
        return searchForSlave;
    }

    public void setSearchForSlave(boolean searchForSlave) {
        this.searchForSlave = searchForSlave;
    }

    public boolean isUniqueLoop() {
        return uniqueLoop;
    }

    public void setUniqueLoop(boolean uniqueLoop) {
        this.uniqueLoop = uniqueLoop;
    }

    @Override
    public String toString() {
        return "SearchFilter{" +
                "searchForMaster=" + searchForMaster +
                ", searchForSlave=" + searchForSlave +
                ", fineIfFoundOnlyMaster=" + fineIfFoundOnlyMaster +
                ", fineIfFoundOnlySlave=" + fineIfFoundOnlySlave +
                ", initialConnection=" + initialConnection +
                ", uniqueLoop=" + uniqueLoop +
                '}';
    }
}
