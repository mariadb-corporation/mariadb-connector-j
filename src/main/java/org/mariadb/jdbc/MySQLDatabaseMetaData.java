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

import java.sql.*;

public final class MySQLDatabaseMetaData extends CommonDatabaseMetaData {
    public MySQLDatabaseMetaData(CommonDatabaseMetaData.Builder builder) {
        super(builder);
    }

    @Override
    public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table) throws SQLException {
        final Connection conn = getConnection();
    	String query = "SELECT NULL TABLE_CAT, " +
                "table_schema TABLE_SCHEM, " +
                "table_name, " +
                "column_name, " +
                "ordinal_position KEY_SEQ," +
                "null pk_name " +
                "FROM information_schema.columns " +
                "WHERE table_name='" + table + "' AND column_key='pri'";

        if (schema != null && (((MySQLConnection)conn).noSchemaPattern == false)) {
            query += " AND table_schema = '" + schema + "'";
        }
        query += " ORDER BY column_name";
        final Statement stmt = conn.createStatement();
        return stmt.executeQuery(query);
    }
   /**
     * Maps standard table types to mysql ones - helper since table type is never "table" in mysql, it is "base table"
     * @param tableType the table type defined by user
     * @return the internal table type.
     */
    private String mapTableTypes(String tableType) {
        if(tableType.equals("TABLE")) {
            return "BASE TABLE";
        }
        return tableType;
    }
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, final String[] types) throws SQLException {

        if (tableNamePattern == null || tableNamePattern.equals(""))
            tableNamePattern = "%";

        String query = "SELECT table_catalog table_cat, "
                        + "table_schema table_schem, "
                        + "table_name, "
                        + "table_type, "
                        + "table_comment as remarks,"
                        + "null as type_cat, "
                        + "null as type_schem,"
                        + "null as type_name, "
                        + "null as self_referencing_col_name,"
                        + "null as ref_generation "
                        + "FROM information_schema.tables "
                        + "WHERE table_name LIKE '"+(tableNamePattern == null?"%":tableNamePattern)+"'"
                        + getSchemaPattern(schemaPattern);

        if(types != null) {
            query += " AND table_type in (";
            boolean first = true;
            for(String s : types) {
                String mappedType = mapTableTypes(s);
                if(!first) {
                    query += ",";
                }
                first = false;
                query += "'"+mappedType+"'";
            }
            query += ")";
        }
        final Statement stmt = getConnection().createStatement();
        return stmt.executeQuery(query);
    }

        public ResultSet getColumns( String catalog,  String schemaPattern,  String tableNamePattern,  String columnNamePattern)
            throws SQLException {

        if (tableNamePattern == null || tableNamePattern.equals(""))
            tableNamePattern = "%";

        if (columnNamePattern == null || columnNamePattern.equals(""))
            columnNamePattern = "%";

        if (schemaPattern == null || schemaPattern.equals(""))
            schemaPattern = "%";

        String query = "     SELECT null as table_cat," +
                "            table_schema as table_schem," +
                "            table_name," +
                "            column_name," +
                dataTypeClause + " data_type," +
                "            column_type type_name," +
                "            character_maximum_length column_size," +
                "            0 buffer_length," +
                "            numeric_precision decimal_digits," +
                "            numeric_scale num_prec_radix," +
                "            if(is_nullable='yes',1,0) nullable," +
                "            column_comment remarks," +
                "            column_default column_def," +
                "            0 sql_data," +
                "            0 sql_datetime_sub," +
                "            character_octet_length char_octet_length," +
                "            ordinal_position," +
                "            is_nullable," +
                "            null scope_catalog," +
                "            null scope_schema," +
                "            null scope_table," +
                "            null source_data_type," +
                "            '' is_autoincrement" +
                "    FROM information_schema.columns " +
                "WHERE ";
        if (((MySQLConnection)getConnection()).noSchemaPattern == false)
        {
        	query = query + "table_schema LIKE '" + schemaPattern + "' AND ";
        }
        query = query + "table_name LIKE '" + tableNamePattern  + "'" +
                " AND column_name LIKE '" +  columnNamePattern + "'" +
                " ORDER BY table_cat, table_schem, table_name, ordinal_position";
        final Statement stmt = getConnection().createStatement();
        return stmt.executeQuery(query);
    }
        
    public ResultSet getExportedKeys(final String catalog, final String schema, final String table) throws SQLException {
    	final Connection conn = getConnection();
    	final String query = "SELECT null PKTABLE_CAT, \n" +
                "kcu.referenced_table_schema PKTABLE_SCHEM, \n" +
                "kcu.referenced_table_name PKTABLE_NAME, \n" +
                "kcu.referenced_column_name PKCOLUMN_NAME, \n" +
                "null FKTABLE_CAT, \n" +
                "kcu.table_schema FKTABLE_SCHEM, \n" +
                "kcu.table_name FKTABLE_NAME, \n" +
                "kcu.column_name FKCOLUMN_NAME, \n" +
                "kcu.position_in_unique_constraint KEY_SEQ,\n" +
                "CASE update_rule \n" +
                "   WHEN 'RESTRICT' THEN 1\n" +
                "   WHEN 'NO ACTION' THEN 3\n" +
                "   WHEN 'CASCADE' THEN 0\n" +
                "   WHEN 'SET NULL' THEN 2\n" +
                "   WHEN 'SET DEFAULT' THEN 4\n" +
                "END UPDATE_RULE,\n" +
                "CASE delete_rule \n" +
                "   WHEN 'RESTRICT' THEN 1\n" +
                "   WHEN 'NO ACTION' THEN 3\n" +
                "   WHEN 'CASCADE' THEN 0\n" +
                "   WHEN 'SET NULL' THEN 2\n" +
                "   WHEN 'SET DEFAULT' THEN 4\n" +
                "END UPDATE_RULE,\n" +
                "rc.constraint_name FK_NAME,\n" +
                "null PK_NAME,\n" +
                "6 DEFERRABILITY\n" +
                "FROM information_schema.key_column_usage kcu\n" +
                "INNER JOIN information_schema.referential_constraints rc\n" +
                "ON kcu.constraint_schema=rc.constraint_schema\n" +
                "AND kcu.constraint_name=rc.constraint_name\n" +
                "WHERE " +
                (schema != null && (((MySQLConnection)conn).noSchemaPattern == false) ? "kcu.referenced_table_schema='" + schema + "' AND " : "") +
                "kcu.referenced_table_name='" +
                table +
                "'" +
                " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";
        final Statement stmt = getConnection().createStatement();
        return stmt.executeQuery(query);
    }
    public ResultSet getImportedKeys(final String catalog, final String schema, final String table) throws SQLException {
    	Connection conn = getConnection();
    	String query = "SELECT null PKTABLE_CAT, \n" +
                "kcu.referenced_table_schema PKTABLE_SCHEM, \n" +
                "kcu.referenced_table_name PKTABLE_NAME, \n" +
                "kcu.referenced_column_name PKCOLUMN_NAME, \n" +
                "null FKTABLE_CAT, \n" +
                "kcu.table_schema FKTABLE_SCHEM, \n" +
                "kcu.table_name FKTABLE_NAME, \n" +
                "kcu.column_name FKCOLUMN_NAME, \n" +
                "kcu.position_in_unique_constraint KEY_SEQ,\n" +
                "CASE update_rule \n" +
                "   WHEN 'RESTRICT' THEN 1\n" +
                "   WHEN 'NO ACTION' THEN 3\n" +
                "   WHEN 'CASCADE' THEN 0\n" +
                "   WHEN 'SET NULL' THEN 2\n" +
                "   WHEN 'SET DEFAULT' THEN 4\n" +
                "END UPDATE_RULE,\n" +
                "CASE delete_rule \n" +
                "   WHEN 'RESTRICT' THEN 1\n" +
                "   WHEN 'NO ACTION' THEN 3\n" +
                "   WHEN 'CASCADE' THEN 0\n" +
                "   WHEN 'SET NULL' THEN 2\n" +
                "   WHEN 'SET DEFAULT' THEN 4\n" +
                "END UPDATE_RULE,\n" +
                "rc.constraint_name FK_NAME,\n" +
                "null PK_NAME,\n" +
                "6 DEFERRABILITY\n" +
                "FROM information_schema.key_column_usage kcu\n" +
                "INNER JOIN information_schema.referential_constraints rc\n" +
                "ON kcu.constraint_schema=rc.constraint_schema\n" +
                "AND kcu.constraint_name=rc.constraint_name\n" +
                "WHERE " +
                (schema != null && (((MySQLConnection)conn).noSchemaPattern == false) ? "kcu.table_schema='" + schema + "' AND " : "") +
                "kcu.table_name='" +
                table +
                "'" +
                " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";
        
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(query);
    }

    public ResultSet getBestRowIdentifier(final String catalog, final String schema, final String table, final int scope, final boolean nullable)
            throws SQLException {
        String query = "SELECT " + DatabaseMetaData.bestRowSession + " scope," +
                "column_name," +
                dataTypeClause + " data_type," +
                "data_type type_name," +
                "if(numeric_precision is null, character_maximum_length, numeric_precision) column_size," +
                "0 buffer_length," +
                "numeric_scale decimal_digits," +
                DatabaseMetaData.bestRowNotPseudo + " pseudo_column" +
                " FROM information_schema.columns" +
                " WHERE column_key in('PRI', 'MUL', 'UNI') ";
        Connection conn = getConnection();
        if (((MySQLConnection)conn).noSchemaPattern == false)
        {
                query = query + " AND table_schema like " + (schema != null ? "'%'" : "'" + schema + "'");
        }
        query = query + " AND table_name='" + table + "' ORDER BY scope";
        final Statement stmt = conn.createStatement();
        return stmt.executeQuery(query);
    }


}
