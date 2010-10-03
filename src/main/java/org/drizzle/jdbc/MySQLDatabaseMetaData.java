package org.drizzle.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Oct 2, 2010 Time: 9:53:38 PM To change this template use File |
 * Settings | File Templates.
 */
public final class MySQLDatabaseMetaData extends CommonDatabaseMetaData {
    public MySQLDatabaseMetaData(CommonDatabaseMetaData.Builder builder) {
        super(builder);
    }

    @Override
    public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table) throws SQLException {
        String query = "SELECT table_catalog TABLE_CAT, " +
                "table_schema TABLE_SCHEM, " +
                "table_name, " +
                "column_name, " +
                "ordinal_position KEY_SEQ," +
                "null pk_name " +
                "FROM information_schema.columns " +
                "WHERE table_name='" + table + "' AND column_key='pri'";

        if (schema != null) {
            query += " AND table_schema = '" + schema + "'";
        }
        query += " ORDER BY column_name";
        final Statement stmt = getConnection().createStatement();
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
    public ResultSet getTables(final String catalog, final String schemaPattern, final String tableNamePattern, final String[] types) throws SQLException {
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
                        + "WHERE table_name LIKE \""+(tableNamePattern == null?"%":tableNamePattern)+"\""
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

        public ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern, final String columnNamePattern)
            throws SQLException {
        final String query = "     SELECT null as table_cat," +
                "            table_schema as table_schem," +
                "            table_name," +
                "            column_name," +
                dataTypeClause + " data_type," +
                "            data_type type_name," +
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
                "WHERE table_schema LIKE '" + ((schemaPattern == null) ? "%" : schemaPattern) + "'" +
                " AND table_name LIKE '" + ((tableNamePattern == null) ? "%" : tableNamePattern) + "'" +
                " AND column_name LIKE '" + ((columnNamePattern == null) ? "%" : columnNamePattern) + "'" +
                " ORDER BY table_cat, table_schem, table_name, ordinal_position";
        final Statement stmt = getConnection().createStatement();
        return stmt.executeQuery(query);
    }
    public ResultSet getExportedKeys(final String catalog, final String schema, final String table) throws SQLException {
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
                (schema != null ? "kcu.referenced_table_schema='" + schema + "' AND " : "") +
                "kcu.referenced_table_name='" +
                table +
                "'" +
                "ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";
        final Statement stmt = getConnection().createStatement();
        return stmt.executeQuery(query);
    }
}
