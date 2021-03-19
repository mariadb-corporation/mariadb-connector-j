package org.mariadb.jdbc.client.result;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import org.mariadb.jdbc.BasePreparedStatement;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketReader;
import org.mariadb.jdbc.codec.*;
import org.mariadb.jdbc.codec.list.*;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.ParameterList;

public class UpdatableResult extends CompleteResult {
  private static final CodecList codecList = CodecLoader.get();
  private static final int STATE_STANDARD = 0;
  private static final int STATE_UPDATE = 1;
  private static final int STATE_UPDATED = 2;
  private static final int STATE_INSERT = 3;
  private static final int STATE_INSERTED = 4;

  private String database;
  private String table;
  private boolean canInsert;
  private boolean isAutoincrementPk;
  private int savedRowPointer;
  private boolean canUpdate;
  private String changeError;
  private int state = STATE_STANDARD;
  private ParameterList parameters;

  public UpdatableResult(
      Statement stmt,
      boolean binaryProtocol,
      long maxRows,
      ColumnDefinitionPacket[] metadataList,
      PacketReader reader,
      Context context,
      int resultSetType,
      boolean closeOnCompletion,
      boolean traceEnable)
      throws IOException, SQLException {
    super(
        stmt,
        binaryProtocol,
        maxRows,
        metadataList,
        reader,
        context,
        resultSetType,
        closeOnCompletion,
        traceEnable);
    checkIfUpdatable();
    parameters = new ParameterList(metadataList.length);
  }

  private void checkIfUpdatable() throws SQLException {
    isAutoincrementPk = false;
    canInsert = true;
    canUpdate = true;

    // check that resultSet concern one table and database exactly
    database = null;
    table = null;
    for (ColumnDefinitionPacket columnDefinition : metadataList) {
      if (columnDefinition.getTable().isEmpty()) {
        cannotUpdateInsertRow(
            "The result-set contains fields without without any database/table information");
        return;
      }

      if (database != null && !database.equals(columnDefinition.getSchema())) {
        cannotUpdateInsertRow("The result-set contains more than one database");
        return;
      }
      database = columnDefinition.getSchema();

      if (table != null && !table.equals(columnDefinition.getTable())) {
        cannotUpdateInsertRow("The result-set contains fields on different tables");
        return;
      }
      table = columnDefinition.getTable();
    }

    // check that listed column contain primary field
    for (ColumnDefinitionPacket col : metadataList) {
      if (col.isPrimaryKey()) {
        isAutoincrementPk = col.isAutoIncrement();
        return;
      }
    }

    canUpdate = false;
    changeError = "Cannot update rows, since primary field is not present in query";

    // check that table contain a generated primary field
    // to check if insert are still possible
    ResultSet rs =
        statement
            .getConnection()
            .createStatement()
            .executeQuery("SHOW COLUMNS FROM `" + database + "`.`" + table + "`");

    while (rs.next()) {
      if ("PRI".equals(rs.getString("Key"))) {
        boolean canBeNull = "YES".equals(rs.getString("Null"));
        boolean hasDefault = rs.getString("Default") != null;
        boolean generated = rs.getString("Extra") != null && !rs.getString("Extra").isEmpty();
        isAutoincrementPk =
            rs.getString("Extra") != null && rs.getString("Extra").contains("auto_increment");
        if (!canBeNull && !hasDefault && !generated) {
          canInsert = false;
          changeError =
              String.format("primary field `%s` is not present in query", rs.getString("Field"));
        }
        return;
      }
    }
    canInsert = false;
  }

  private void cannotUpdateInsertRow(String reason) {
    changeError = reason;
    canUpdate = false;
    canInsert = false;
  }

  private void checkUpdatable(int position) throws SQLException {
    if (position <= 0 || position > metadataList.length) {
      throw exceptionFactory.create("No such column: " + position, "22023");
    }

    if (state == STATE_STANDARD || state == STATE_UPDATED) {
      state = STATE_UPDATE;
    }
    if (state == STATE_UPDATE) {
      if (rowPointer <= BEFORE_FIRST_POS) {
        throw new SQLDataException("Current position is before the first row", "22023");
      }
      if (rowPointer >= dataSize) {
        throw new SQLDataException("Current position is after the last row", "22023");
      }
      if (!canUpdate) {
        throw exceptionFactory.create("ResultSet cannot be updated. " + changeError);
      }
    }
  }

  @Override
  public boolean rowUpdated() {
    return state == STATE_UPDATED;
  }

  @Override
  public boolean rowInserted() {
    return state == STATE_INSERTED;
  }

  @Override
  public boolean rowDeleted() {
    return false;
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, Parameter.NULL_PARAMETER);
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(BooleanCodec.INSTANCE, x));
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(ByteCodec.INSTANCE, x));
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(ShortCodec.INSTANCE, x));
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(IntCodec.INSTANCE, x));
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(LongCodec.INSTANCE, x));
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(FloatCodec.INSTANCE, x));
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(DoubleCodec.INSTANCE, x));
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(BigDecimalCodec.INSTANCE, x));
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(StringCodec.INSTANCE, x));
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(ByteArrayCodec.INSTANCE, x));
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(DateCodec.INSTANCE, x));
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(TimeCodec.INSTANCE, x));
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(TimestampCodec.INSTANCE, x));
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(StreamCodec.INSTANCE, x, (long) length));
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(StreamCodec.INSTANCE, x, (long) length));
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(ReaderCodec.INSTANCE, x, (long) length));
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    updateInternalObject(columnIndex, x, (long) scaleOrLength);
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    updateInternalObject(columnIndex, x, null);
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    updateNull(row.getIndex(columnLabel));
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    updateBoolean(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    updateByte(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    updateShort(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    updateInt(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    updateLong(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    updateFloat(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    updateDouble(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    updateBigDecimal(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    updateString(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    updateBytes(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    updateDate(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    updateTime(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    updateTimestamp(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    updateAsciiStream(row.getIndex(columnLabel), x, length);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    updateBinaryStream(row.getIndex(columnLabel), x, length);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    updateCharacterStream(row.getIndex(columnLabel), reader, length);
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    updateObject(row.getIndex(columnLabel), x, scaleOrLength);
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    updateObject(row.getIndex(columnLabel), x);
  }

  @Override
  public void insertRow() throws SQLException {
    if (state == STATE_INSERT || state == STATE_INSERTED) {

      // Create query will all field with WHERE clause contain primary field.
      // if field are not updated, value DEFAULT will be set
      // (if field has no default, then insert will throw an exception that will be return to
      // user)

      String insertSql = buildInsertQuery();
      try (PreparedStatement insertPreparedStatement =
          ((Connection) statement.getConnection())
              .prepareInternal(
                  insertSql,
                  Statement.RETURN_GENERATED_KEYS,
                  ResultSet.TYPE_FORWARD_ONLY,
                  ResultSet.CONCUR_READ_ONLY,
                  row instanceof BinaryRowDecoder)) {

        int paramPos = 0;
        for (int pos = 0; pos < metadataList.length; pos++) {
          ColumnDefinitionPacket colInfo = metadataList[pos];
          Parameter<?> param = parameters.size() > pos ? parameters.get(pos) : null;
          if (param != null) {
            ((BasePreparedStatement) insertPreparedStatement).setParameter(paramPos++, param);
          } else if (!colInfo.isPrimaryKey() && !colInfo.hasDefault()) {
            ((BasePreparedStatement) insertPreparedStatement)
                .setParameter(paramPos++, Parameter.NULL_PARAMETER);
          }
        }
        ResultSet insertRs = insertPreparedStatement.executeQuery();
        if (context.getVersion().isMariaDBServer()
            && context.getVersion().versionGreaterOrEqual(10, 5, 1)) {
          if (insertRs.next()) {
            byte[] rowByte = ((Result) insertRs).getCurrentRowData();
            addRowData(rowByte);
          }
        } else if (isAutoincrementPk) {
          // primary is auto_increment (only one field)
          ResultSet rsKey = insertPreparedStatement.getGeneratedKeys();
          if (rsKey.next()) {
            try (PreparedStatement refreshPreparedStatement = prepareRefreshStmt()) {
              refreshPreparedStatement.setObject(1, rsKey.getObject(1));
              Result rs = (Result) refreshPreparedStatement.executeQuery();
              // update row data only if not deleted externally
              if (rs.next()) {
                addRowData(rs.getCurrentRowData());
              }
            }
          }
        } else {
          addRowData(refreshRawData());
        }
      }
      parameters = new ParameterList(parameters.size());
      state = STATE_INSERTED;
    }
  }

  /**
   * Build insert query
   *
   * @return insert sql
   * @throws SQLException exception
   */
  private String buildInsertQuery() throws SQLException {
    StringBuilder insertSql = new StringBuilder("INSERT `" + database + "`.`" + table + "` ( ");
    StringBuilder valueClause = new StringBuilder();
    StringBuilder returningClause = new StringBuilder();

    boolean firstParam = true;

    for (int pos = 0; pos < metadataList.length; pos++) {
      ColumnDefinitionPacket colInfo = metadataList[pos];

      if (pos != 0) {
        returningClause.append(", ");
      }
      returningClause.append("`").append(colInfo.getColumn()).append("`");

      Parameter<?> param = parameters.size() > pos ? parameters.get(pos) : null;
      if (param != null) {
        if (!firstParam) {
          insertSql.append(",");
          valueClause.append(", ");
        }
        insertSql.append("`").append(colInfo.getColumn()).append("`");
        valueClause.append("?");
        firstParam = false;
      } else {
        if (colInfo.isPrimaryKey()) {
          if (colInfo.isAutoIncrement() || colInfo.hasDefault()) {
            if (!colInfo.isAutoIncrement()
                && (!context.getVersion().isMariaDBServer()
                    || !context.getVersion().versionGreaterOrEqual(10, 5, 1))) {
              // driver cannot know generated default value like uuid().
              // but for server 10.5+, will use RETURNING to know primary key
              throw exceptionFactory.create(
                  String.format(
                      "Cannot call insertRow() not setting value for primary key %s "
                          + "with default value before server 10.5",
                      colInfo.getColumn()));
            }
          } else {
            throw exceptionFactory.create(
                String.format(
                    "Cannot call insertRow() not setting value for primary key %s",
                    colInfo.getColumn()));
          }
        } else if (!colInfo.hasDefault()) {
          if (!firstParam) {
            insertSql.append(",");
            valueClause.append(", ");
          }
          firstParam = false;
          insertSql.append("`").append(colInfo.getColumn()).append("`");
          valueClause.append("?");
        }
      }
    }
    insertSql.append(") VALUES (").append(valueClause).append(")");
    if (context.getVersion().isMariaDBServer()
        && context.getVersion().versionGreaterOrEqual(10, 5, 1)) {
      insertSql.append(" RETURNING ").append(returningClause);
    }
    return insertSql.toString();
  }

  private String refreshStmt() {
    // Construct SELECT query according to column metadata, with WHERE part containing primary
    // fields
    StringBuilder selectSql = new StringBuilder("SELECT ");
    StringBuilder whereClause = new StringBuilder(" WHERE ");

    boolean firstPrimary = true;
    for (int pos = 0; pos < metadataList.length; pos++) {
      ColumnDefinitionPacket colInfo = metadataList[pos];
      if (pos != 0) {
        selectSql.append(",");
      }
      selectSql.append("`").append(colInfo.getColumn()).append("`");

      if (colInfo.isPrimaryKey()) {
        if (!firstPrimary) {
          whereClause.append("AND ");
        }
        firstPrimary = false;
        whereClause.append("`").append(colInfo.getColumn()).append("` = ? ");
      }
    }
    selectSql
        .append(" FROM `")
        .append(database)
        .append("`.`")
        .append(table)
        .append("`")
        .append(whereClause);
    return selectSql.toString();
  }

  private PreparedStatement prepareRefreshStmt() throws SQLException {
    // row's raw bytes must be encoded according to current resultSet type
    // so use Server or Client PrepareStatement accordingly
    return ((Connection) statement.getConnection())
        .prepareInternal(
            refreshStmt(),
            Statement.RETURN_GENERATED_KEYS,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            row instanceof BinaryRowDecoder);
  }

  private byte[] refreshRawData() throws SQLException {
    int fieldsPrimaryIndex = 0;
    try (PreparedStatement refreshPreparedStatement = prepareRefreshStmt()) {
      for (int pos = 0; pos < metadataList.length; pos++) {
        ColumnDefinitionPacket colInfo = metadataList[pos];
        if (colInfo.isPrimaryKey()) {
          if ((state != STATE_STANDARD) && parameters.size() > pos && parameters.get(pos) != null) {
            // Row has just been updated using updateRow() methods.
            // updateRow might have changed primary key, so must use the new value.
            Parameter<?> value = parameters.get(pos);
            ((BasePreparedStatement) refreshPreparedStatement)
                .setParameter(fieldsPrimaryIndex++, value);
          } else {
            refreshPreparedStatement.setObject(++fieldsPrimaryIndex, getObject(pos + 1));
          }
        }
      }

      Result rs = (Result) refreshPreparedStatement.executeQuery();
      rs.next();
      return rs.getCurrentRowData();
    }
  }

  private String updateQuery() {
    StringBuilder updateSql = new StringBuilder("UPDATE `" + database + "`.`" + table + "` SET ");
    StringBuilder whereClause = new StringBuilder(" WHERE ");

    boolean firstUpdate = true;
    boolean firstPrimary = true;
    for (int pos = 0; pos < metadataList.length; pos++) {
      ColumnDefinitionPacket colInfo = metadataList[pos];

      if (colInfo.isPrimaryKey()) {
        if (!firstPrimary) {
          whereClause.append("AND ");
        }
        firstPrimary = false;
        whereClause.append("`").append(colInfo.getColumn()).append("` = ? ");
      }

      if (parameters.size() > pos && parameters.get(pos) != null) {
        if (!firstUpdate) {
          updateSql.append(",");
        }
        firstUpdate = false;
        updateSql.append("`").append(colInfo.getColumn()).append("` = ? ");
      }
    }
    if (firstUpdate) return null;
    return updateSql.append(whereClause.toString()).toString();
  }

  @Override
  public void updateRow() throws SQLException {

    if (state == STATE_INSERT) {
      throw exceptionFactory.create("Cannot call updateRow() when inserting a new row");
    }

    if (rowPointer < 0) {
      throw exceptionFactory.create("Current position is before the first row", "22023");
    }

    if (rowPointer >= dataSize) {
      throw exceptionFactory.create("Current position is after the last row", "22023");
    }

    if (state == STATE_UPDATE || state == STATE_UPDATED) {

      // state is STATE_UPDATE, meaning that at least one field is modified, update query can be
      // run.
      // Construct UPDATE query according to modified field only
      String updateQuery = updateQuery();
      if (updateQuery != null) {
        try (PreparedStatement preparedStatement =
            ((Connection) statement.getConnection())
                .prepareInternal(
                    updateQuery,
                    Statement.RETURN_GENERATED_KEYS,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    row instanceof BinaryRowDecoder)) {

          int fieldsIndex = 0;
          for (int pos = 0; pos < metadataList.length; pos++) {
            if (parameters.size() > pos) {
              Parameter<?> param = parameters.get(pos);
              if (param != null) {
                ((BasePreparedStatement) preparedStatement).setParameter(fieldsIndex++, param);
              }
            }
          }

          for (int pos = 0; pos < metadataList.length; pos++) {
            ColumnDefinitionPacket colInfo = metadataList[pos];
            if (colInfo.isPrimaryKey()) {
              preparedStatement.setObject(++fieldsIndex, getObject(pos + 1));
            }
          }

          preparedStatement.execute();
        }
        refreshRow();
      }
      parameters = new ParameterList(parameters.size());
      state = STATE_UPDATED;
    }
  }

  @Override
  public void deleteRow() throws SQLException {

    if (state == STATE_INSERT) {
      throw exceptionFactory.create("Cannot call deleteRow() when inserting a new row");
    }
    if (!canUpdate) {
      throw exceptionFactory.create("ResultSet cannot be updated. " + changeError);
    }
    if (rowPointer < 0) {
      throw new SQLDataException("Current position is before the first row", "22023");
    }
    if (rowPointer >= dataSize) {
      throw new SQLDataException("Current position is after the last row", "22023");
    }

    // Create query with WHERE clause contain primary field.
    StringBuilder deleteSql =
        new StringBuilder("DELETE FROM `" + database + "`.`" + table + "` WHERE ");
    boolean firstPrimary = true;
    for (ColumnDefinitionPacket colInfo : metadataList) {
      if (colInfo.isPrimaryKey()) {
        if (!firstPrimary) {
          deleteSql.append("AND ");
        }
        firstPrimary = false;
        deleteSql.append("`").append(colInfo.getColumn()).append("` = ? ");
      }
    }

    try (PreparedStatement deletePreparedStatement =
        ((Connection) statement.getConnection())
            .prepareInternal(
                deleteSql.toString(),
                Statement.RETURN_GENERATED_KEYS,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                false)) {

      int fieldsPrimaryIndex = 1;
      for (int pos = 0; pos < metadataList.length; pos++) {
        ColumnDefinitionPacket colInfo = metadataList[pos];
        if (colInfo.isPrimaryKey()) {
          deletePreparedStatement.setObject(fieldsPrimaryIndex++, getObject(pos + 1));
        }
      }

      deletePreparedStatement.executeUpdate();

      // remove data
      System.arraycopy(data, rowPointer + 1, data, rowPointer, dataSize - 1 - rowPointer);
      data[dataSize - 1] = null;
      dataSize--;
      previous();
    }
  }

  @Override
  public void refreshRow() throws SQLException {
    if (state == STATE_INSERT) {
      throw exceptionFactory.create("Cannot call refreshRow() when inserting a new row");
    }
    if (rowPointer < 0) {
      throw exceptionFactory.create("Current position is before the first row", "22023");
    }
    if (rowPointer >= data.length) {
      throw exceptionFactory.create("Current position is after the last row", "22023");
    }
    if (canUpdate) {
      updateRowData(refreshRawData());
    }
  }

  @Override
  public void cancelRowUpdates() {
    parameters = new ParameterList(parameters.size());
    state = STATE_STANDARD;
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    if (!canInsert) {
      throw exceptionFactory.create("No row can be inserted. " + changeError);
    }
    parameters = new ParameterList(parameters.size());
    state = STATE_INSERT;
    savedRowPointer = rowPointer;
  }

  @Override
  public void moveToCurrentRow() {
    state = STATE_STANDARD;
    resetToRowPointer();
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(BlobCodec.INSTANCE, x));
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    updateBlob(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(ClobCodec.INSTANCE, x));
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    updateClob(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    updateString(columnIndex, nString);
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    updateString(columnLabel, nString);
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    updateClob(columnIndex, nClob);
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    updateClob(columnLabel, nClob);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    updateCharacterStream(columnIndex, x, length);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    updateCharacterStream(columnLabel, reader, length);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(StreamCodec.INSTANCE, x, length));
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(StreamCodec.INSTANCE, x, length));
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(ReaderCodec.INSTANCE, x, length));
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    updateAsciiStream(row.getIndex(columnLabel), x, length);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    updateBinaryStream(row.getIndex(columnLabel), x, length);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    updateCharacterStream(row.getIndex(columnLabel), reader, length);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream x, long length) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(StreamCodec.INSTANCE, x, length));
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    updateBlob(row.getIndex(columnLabel), inputStream, length);
  }

  @Override
  public void updateClob(int columnIndex, Reader x, long length) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(ReaderCodec.INSTANCE, x, length));
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    super.updateClob(row.getIndex(columnLabel), reader, length);
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    updateClob(columnIndex, reader, length);
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    updateClob(columnLabel, reader, length);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    updateCharacterStream(columnIndex, x);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    updateCharacterStream(columnLabel, reader);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(StreamCodec.INSTANCE, x));
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(StreamCodec.INSTANCE, x));
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(ReaderCodec.INSTANCE, x));
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    updateAsciiStream(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    updateBinaryStream(row.getIndex(columnLabel), x);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    updateCharacterStream(row.getIndex(columnLabel), reader);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(StreamCodec.INSTANCE, x));
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    updateBlob(row.getIndex(columnLabel), inputStream);
  }

  @Override
  public void updateClob(int columnIndex, Reader x) throws SQLException {
    checkUpdatable(columnIndex);
    parameters.set(columnIndex - 1, new Parameter<>(ReaderCodec.INSTANCE, x));
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    updateClob(row.getIndex(columnLabel), reader);
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    updateClob(columnIndex, reader);
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    updateClob(columnLabel, reader);
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    updateInternalObject(columnIndex, x, (long) scaleOrLength);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void updateInternalObject(int columnIndex, Object x, Long scaleOrLength)
      throws SQLException {
    checkUpdatable(columnIndex);
    if (x == null) {
      parameters.set(columnIndex - 1, Parameter.NULL_PARAMETER);
      return;
    }

    for (Codec<?> codec : codecList.getCodecs()) {
      if (codec.canEncode(x)) {
        Parameter p = new Parameter(codec, x, scaleOrLength);
        parameters.set(columnIndex - 1, p);
        return;
      }
    }

    throw new SQLException(String.format("Type %s not supported type", x.getClass().getName()));
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    updateObject(row.getIndex(columnLabel), x, targetSqlType, scaleOrLength);
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
    updateInternalObject(columnIndex, x, null);
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType)
      throws SQLException {
    updateObject(row.getIndex(columnLabel), x, targetSqlType);
  }

  @Override
  public int getConcurrency() {
    return CONCUR_UPDATABLE;
  }

  private void resetToRowPointer() {
    rowPointer = savedRowPointer;
    if (rowPointer != BEFORE_FIRST_POS && rowPointer < dataSize - 1) {
      row.setRow(data[rowPointer]);
    } else {
      // all data are reads and pointer is after last
      row.setRow(null);
    }
    savedRowPointer = -1;
  }

  @Override
  public void beforeFirst() throws SQLException {
    if (state == STATE_INSERT) {
      resetToRowPointer();
    }
    state = STATE_STANDARD;
    super.beforeFirst();
  }

  @Override
  public boolean first() throws SQLException {
    if (state == STATE_INSERT) {
      resetToRowPointer();
    }
    state = STATE_STANDARD;
    return super.first();
  }

  @Override
  public boolean last() throws SQLException {
    if (state == STATE_INSERT) {
      resetToRowPointer();
    }
    state = STATE_STANDARD;
    return super.last();
  }

  @Override
  public void afterLast() throws SQLException {
    if (state == STATE_INSERT) {
      resetToRowPointer();
    }
    state = STATE_STANDARD;
    super.afterLast();
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    if (state == STATE_INSERT) {
      resetToRowPointer();
    }
    state = STATE_STANDARD;
    return super.absolute(row);
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    if (state == STATE_INSERT) {
      resetToRowPointer();
    }
    state = STATE_STANDARD;
    return super.relative(rows);
  }

  @Override
  public boolean next() throws SQLException {
    if (state == STATE_INSERT) {
      resetToRowPointer();
    }
    state = STATE_STANDARD;
    return super.next();
  }

  @Override
  public boolean previous() throws SQLException {
    if (state == STATE_INSERT) {
      resetToRowPointer();
    }
    state = STATE_STANDARD;
    return super.previous();
  }
}
