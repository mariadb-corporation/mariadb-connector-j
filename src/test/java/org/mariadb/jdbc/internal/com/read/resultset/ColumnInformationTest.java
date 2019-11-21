package org.mariadb.jdbc.internal.com.read.resultset;

import org.junit.Assert;
import org.junit.Test;
import org.mariadb.jdbc.internal.ColumnType;

public class ColumnInformationTest {

  @Test
  public void fastPathTest() {
    String column4BytesUtf8 = "_\uD83D\uDE0E\uD83D\uDE0E\uD83C\uDF36\uD83C\uDF36\uD83C\uDFA4\uD83C\uDFA4\uD83E\uDD42\uD83E\uDD42";

    ColumnInformation col = ColumnInformation.create(column4BytesUtf8 + column4BytesUtf8, ColumnType.STRING);
    Assert.assertEquals("", col.getDatabase());
    Assert.assertEquals("", col.getTable());
    Assert.assertEquals("", col.getOriginalTable());
    Assert.assertEquals(column4BytesUtf8 + column4BytesUtf8, col.getName());
    Assert.assertEquals(column4BytesUtf8 + column4BytesUtf8, col.getOriginalName());
    Assert.assertEquals(33, col.getCharsetNumber());
  }
}
