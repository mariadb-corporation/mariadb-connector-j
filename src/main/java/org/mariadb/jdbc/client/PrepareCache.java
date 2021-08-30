package org.mariadb.jdbc.client;

import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.export.Prepare;

public interface PrepareCache {

  Prepare get(String key, ServerPreparedStatement preparedStatement);

  Prepare put(String key, Prepare result, ServerPreparedStatement preparedStatement);

  void reset();
}
