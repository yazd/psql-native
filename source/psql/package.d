module psql;

public import
  psql.connection,
  psql.exceptions,
  psql.query;

import
  vibe.core.connectionpool;

/**
 * PSQL is the source of connections to a postgres database.
 */
final class PSQL
{
  private
  {
    ConnectionPool!Connection m_pool;
  }

  static immutable ushort defaultPort = 5432;

  /**
   * Setups postgres connection parameters for new connections.
   */
  this(string database, string username, string host, ushort port = defaultPort)
  {
    m_pool = new ConnectionPool!Connection({
      auto ret = new Connection(database, username, host, port);
      ret.connect();
      ret.authenticate();
      ret.waitForSetup();
      return ret;
    });
  }

  /**
   * Provides a new postgres connection associated with the calling fiber.
   * The connection provided will be ready to make queries.
   */
  auto lockConnection()
  {
    return m_pool.lockConnection();
  }
}
