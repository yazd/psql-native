module psql;

public import
	psql.connection,
	psql.query;

import
	vibe.core.connectionpool;

final class PSQL
{
	private
	{
		ConnectionPool!Connection m_pool;
	}

	static immutable ushort defaultPort = 5432;

	this(string database, string username, string host, ushort port = defaultPort)
	{
		m_pool = new ConnectionPool!Connection({
			auto ret = new Connection(database, username, host, port);
			ret.connect();
			return ret;
		});
	}

	auto lockConnection()
	{
		return m_pool.lockConnection();
	}
}