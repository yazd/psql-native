module psql.connection;

import
	psql.common,
	psql.query;

import
	vibe.core.net;

import
	std.conv,
	std.exception,
	std.traits;

final class Connection
{
	package
	{
		TCPConnection m_connection;
		ConnectionState m_state;

		string m_db;
		string m_username;
		string m_host;
		ushort m_port;
	}

	static immutable u32 protocolVersion = 0x00030000;

	this(string database, string username, string host, ushort port)
	{
		m_state = ConnectionState.setup;
		m_db = database;
		m_username = username;
		m_host = host;
		m_port = port;
	}

	void connect()
	{
		m_state = ConnectionState.connecting;
		m_connection = connectTCP(m_host, m_port);
		m_state = ConnectionState.connected;
	}

	void authenticate()
	{
		m_state = ConnectionState.authenticating;

		// request
		{
			static immutable userMsg = "user";
			static immutable databaseMsg = "database";
			static immutable encodingMsg = "client_encoding";
			static immutable utf8Msg = "UTF8";

			u32 msgLength = cast(u32) (
				u32.sizeof + u32.sizeof
				+ userMsg.length + 1
				+ m_username.length + 1
				+ databaseMsg.length + 1
				+ m_db.length + 1
				+ encodingMsg.length + 1
				+ utf8Msg.length + 1
				+ 1
			);

			send(msgLength);
			send(protocolVersion);
			sendz(userMsg);
			sendz(m_username);
			sendz(databaseMsg);
			sendz(m_db);
			sendz(encodingMsg);
			sendz(utf8Msg);
			send!ubyte(0);
			flush();
		}

		// response
		{
			char response = recv!ubyte();
			u32 msgLength = recv!u32();

			enforce(response == 'R', "protocol error");
			u32 auth = recv!u32();
			enforce(auth == 0, "authentication failure");
			m_state = ConnectionState.authenticated;
		}
	}

	void waitForSetup()
	{
		m_state = ConnectionState.backendSetup;

		wait:
		while (true)
		{
			// wait for ReadyForQuery message or error
			char response = recv!ubyte();
			u32 msgLength = recv!u32();

			switch (response)
			{
				case 'K': // BackendKeyData
					skipRecv(msgLength - u32size);
					break;

				case 'S': // ParameterStatus
					skipRecv(msgLength - u32size);
					break;

				case 'Z': // ReadyForQuery
					auto transactionStatus = handleReadyForQuery(msgLength);
					assert(transactionStatus == TransactionStatus.idle);
					break wait;

				case 'E': // ErrorResponse
					handleErrorResponse(msgLength);
					throw new Exception("error response received");

				case 'N': // NoticeResponse
					handleNoticeResponse(msgLength);
					break;

				default:
					m_state = ConnectionState.invalid;
					assert(0);
			}
		}
	}

	SimpleQuery query(const(char[]) command)
	{
		assert(m_state == ConnectionState.readyForQuery);

		SimpleQuery q = SimpleQuery(this);
		q.sendCommand(command);
		q.nextCommand();
		return q;
	}

	package
	TransactionStatus handleReadyForQuery(u32 length)
	{
		m_state = ConnectionState.readyForQuery;
		assert(length == 5);
		char indicator = recv!ubyte();
		return indicator.to!TransactionStatus;
	}

	package
	void handleNoticeResponse(u32 length)
	{
		skipRecv(length - u32size);
		// if (m_onNotice) m_onNotice();
	}

	package
	void handleErrorResponse(u32 length)
	{
		m_state = ConnectionState.invalid;
		skipRecv(length - u32size);
		throw new ErrorResponseException();
	}

	package
	void send(T)(T value) if (isNumeric!T)
	{
		import std.bitmanip;
		m_connection.write(value.nativeToBigEndian());
	}

	package
	void send(T)(T value) if (isSomeString!T)
	{
		import std.string;
		m_connection.write(value.representation);
	}

	package
	void sendz(T)(T value) if (isSomeString!T)
	{
		import std.string;
		m_connection.write(value.representation);
		m_connection.write(['\0']);
	}

	package
	void flush()
	{
		m_connection.flush();
	}

	package
	T recv(T)() if (isNumeric!T)
	{
		import std.bitmanip;
		ubyte[T.sizeof] buf;
		m_connection.read(buf);
		return bigEndianToNative!T(buf);
	}

	package
	void recv(ubyte[] buffer)
	{
		m_connection.read(buffer);
	}

	package
	T recv(T)(ref u32 maxLength) if (isSomeString!T)
	{
		import std.algorithm : countUntil;

		// reads until \0 byte
		while (m_connection.dataAvailableForRead())
		{
			const(ubyte[]) availableData = m_connection.peek();

			auto strLength = availableData.countUntil('\0');
			if (strLength >= 0)
			{
				enforceEx!ProtocolException(strLength < maxLength);
				char[] buffer = new char[strLength];
				ubyte[] buf = (cast(ubyte*) buffer.ptr)[0 .. strLength];
				m_connection.read(buf); // read string
				skipRecv(1); // skip zero
				maxLength -= strLength + 1;
				return assumeUnique(buffer);
			}
		}

		return null;
	}

	package
	void skipRecv(u32 bytes)
	{
		ubyte[32] buf;
		while (bytes > 0)
		{
			u32 l = bytes > buf.length ? buf.length : bytes;
			m_connection.read(buf[0 .. l]);
			bytes -= l;
		}
	}
}

enum ConnectionState
{
	setup, connecting, connected, authenticating, authenticated, backendSetup, readyForQuery, inQuery, closing, invalid,
}

enum TransactionStatus
{
	idle = 'I', inBlock = 'T', inFailedBlock = 'E'
}
