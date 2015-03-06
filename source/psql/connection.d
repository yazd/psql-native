module psql.connection;

import
	psql.common;

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
		Stream m_stream;

		string m_db;
		string m_username;
		string m_host;
		ushort m_port;

		enum TransactionStatus
		{
			idle = 'I', inBlock = 'T', inFailedBlock = 'E'
		}
	}

	static immutable u32 protocolVersion = 0x00030000;

	this(string database, string username, string host, ushort port)
	{
		m_db = database;
		m_username = username;
		m_host = host;
		m_port = port;
	}

	void connect()
	{
		m_connection = connectTCP(m_host, m_port);
		m_stream = m_connection;
		authenticate();
	}

	void authenticate()
	{
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

			wait:
			while (true)
			{
				// wait for ReadyForQuery message or error
				response = recv!ubyte();
				msgLength = recv!u32();

				switch (response)
				{
					case 'K': // BackendKeyData
						skipRecv(msgLength - u32size);
						break;

					case 'S': // ParameterStatus
						skipRecv(msgLength - u32size);
						break;

					case 'Z': // ReadyForQuery
						assert(handleReadyForQuery(msgLength) == TransactionStatus.idle);
						break wait;

					case 'E': // ErrorResponse
						skipRecv(msgLength - u32size);
						throw new Exception("error response received");

					case 'N': // NoticeResponse
						handleNoticeResponse(msgLength);
						break;

					default:
						assert(0);
				}
			}
		}
	}

	package TransactionStatus handleReadyForQuery(u32 length)
	{
		assert(length == 5);
		char indicator = recv!ubyte();
		return indicator.to!TransactionStatus;
	}

	package void handleNoticeResponse(u32 length)
	{
		skipRecv(length - u32size);
	}

	package void handleErrorResponse(u32 length)
	{
		skipRecv(length - u32size);
		throw new ErrorResponseException();
	}

	package void send(T)(T value) if (isNumeric!T)
	{
		import std.bitmanip;
		m_stream.write(value.nativeToBigEndian());
	}

	package void send(T)(T value) if (isSomeString!T)
	{
		import std.string;
		m_stream.write(value.representation);
	}

	package void sendz(T)(T value) if (isSomeString!T)
	{
		import std.string;
		m_stream.write(value.representation);
		m_stream.write(['\0']);
	}

	package void flush()
	{
		m_stream.flush();
	}

	package T recv(T)() if (isNumeric!T)
	{
		import std.bitmanip;
		ubyte[T.sizeof] buf;
		m_stream.read(buf);
		return bigEndianToNative!T(buf);
	}

	package T recv(T)(ref u32 maxLength) if (isSomeString!T)
	{
		import std.algorithm : countUntil;

		// reads until \0 byte
		while (m_stream.dataAvailableForRead())
		{
			const(ubyte[]) availableData = m_stream.peek();

			auto strLength = availableData.countUntil('\0');
			if (strLength >= 0)
			{
				enforceEx!ProtocolException(strLength < maxLength);
				char[] buffer = new char[strLength];
				ubyte[] buf = (cast(ubyte*) buffer.ptr)[0 .. strLength];
				m_stream.read(buf); // read string
				skipRecv(1); // skip zero
				maxLength -= strLength + 1;
				return assumeUnique(buffer);
			}
		}

		return null;
	}

	package void skipRecv(u32 bytes)
	{
		import core.stdc.stdlib;
		ubyte* buf = cast(ubyte*) malloc(bytes);
		ubyte[] writeTo = buf[0 .. bytes];
		m_stream.read(writeTo);
		free(buf);
	}
}
