module psql.prepared_statement;

debug import std.stdio;

import
	psql.oid,
	psql.common,
	psql.connection,
	psql.exceptions,
	psql.rows;

/**
 * A PreparedStatement is used to execute the same SQL statement repeatedly with possibly different parameters.
 */
struct PreparedStatement
{
	private
	{
		Connection m_connection;
		string m_name;
		Field[] m_fields;
		bool m_done;
	}

	package
	this(Connection connection, string name)
	{
		m_connection = connection;
		m_name = name;
	}

	/**
	 * Puts the connection back in a state where it can send new queries.
	 *
	 * Params:
	 *	 consumeAll = if `consumeAll` is false, all data from the query must
	 *								already be consumed.
	 *                If `consumeAll` is true, all data left will be consumed
	 *								until the connection can be put back in a state where
	 *								it can send new queries.
	 */
	void close(bool consumeAll = false)
	{
		if (!m_done)
		{
			assert(m_connection.m_state == ConnectionState.inQuery);

			with (m_connection)
			{
				char response;
				u32 msgLength;

				wait:
				while (true)
				{
					// expect ReadyForQuery
					response = recv!ubyte();
					msgLength = recv!u32();

					switch (response)
					{
						case 'C': // CommandComplete
							skipRecv(msgLength - u32size);
							break;

						case 'D': // DataRow
						case 'I': // EmptyQueryMessage
						case 'T': // RowDescription
							if (consumeAll)
							{
								skipRecv(msgLength - u32size);
								break;
							}
							else
							{
								goto default;
							}

						case 'E': // ErrorResponse
							handleErrorResponse(msgLength);
							break;

						case 'N': // NoticeResponse
							handleNoticeResponse(msgLength);
							break;

						case 'Z': // ReadyForQuery
							handleReadyForQuery(msgLength);
							m_done = true;
							break wait;

						default:
							debug writeln("Unhandled message: ", response);
							skipRecv(msgLength - u32size);
							throw new UnhandledMessageException();
					}
				}
			}
		}
	}

	/**
	 * Prepares the SQL statement for binding and execution later
	 */
	void prepare(const(char[]) statement)
	{
		assert(m_connection.m_state == ConnectionState.readyForQuery);
		m_connection.m_state = ConnectionState.inQuery;

		// request
		{
			immutable u32 msgLength = cast(u32) (
				u32.sizeof +
				m_name.length + 1 +
				statement.length + 1 +
				u16.sizeof
			);

			with (m_connection)
			{
				send!ubyte('P'); // parse
				send(msgLength);
				sendz(m_name);
				sendz(statement);
				send!i16(0); // not going to prespecify types (at least for now)
				flush();
			}
		}

		sync();

		// response
		{
			with (m_connection)
			{
				char response;
				u32 msgLength;

				wait:
				while (true)
				{
					// wait for ParseComplete message or error
					response = recv!ubyte();
					msgLength = recv!u32();

					switch (response)
					{
						case '1': // ParseComplete
							skipRecv(msgLength - u32size);
							break;

						case 'Z': // ReadyForQuery
							handleReadyForQuery(msgLength);
							m_done = true;
							break wait;

						case 'I': // EmptyQueryMessage
							assert(msgLength == 4);
							throw new EmptyQueryMessageException();

						case 'E': // ErrorResponse
							handleErrorResponse(msgLength);
							break wait;

						case 'N': // NoticeResponse
							handleNoticeResponse(msgLength);
							break;

						default:
							skipRecv(msgLength - u32size);
							throw new UnhandledMessageException();
					}
				}
			}
		}
	}

	/**
	 * Syncs
	 */
	package
	void sync()
	{
		// request
		{
			immutable u32 msgLength = cast(u32) (
				i32.sizeof
			);

			with (m_connection)
			{
				send!ubyte('S'); // sync
				send(msgLength);
				flush();
			}
		}
	}

	/**
	 * Binds a prepared statement
	 */
 	void bind()
 	{
 		bind!()();
 	}

	void bind(Args...)(Args[] args)
	{
		enum portalName = "";
		enum sendRep = FieldRepresentation.binary;
		enum recvRep = FieldRepresentation.text;

		// request
		{
			immutable dataSize = () {
				u32 size = 0;
				foreach (immutable i, ref arg; args)
				{
					immutable ds = getSize!(Args[i], sendRep)(arg);
					if (ds > 0)
					{
						size += ds;
					}
				}
				return size;
			}();

			immutable u32 msgLength = cast(u32) (
				u32.sizeof +
				portalName.length + 1 +
				m_name.length + 1 +
				u16.sizeof +
				u16.sizeof * Args.length +
				u16.sizeof +
				i32.sizeof * Args.length +
				dataSize +
				u16.sizeof +
				i16.sizeof
			);

			with (m_connection)
			{
				send!ubyte('B'); // bind
				send(msgLength);
				sendz(portalName);
				sendz(m_name);
				static if (Args.length == 0)
				{
					send!i16(0);
				}
				else
				{
					send!i16(1);
					send!i16(sendRep);
				}
				send!i16(Args.length);

				foreach (immutable i, ref arg; args)
				{
					toBinaryRep!(Args[i], sendRep)(m_connection, arg);
				}

				send!i16(1);
				send!i16(recvRep);
				flush();
			}
		}

		sync();

		// response
		{
			with (m_connection)
			{
				char response;
				u32 msgLength;

				wait:
				while (true)
				{
					// wait for BindComplete message or error
					response = recv!ubyte();
					msgLength = recv!u32();

					switch (response)
					{
						case '2': // BindComplete
							skipRecv(msgLength - u32size);
							break;

						case 'Z': // ReadyForQuery
							handleReadyForQuery(msgLength);
							m_done = true;
							break wait;

						case 'E': // ErrorResponse
							handleErrorResponse(msgLength);
							break wait;

						case 'N': // NoticeResponse
							handleNoticeResponse(msgLength);
							break;

						default:
							skipRecv(msgLength - u32size);
							throw new UnhandledMessageException();
					}
				}
			}
		}
	}

	void execute(int maximumNumberOfRows = 0)
	{
		enum portalName = "";

		// request
		{
			immutable u32 msgLength = cast(u32) (
				i32.sizeof +
				portalName.length + 1 +
				i32.sizeof
			);

			with (m_connection)
			{
				send!ubyte('E'); // execute
				send(msgLength);
				sendz(portalName);
				send!i32(maximumNumberOfRows);
				flush();
			}
		}

		m_connection.m_state = ConnectionState.inQuery;
	}

	/**
	 * Reads the row description message and fills the fields array.
	 */
	private
	void readRowDescription(u32 length)
	{
		assert(m_connection.m_state == ConnectionState.inQuery);
		assert(length >= u16size);

		with (m_connection)
		{
			u16 nFields = recv!u16();

			if (nFields == 0) return;
			length -= u16size;

			m_fields = new Field[nFields];
			foreach (ref field; m_fields)
			{
				field.name = recv!string(length);
				field.tableObjectID = recv!u32();
				field.columnAttribute = recv!u16();
				field.typeObjectID = recv!u32();
				field.typeLength = recv!i16();
				field.typeModifier = recv!u32();
				field.representation = recv!u16();
			}
		}
	}

	/**
	 * Returns a generic `RowRange`, an input range, to provide the data returned by the server.
	 */
	auto rows()
	{
		return RowRange!Row(m_connection);
	}

	/**
	 * Returns a `RowRange`, an input range, that automatically fills types `RowType`
	 * to provide the data returned by the server.
	 *
	 * The mapping of the columns is done by name matching.
	 */
	auto fill(RowType)()
	{
		return RowRange!RowType(m_connection, m_fields);
	}

	/**
	 * Returns the fields description.
	 */
	@property
	const(Field[]) fields() const
	{
		return m_fields;
	}
}
