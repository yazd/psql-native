module psql.query;

import
	psql.common,
	psql.connection,
	psql.exceptions,
	psql.oid,
	psql.rows;

debug import
	std.stdio;

alias SimpleQueryResult = QueryResult!true;
alias PreparedQueryResult = QueryResult!false;

/**
 * A QueryResult provides a way to do simple SQL commands without much setup.
 */
struct QueryResult(bool isSimple)
{
	private
	{
		Connection m_connection;
		Field[] m_fields;
		bool m_done;
	}

	package
	this(Connection connection)
	{
		m_connection = connection;
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

						static if (!isSimple)
						{
							case '2': // BindCompletion
							goto case;
						}

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

	static if (isSimple)
	{
		/**
		 * Sends the SQL command/commands to the server.
		 */
		package
		void sendCommand(const(char[]) command)
		{
			assert(m_connection.m_state == ConnectionState.readyForQuery);
			m_connection.m_state = ConnectionState.inQuery;

			// request
			{
				immutable u32 msgLength = cast(u32) (
					u32.sizeof +
					command.length + 1
				);

				with (m_connection)
				{
					send!ubyte('Q'); // query
					send(msgLength);
					sendz(command);
					flush();
				}
			}
		}

		/**
		 * If multiple commands are sent, this function must be called
		 * to separate handling of the commands.
		 */
		void nextCommand()
		{
			handleCommand();
		}
	}
	else static if (!isSimple)
	{
		/**
		 * Binds a prepared statement
		 */
		void sendBind(Args...)(string statementName, string portalName, Args args)
		{
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
					statementName.length + 1 +
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
					sendz(statementName);
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
				}

				foreach (immutable i, ref arg; args)
				{
					toPostgres!(Args[i], sendRep)(m_connection, arg);
				}

				with (m_connection)
				{
					send!i16(1);
					send!i16(recvRep);
					flush();
				}
			}
		}

		/**
		 * Sends the execute message
		 */
		void sendExecute(string portalName, int maximumNumberOfRows = 0)
		{
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
		 * Sends the describe message
		 */
		void sendDescribe(string statementName)
		{
			// request
			{
				immutable u32 msgLength = cast(u32) (
					i32.sizeof +
					1 +
					statementName.length + 1
				);

				with (m_connection)
				{
					send!ubyte('D'); // describe
					send(msgLength);
					send!ubyte('S');
					sendz(statementName);
					flush();
				}
			}
		}
	}

	/**
	 * Handles the responses from the psql server.
	 */
	package
	void handleCommand()
	{
		assert(m_connection.m_state == ConnectionState.inQuery);
		m_fields = null;

		// response
		with (m_connection)
		{
			char response;
			u32 msgLength;

			wait:
			while (true)
			{
				// wait for RowDescription/CommandComplete message or error
				response = recv!ubyte();
				msgLength = recv!u32();

				switch (response)
				{
					case 'C': // CommandComplete
						skipRecv(msgLength - u32size);
						break wait;

					case 't': // ParameterDescription
						skipRecv(msgLength - u32size);
						break;

					case 'T': // RowDescription
						readRowDescription(msgLength);
						break wait;

					case '2': // BindComplete
						skipRecv(msgLength - u32size);
						break;

					case 'D': // DataRow
						skipRecv(msgLength - u32size);
						throw new UnexpectedMessageException();

					case 'I': // EmptyQueryMessage
						assert(msgLength == 4);
						throw new EmptyQueryMessageException();

					case 'E': // ErrorResponse
						handleErrorResponse(msgLength);
						break wait;

					case 'Z': // ReadyForQuery
						throw new UnexpectedMessageException();
						//handleReadyForQuery(msgLength);
						//break wait;

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
		handleCommand();
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
		handleCommand();
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
