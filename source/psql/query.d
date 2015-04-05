module psql.query;

import
	psql.common,
	psql.connection,
	psql.exceptions,
	psql.messages,
	psql.oid,
	psql.rows;

debug import
	std.stdio;

alias SimpleQueryResult = QueryResult!(true, FieldRepresentation.text);
alias PreparedQueryResult = QueryResult!(false, FieldRepresentation.binary);

/**
 * A QueryResult provides a way to do simple SQL commands without much setup.
 */
struct QueryResult(bool isSimple, FieldRepresentation representation)
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
						case Backend.commandComplete:
							skipRecv(msgLength - u32size);
							break;

						static if (!isSimple)
						{
							case Backend.bindComplete:
							goto case;
						}

						case Backend.dataRow:
						case Backend.emptyQueryResponse:
						case Backend.rowDescription:
							if (consumeAll)
							{
								skipRecv(msgLength - u32size);
								break;
							}
							else
							{
								goto default;
							}

						case Backend.errorResponse:
							handleErrorResponse(msgLength);
							break;

						case Backend.noticeResponse:
							handleNoticeResponse(msgLength);
							break;

						case Backend.readyForQuery:
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
					send!ubyte(Frontend.query);
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
			enum recvRep = representation;

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
					(Args.length > 0 ? u16.sizeof : 0) +
					u16.sizeof +
					i32.sizeof * Args.length +
					dataSize +
					u16.sizeof +
					i16.sizeof
				);

				with (m_connection)
				{
					send!ubyte(Frontend.bind);
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
					send!ubyte(Frontend.execute);
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
					send!ubyte(Frontend.describe);
					send(msgLength);
					send!ubyte('S'); // 'S' for prepared statement, 'P' for portal
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
					case Backend.commandComplete:
						skipRecv(msgLength - u32size);
						break wait;

					case Backend.parameterDescription:
						skipRecv(msgLength - u32size);
						break;

					case Backend.rowDescription:
						readRowDescription(msgLength);
						break wait;

					case Backend.bindComplete:
						skipRecv(msgLength - u32size);
						break;

					case Backend.dataRow:
						skipRecv(msgLength - u32size);
						throw new UnexpectedMessageException();

					case Backend.emptyQueryResponse:
						assert(msgLength == 4);
						throw new EmptyQueryMessageException();

					case Backend.errorResponse:
						handleErrorResponse(msgLength);
						break wait;

					case Backend.readyForQuery:
						throw new UnexpectedMessageException();
						//handleReadyForQuery(msgLength);
						//break wait;

					case Backend.noticeResponse:
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
		return RowRange!(Row, representation)(m_connection);
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
		return RowRange!(RowType, representation)(m_connection, m_fields);
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
