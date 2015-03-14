module psql.query;

import
	psql.oid,
	psql.common,
	psql.connection;

debug import
	std.stdio;

/**
 * A SimpleQuery provides a way to do simple SQL commands without much setup.
 */
struct SimpleQuery
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
	 *								already be consumed and all command completion messages
	 *								be received.
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
						case 'D': // DataRow
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
							break wait;

						case 'Z': // ReadyForQuery
							handleReadyForQuery(msgLength);
							m_done = true;
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

					case 'T': // RowDescription
						readRowDescription(msgLength);
						break wait;

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

/**
 * Field description
 */
struct Field
{
	string name;
	u32 tableObjectID;
	u16 columnAttribute;
	u32 typeObjectID;
	i16 typeLength;
	u32 typeModifier;
	u16 representation;
}

/**
 * RowRange is an input range that provides the data returned from the server.
 */
package
struct RowRange(RowType)
{
	private
	{
		enum isGenericRow = is(RowType == psql.query.Row);

		Connection m_connection;
		bool m_empty = false;
		RowType m_front;

		static if (!isGenericRow)
		{
			ColumnMap!RowType[] m_mapping;
		}
	}

	static if (isGenericRow)
	{
		/**
		 * Constructs a `RowRange` from a connection and reads the first data row.
		 */
		package
		this(Connection connection)
		{
			m_connection = connection;
			popFront();
		}
	}
	else
	{
		/**
		 * Constructs a `RowRange` from a connection, builds field mapping and reads the first data row.
		 */
		package
		this(Connection connection, const(Field[]) fields)
		{
			m_connection = connection;
			buildMapping(fields);
			popFront();
		}
	}

	/**
	 * Input range primitve `empty`.
	 */
	@property
	bool empty()
	{
		return m_empty;
	}

	/**
	 * Input range primitve `front`. Returns a `RowType`.
	 */
	const(RowType) front() const
	{
		assert(!m_empty);
		return m_front;
	}

	/**
	 * Input range primitve `popFront`.
	 */
	void popFront()
	{
		assert(m_connection.m_state == ConnectionState.inQuery);
		assert(!m_empty);

		with (m_connection)
		{
			char response;
			u32 msgLength;

			wait:
			while (true)
			{
				// wait for DataRow/CommandComplete message or error
				response = recv!ubyte();
				msgLength = recv!u32();

				switch (response)
				{
					case 'D': // DataRow
						readDataRow(msgLength - u32size);
						break wait;

					case 'C': // CommandComplete
						m_empty = true;
						skipRecv(msgLength - u32size);
						break wait;

					case 'E': // ErrorResponse
						m_empty = true;
						handleErrorResponse(msgLength);
						break wait;

					case 'N': // NoticeResponse
						handleNoticeResponse(msgLength);
						break;

					default:
						debug writeln(response);
						throw new UnhandledMessageException();
				}
			}
		}
	}

	/**
	 * Implementation of data row parsing.
	 */
	private
	void readDataRow(u32 length)
	{
		assert(length >= u16size);
		u16 nCols = m_connection.recv!u16();

		m_front = RowType();
		if (nCols == 0) return;

		static if (isGenericRow)
		{
			m_front.columns = new Row.Column[nCols];
			foreach (ref column; m_front.columns)
			{
				i32 size = m_connection.recv!i32();

				if (size > 0)
				{
					column = new ubyte[size];
					m_connection.recv(column);
				}
			}
		}
		else
		{
			foreach (columnIndex; 0 .. nCols)
			{
				i32 size = m_connection.recv!i32();

				if (size > 0)
				{
					if (m_mapping[columnIndex].fill !is null)
					{
						m_mapping[columnIndex].fill(m_front, m_connection, size);
					}
					else
					{
						m_connection.skipRecv(size);
					}
				}
			}
		}
	}

	static if (!isGenericRow)
	{
		/**
		 * Builds the mapping from the fields to `RowType`.
		 */
		private void buildMapping(const(Field[]) fields)
		{
			m_mapping = new ColumnMap!RowType[fields.length];
			foreach (immutable i, const ref field; fields)
			{
				nameSwitch:
				switch (field.name)
				{
					foreach (dataMemberName; getDataMembers!RowType)
					{
						case dataMemberName:
							m_mapping[i].fill = getMapFunction!(RowType, dataMemberName, FieldRepresentation.text)();
							break nameSwitch;
					}

					default:
						m_mapping[i].fill = null;
				}
			}
		}
	}
}

/**
 * Generic row.
 * It is a list of columns.
 */
struct Row
{
	alias Column = ubyte[];
	Column[] columns;
}

/**
 * Mapping used to read from connection and fill `RowType` directly.
 */
private
struct ColumnMap(RowType)
{
	void function(ref RowType row, Connection connection, u32 size) fill;
}
