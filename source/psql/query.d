module psql.query;

import
	psql.oid,
	psql.common,
	psql.connection;

debug import
	std.stdio;

struct Query
{
	private
	{
		Connection m_connection;
		Field[] m_fields;
	}

	this(Connection connection, const(char[]) simpleQuery)
	{
		m_connection = connection;

		// request
		{
			u32 msgLength = cast(u32) (
				u32.sizeof +
				simpleQuery.length + 1
			);

			with (m_connection)
			{
				send!ubyte('Q'); // query
				send(msgLength);
				sendz(simpleQuery);
				flush();
			}
		}

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

					case 'G': // CopyInResponse
						skipRecv(msgLength - u32size);
						// send CopyFail
						{
							send!ubyte('f');
							send!u32(u32size + 1);
							send!ubyte('\0');
						}

						throw new UnhandledMessageException();

					case 'H': // CopyOutResponse
						skipRecv(msgLength - u32size);
						throw new UnhandledMessageException();

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
						handleReadyForQuery(msgLength);
						break;

					case 'N': // NoticeResponse
						handleNoticeResponse(msgLength);
						break;

					default:
						throw new UnhandledMessageException();
				}
			}
		}
	}

	private void readRowDescription(u32 length)
	{
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

	auto rows()
	{
		return RowRange!Row(m_connection);
	}

	auto fill(RowType)()
	{
		return RowRange!RowType(m_connection, m_fields);
	}

	@property
	const(Field[]) fields() const
	{
		return m_fields;
	}
}

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

package struct RowRange(RowType)
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
		this(Connection connection)
		{
			m_connection = connection;
			popFront();
		}
	}
	else
	{
		this(Connection connection, const(Field[]) fields)
		{
			m_connection = connection;
			buildMapping(fields);
			popFront();
		}
	}

	@property
	bool empty()
	{
		return m_empty;
	}

	const(RowType) front() const
	{
		assert(!m_empty);
		return m_front;
	}

	void popFront()
	{
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

	private void readDataRow(u32 length)
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

struct Row
{
	alias Column = ubyte[];
	Column[] columns;
}

// this contains data on how to read a column and fill it
struct ColumnMap(RowType)
{
	void function(ref RowType row, Connection connection, u32 size) fill;
}
