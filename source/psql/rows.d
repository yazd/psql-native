module psql.rows;

debug import std.stdio;

import
	psql.oid,
	psql.common,
	psql.connection,
	psql.exceptions,
	psql.messages;

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
struct RowRange(RowType, FieldRepresentation representation)
{
	private
	{
		enum isGenericRow = is(RowType == psql.query.Row);

		Connection m_connection;
		bool m_empty = false;
		RowType m_front;

		static if (!isGenericRow)
		{
			immutable(ColumnMap!RowType[]) m_mapping;
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
			m_mapping = cast(immutable) getMapping(fields);
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
					case Backend.dataRow:
						readDataRow(msgLength - u32size);
						break wait;

					case Backend.commandComplete:
						m_empty = true;
						skipRecv(msgLength - u32size);
						break wait;

					case Backend.errorResponse:
						m_empty = true;
						handleErrorResponse(msgLength);
						break wait;

					case Backend.noticeResponse:
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
					if (m_mapping[columnIndex] !is null)
					{
						m_mapping[columnIndex](m_front, m_connection, size);
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
		private ColumnMap!RowType[] getMapping(const(Field[]) fields)
		{
			auto mapping = new ColumnMap!RowType[fields.length];
			foreach (immutable i, const ref field; fields)
			{
				nameSwitch:
				switch (field.name)
				{
					foreach (dataMemberName; getDataMembers!RowType)
					{
						case dataMemberName:
							mapping[i] = getMapFunction!(RowType, dataMemberName, representation)();
							break nameSwitch;
					}

					default:
						mapping[i] = null;
				}
			}
			return mapping;
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
alias ColumnMap(RowType) = void function(ref RowType row, Connection connection, u32 size);
