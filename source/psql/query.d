module psql.query;

import
	psql.common,
	psql.connection;

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
				// wait for RowDescription/ReadyForQuery message or error
				response = recv!ubyte();
				msgLength = recv!u32();

				switch (response)
				{
					case 'C': // CommandComplete
						skipRecv(msgLength - u32size);
						break;

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
						break wait;

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

	@property
	const(Field[]) fields() const
	{
		return m_fields;
	}
}

enum FieldRepresentation : u16
{
	text = 0, binary = 1,
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
