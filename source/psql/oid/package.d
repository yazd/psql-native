module psql.oid;

import
	psql.oid.converters,
	psql.connection,
	psql.common;

import
	std.typetuple;

enum FieldRepresentation : u16
{
	text = 0, binary = 1,
}

struct Oid { u32 number; i16 length; }

template isOidConverter(alias Thing)
{
	static if (!is(Thing == struct) && !is(Thing == class))
	{
		enum isOidConverter = false;
	}
	else
	{
		enum isOidConverter = hasUDA!(Thing, Oid);
	}
}

alias OidConverterType(OidType) = typeof(__traits(getMember, OidType, "value"));

template fromTextRep(OidConverter) if (hasUDA!(OidConverter, Oid))
{
	static if (hasMember!(OidConverter, "fromTextRep"))
	{
		alias fromTextRep = OidConverter.fromTextRep;
	}
	else
	{
		OidConverterType!OidConverter fromTextRep(const(char[]) text)
		{
			import std.conv : to;
			return text.to!(OidConverterType!OidConverter);
		}
	}
}

template fromBinaryRep(OidConverter) if (hasUDA!(OidConverter, Oid))
{
	static if (hasMember!(OidConverter, "fromBinaryRep"))
	{
		alias fromBinaryRep = OidConverter.fromBinaryRep;
	}
	else
	{
		alias FieldType = OidConverterType!OidConverter;
		void fromBinaryRep(Connection connection, i32 size, ref FieldType field)
		{
			assert(size == FieldType.sizeof);
			ubyte[FieldType.sizeof] fieldBytes = (cast(ubyte*) field.ptr)[0 .. FieldType.sizeof];
			connection.recv(fieldBytes);

			import std.bitmanip;
			field = bigEndianToNative!FieldType(fieldBytes);
		}
	}
}

void function(ref RowType row, Connection connection, u32 size) getMapFunction(RowType, string MemberName, FieldRepresentation representation)()
{
	alias ColumnType = typeof(__traits(getMember, RowType, MemberName));

	static string[] getOidConverters()
	{
		alias members = TypeTuple!(__traits(allMembers, psql.oid.converters));

		string[] converters;
		foreach (memberName; members)
		{
			static if (isOidConverter!(__traits(getMember, psql.oid.converters, memberName)))
			{
				converters ~= memberName;
			}
		}
		return converters;
	}

	// foreach Oid struct, build function
	foreach (converter; typeTuple!(getOidConverters()))
	{
		alias OidConverter = TypeTuple!(__traits(getMember, psql.oid, converter));

		static if (is(OidConverterType!OidConverter == ColumnType))
		{
			static if (representation == FieldRepresentation.text)
			{
				static void func(ref RowType row, Connection connection, u32 size)
				{
					ubyte[64] stackBuffer;

					// TODO: double check memory allocation
					if (size > 0)
					{
						ubyte[] buffer;
						if (size <= stackBuffer.length)
						{
							buffer = stackBuffer[0 .. size];
							connection.recv(buffer);
							__traits(getMember, row, MemberName) = fromTextRep!OidConverter((cast(char[]) buffer));
						}
						else
						{
							buffer = new ubyte[size];
							connection.recv(buffer);
							__traits(getMember, row, MemberName) = fromTextRep!OidConverter((cast(char[]) buffer));
							destroy(buffer);
						}
					}
				}
				return &func;
			}
			else static if (representation == FieldRepresentation.binary)
			{
				static void func(ref RowType row, Connection connection, u32 size)
				{
					i32 size = connection.recv!i32();

					// TODO: double check memory allocation
					if (size > 0)
					{
						ubyte[] buffer;
						if (size <= stackBuffer)
						{
							buffer = stackBuffer[0 .. size];
							m_connection.recv(buffer);
							__traits(getMember, row, MemberName) = fromTextRep!OidConverter((cast(char[]) buffer));
						}
						else
						{
							buffer = new ubyte[size];
							m_connection.recv(buffer);
							__traits(getMember, row, MemberName) = fromTextRep!OidConverter((cast(char[]) buffer));
							destroy(buffer);
						}
					}
				}
				return &func;
			}
			else
			{
				assert(0, "unimplemented representation");
			}
		}
	}
	assert(0, "unimplemented converter for " ~ ColumnType.stringof);
}
