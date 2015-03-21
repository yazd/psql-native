module psql.oid;

import
	psql.oid.converters,
	psql.connection,
	psql.common;

debug import
	psql.oid.tests;

import
	std.typetuple;

/**
 * Field representation (text or binary).
 */
enum FieldRepresentation : u16
{
	text = 0, binary = 1,
}

/**
 * Postgres oid.
 *
 * Contains oid number and data type size.
 */
struct Oid { u32 number; i16 length; }

/**
 * Returns whether the `Thing` is considered an oid.
 */
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

/**
 * Alias to all OidConverters names as a TypeTuple.
 */
template oidConverterNames()
{
	private string[] getOidConvertersImpl() pure
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

	alias oidConverterNames = typeTuple!(getOidConvertersImpl());
}

/**
 * Alias to an OidConverter that handles the native type DataType.
 */
template getOidConverter(DataType)
{
	template filterFirstMatch(Converters...)
	{
		static if (Converters.length == 0)
		{
			alias filterFirstMatch = TypeTuple!();
		}
		else
		{
			alias OidConverter = TypeTuple!(__traits(getMember, psql.oid.converters, Converters[0]));
			static if (is(OidConverterType!OidConverter == DataType))
			{
				alias filterFirstMatch = OidConverter;
			}
			else
			{
				alias filterFirstMatch = filterFirstMatch!(Converters[1 .. $]);
			}
		}
	}

	alias getOidConverter = filterFirstMatch!(oidConverterNames!());
}

/**
 * Returns the native type that the oid converts to.
 */
alias OidConverterType(OidType) = typeof(__traits(getMember, OidType, "value"));

/**
 * Default implementation of a field's text representation conversion to native data type.
 */
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

/**
 * Default implementation of a field's text representation conversion to psql data type.
 */
template toTextRep(OidConverter) if (hasUDA!(OidConverter, Oid))
{
	static if (hasMember!(OidConverter, "toTextRep"))
	{
		alias toTextRep = OidConverter.toTextRep;
	}
	else
	{
		alias DataType = OidConverterType!OidConverter;
		void toTextRep(Connection connection, DataType value)
		{
			import std.conv : to;
			string str = value.to!string;
			connection.send(str.length);
			connection.send(str);
		}
	}
}

/**
 * Default implementation of a field's text representation conversion to psql size.
 */
template toTextRepSize(OidConverter) if (hasUDA!(OidConverter, Oid))
{
	static if (hasMember!(OidConverter, "toTextRepSize"))
	{
		alias toTextRepSize = OidConverter.toTextRepSize;
	}
	else
	{
		i32 toTextRepSize(OidConverterType!OidConverter value)
		{
			import std.conv : to;
			return cast(i32) (value.to!(char[]).length);
		}
	}
}

/**
 * Default implementation of a field's binary representation conversion to native data type.
 */
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

/**
 * Default implementation of a field's binary representation conversion to psql data type.
 */
template toBinaryRep(OidConverter) if (hasUDA!(OidConverter, Oid))
{
	static if (hasMember!(OidConverter, "toBinaryRep"))
	{
		alias toBinaryRep = OidConverter.toBinaryRep;
	}
	else
	{
		alias DataType = OidConverterType!OidConverter;
		static if (is(DataType : U[], U))
		{
			void toBinaryRep(Connection connection, DataType value)
			{
				connection.send!i32(getSize!(DataType, FieldRepresentation.binary)(value));
				connection.send!DataType(value);
			}
		}
		else
		{
			void toBinaryRep(Connection connection, DataType value)
			{
				connection.send!i32(getSize!(DataType, FieldRepresentation.binary)(value));
				connection.send!DataType(value);
			}
		}
	}
}

/**
 * Default implementation of a field's binary representation conversion to psql size.
 */
template toBinaryRepSize(OidConverter) if (hasUDA!(OidConverter, Oid))
{
	static if (hasMember!(OidConverter, "toBinaryRepSize"))
	{
		alias toBinaryRepSize = OidConverter.toBinaryRepSize;
	}
	else
	{
		alias DataType = OidConverterType!OidConverter;
		static if (is(DataType : U[], U))
		{
			i32 toBinaryRepSize(DataType value)
			{
				alias ElementType = typeof(value[0]);
				return cast(i32) (value.length * ElementType.sizeof);
			}
		}
		else
		{
			i32 toBinaryRepSize(DataType value)
			{
				return DataType.sizeof;
			}
		}
	}
}

/**
 * Returns the number of bytes required to send the passed-in argument in the representation provided.
 */
i32 getSize(DataType, FieldRepresentation representation)(DataType value)
{
	alias OidConverter = getOidConverter!DataType;

	static if (!is(OidConverter))
	{
		static assert(0, "unimplemented converter for " ~ DataType.stringof);
	}

	static if (representation == FieldRepresentation.text)
	{
		return toTextRepSize!OidConverter(value);
	}
	else static if (representation == FieldRepresentation.binary)
	{
		return toBinaryRepSize!OidConverter(value);
	}
	else
	{
		assert(0, "unimplemented representation");
	}
}

/**
 * Constructs a function to map a data row directly into a type `RowType`.
 */
void function(ref RowType row, Connection connection, u32 size) getMapFunction(RowType, string MemberName, FieldRepresentation representation)()
{
	alias ColumnType = typeof(__traits(getMember, RowType, MemberName));
	alias OidConverter = getOidConverter!ColumnType;

	static if (!is(OidConverter))
	{
		static assert(0, "unimplemented converter for " ~ ColumnType.stringof);
	}

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
		assert(0, "unimplemented binary representation");
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
