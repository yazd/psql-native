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
struct Oid(NativeType)
{
	alias Type = NativeType;
	u32 number;
}

/**
 * Returns whether the `Thing` is considered an oid converter.
 */
template isOidConverter(alias Thing)
{
	static if ((!is(Thing == struct) && !is(Thing == class)) || is(Thing == InvalidConverter))
	{
		enum isOidConverter = false;
	}
	else
	{
		enum isOidConverter = hasUDA!(Thing, Oid);
	}
}

/**
 * Returns the `Oid` attached to `Thing`.
 */
enum getOid(alias Thing) = choose!(Oid, getUDAs!Thing);

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
			alias filterFirstMatch = InvalidConverter;
		}
		else
		{
			alias OidConverter = TypeTuple!(__traits(getMember, psql.oid.converters, Converters[0]));
			enum oid = getOid!OidConverter;

			static if (isOidConverter!OidConverter && is(oid.Type == DataType))
			{
				alias filterFirstMatch = OidConverter[0];
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
 * Returns the number of bytes required to send the passed-in argument in the representation provided.
 */
i32 getSize(DataType, FieldRepresentation representation)(DataType value)
{
	alias OidConverter = getOidConverter!DataType;

	static if (!isOidConverter!OidConverter)
	{
		static assert(0, "unimplemented converter for " ~ DataType.stringof);
	}
	else static if (representation == FieldRepresentation.text)
	{
		return OidConverter.toTextSize(value);
	}
	else static if (representation == FieldRepresentation.binary)
	{
		return OidConverter.toBinarySize(value);
	}
	else
	{
		assert(0, "unimplemented representation");
	}
}

void toPostgres(DataType, FieldRepresentation representation)(Connection connection, DataType value)
{
	alias OidConverter = getOidConverter!DataType;

	static if (!isOidConverter!OidConverter)
	{
		static assert(0, "unimplemented converter for " ~ DataType.stringof);
	}
	else static if (representation == FieldRepresentation.text)
	{
		alias conversionFunction = OidConverter.toText;
	}
	else static if (representation == FieldRepresentation.binary)
	{
		alias conversionFunction = OidConverter.toBinary;
	}
	else
	{
		static assert(0, "unimplemented representation");
	}

	conversionFunction(connection, value);
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
					__traits(getMember, row, MemberName) = OidConverter.fromText((cast(char[]) buffer));
				}
				else
				{
					buffer = new ubyte[size];
					connection.recv(buffer);
					__traits(getMember, row, MemberName) = OidConverter.fromText((cast(char[]) buffer));
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
			alias func = OidConverter.fromBinary;
			func(connection, size, __traits(getMember, row, MemberName));
		}
		return &func;
	}
	else
	{
		assert(0, "unimplemented representation");
	}
}
