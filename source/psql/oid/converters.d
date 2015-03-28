module psql.oid.converters;

import
	psql.oid,
	psql.common,
	psql.connection;

/**
 * Default converters
 */
mixin template DefaultTextConverters(Type)
{
	/**
	 * Default implementation of a field's text representation conversion to native data type.
	 */
	static
	Type fromText(const(char[]) text)
	{
		import std.conv : to;
		return text.to!Type;
	}

	/**
	 * Default implementation of a field's text representation conversion to psql data type.
	 */
	static
	void toText(Connection connection, Type value)
	{
		import std.conv : to;
		string str = value.to!string;
		connection.send(str.length);
		connection.send(str);
	}

	/**
	 * Default implementation of a field's text representation conversion to psql size.
	 */
	static
	i32 toTextSize(Type value)
	{
		import std.conv : to;
		return cast(i32) (value.to!(char[]).length);
	}
}

mixin template DefaultBinaryConverters(Type)
{
	/**
	 * Default implementation of a field's binary representation conversion to native data type.
	 */
	template fromBinary()
	{
		static if (is(Type : U[], U))
		{
		 	static
			void fromBinary(Connection connection, i32 size, ref Type field)
			{
				ubyte[] buffer = new ubyte[size];
				connection.recv(buffer);
				field = cast(Type) buffer;
			}
		}
		else
		{
		 	static
			void fromBinary(Connection connection, i32 size, ref Type field)
			{
				assert(size == Type.sizeof);
				ubyte[Type.sizeof] fieldBytes = (cast(ubyte*) &field)[0 .. Type.sizeof];
				connection.recv(fieldBytes);

				import std.bitmanip;
				field = bigEndianToNative!Type(fieldBytes);
			}
		}
	}


	/**
	 * Default implementation of a field's binary representation conversion to psql data type.
	 */
	template toBinary()
	{
		static if (is(Type : U[], U))
		{
			static
			void toBinary(Connection connection, Type value)
			{
				connection.send!i32(getSize!(Type, FieldRepresentation.binary)(value));
				connection.send!Type(value);
			}
		}
		else
		{
			static
			void toBinary(Connection connection, Type value)
			{
				connection.send!i32(getSize!(Type, FieldRepresentation.binary)(value));
				connection.send!Type(value);
			}
		}
	}

	/**
	 * Default implementation of a field's binary representation conversion to psql size.
	 */
	template toBinarySize()
	{
		static if (is(Type : U[], U))
		{
			static
			i32 toBinarySize(Type value)
			{
				alias ElementType = typeof(value[0]);
				return cast(i32) (value.length * ElementType.sizeof);
			}
		}
		else
		{
			static
			i32 toBinarySize(Type value)
			{
				return Type.sizeof;
			}
		}
	}
}

mixin template DefaultConverters(Type)
{
	mixin DefaultTextConverters!Type;
	mixin DefaultBinaryConverters!Type;
}

/**
 * Invalid converter, used to report errors
 */
@Oid!void(0)
struct InvalidConverter
{

}

/**
 * List of standard postgres oids.
 */
@Oid!bool(16)
struct BoolConverter
{
	mixin DefaultConverters!bool;
}

/*
/// ditto
@Oid(17, -1)
struct Bytea
{

}
*/

/// ditto
@Oid!ubyte(18)
struct CharConverter
{
	mixin DefaultConverters!ubyte;
}

/// ditto
//@Oid!(char[64])(19)
//struct NameConverter
//{
//	mixin DefaultConverters!(char[64]);
//}

/// ditto
@Oid!i64(20)
struct Int8Converter
{
	mixin DefaultConverters!i64;
}

/// ditto
@Oid!i16(21)
struct Int2Converter
{
	mixin DefaultConverters!i16;
}

/*
/// ditto
@Oid(22, -1)
struct Int2VectorConverter
{

}
*/

/// ditto
@Oid!i32(23)
struct Int4Converter
{
	mixin DefaultConverters!i32;
}

/// ditto
@Oid!(char[])(25)
struct TextConverter
{
	mixin DefaultConverters!(char[]);
}

/// ditto
@Oid!string(25)
struct TextStringConverter
{
	mixin DefaultConverters!string;
}
