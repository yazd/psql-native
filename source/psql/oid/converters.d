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
  Type fromText(scope const(char[]) text)
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
  static
  bool fromText(scope const(char[]) text)
  {
    if (text == "t") return true;
    else if (text == "f") return false;
    assert(0, "invalid conversion: expected 't' or 'f'");
  }

  static
  void toText(Connection connection, bool value)
  {
    connection.send!u32(1);
    connection.send!ubyte(value ? 't' : 'f');
  }

  static
  i32 toTextSize(bool value)
  {
    return 1;
  }

  static
  void fromBinary(Connection connection, i32 size, ref bool field)
  {
    assert(size == 1);
    ubyte[1] buffer;
    connection.recv(buffer);
    field = buffer[0] == 't' ? true : false;
  }

  static
  void toBinary(Connection connection, bool value)
  {
    connection.send!u32(1);
    connection.send!ubyte(value ? 't' : 'f');
  }

  static
  i32 toBinarySize(bool value)
  {
    return 1;
  }
}

/// ditto
@Oid!(ubyte[])(17)
struct ByteaConverter
{
  static
  ubyte[] fromText(scope const(char[]) text)
  {
    if (text.length >= 2 && text[0] == '\\' && text[1] == 'x')
    {
      assert(text.length % 2 == 0, "expected even number of bytes");

      ubyte[] result = new ubyte[text.length / 2 - 1];
      for (int i = 2, k = 0; i < text.length; i += 2, k++)
      {
        import std.conv : to;
        result[k] = text[i .. i + 2].to!ubyte(16);
      }
      return result;
    }
    else
    {
      // TODO: implement escape format for bytea
      assert(0, "unimplemented text bytea conversion (possibly from escape format)");
    }
  }

  static
  void toText(Connection connection, ubyte[] value)
  {
    assert(0, "unimplemented text bytea conversion");
  }

  static
  i32 toTextSize(ubyte[] value)
  {
    assert(0, "unimplemented text bytea conversion");
  }

  static
  void fromBinary(Connection connection, i32 size, ref ubyte[] field)
  {
    field.length = size;
    connection.recv(field[0 .. $]);
  }

  static
  void toBinary(Connection connection, ubyte[] value)
  {
    assert(value.length <= i32.max, "too big byte array");
    connection.send!i32(cast(i32) value.length);
    connection.send(value);
  }

  static
  i32 toBinarySize(ubyte[] value)
  {
    assert(value.length <= i32.max, "too big byte array");
    return cast(i32) value.length;
  }
}

/// ditto
@Oid!ubyte(18)
struct CharConverter
{
  static
  ubyte fromText(scope const(char[]) text)
  {
    assert(text.length == 1);
    return cast(ubyte) text[0];
  }

  static
  void toText(Connection connection, ubyte value)
  {
    connection.send!u32(1);
    connection.send!ubyte(value);
  }

  static
  i32 toTextSize(ubyte value)
  {
    return 1;
  }

  static
  void fromBinary(Connection connection, i32 size, ref ubyte field)
  {
    assert(size == 1);
    connection.recv((&field)[0 .. 1]);
  }

  static
  void toBinary(Connection connection, ubyte value)
  {
    connection.send!u32(1);
    connection.send!ubyte(value);
  }

  static
  i32 toBinarySize(ubyte value)
  {
    return 1;
  }
}

/*
/// ditto
@Oid!(char[64])(19)
struct NameConverter
{
  mixin DefaultConverters!(char[64]);
}
*/

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

/// ditto
@Oid!u32(26)
struct OidConverter
{
  mixin DefaultConverters!u32;
}

/// ditto
@Oid!float(700)
struct FloatConverter
{
  mixin DefaultConverters!float;
}

/// ditto
@Oid!double(701)
struct DoubleConverter
{
  mixin DefaultConverters!double;
}