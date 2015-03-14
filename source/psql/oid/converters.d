module psql.oid.converters;

import
	psql.oid,
	psql.common;

/**
 * List of standard postgres oids.
 */

@Oid(16, 1)
struct Bool
{
	bool value;
}

/*
/// ditto
@Oid(17, -1)
struct Bytea
{

}
*/

/// ditto
@Oid(18, 1)
struct Char
{
	ubyte value;
}

/// ditto
@Oid(19, 64)
struct Name
{
	char[64] value;
}

/// ditto
@Oid(20, 8)
struct Int8
{
	i64 value;
}

/// ditto
@Oid(21, 2)
struct Int2
{
	i16 value;
}

/*
/// ditto
@Oid(22, -1)
struct Int2Vector
{

}
*/

/// ditto
@Oid(23, 4)
struct Int4
{
	i32 value;
}

/// ditto
@Oid(25, -1)
struct Text
{
	string value;
}
