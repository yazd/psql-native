module psql.oid.converters;

import
	psql.oid,
	psql.common;

@Oid(16, 1)
struct Bool
{
	bool value;
}

/*
@Oid(17, -1)
struct Bytea
{

}
*/

@Oid(18, 1)
struct Char
{
	ubyte value;
}

@Oid(19, 64)
struct Name
{
	char[64] value;
}

@Oid(20, 8)
struct Int8
{
	i64 value;
}

@Oid(21, 2)
struct Int2
{
	i16 value;
}

/*
@Oid(22, -1)
struct Int2Vector
{

}
*/

@Oid(23, 4)
struct Int4
{
	i32 value;
}

@Oid(25, -1)
struct Text
{
	string value;
}
