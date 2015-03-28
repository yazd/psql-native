module psql.oid.tests;

import
	psql.oid;

static assert(getSize!(int, FieldRepresentation.binary)(0) == int.sizeof);
static assert(getSize!(int, FieldRepresentation.text)(0) == 1);
static assert(getSize!(string, FieldRepresentation.text)("hello") == 5);
static assert(getSize!(string, FieldRepresentation.binary)("hello") == 5);
