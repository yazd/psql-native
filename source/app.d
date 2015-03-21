import std.stdio;
import std.exception;
import psql;

void main()
{
	auto psql = new PSQL("codename", "yazan", "127.0.0.1", 5432);
	auto conn = psql.lockConnection();

	testGenericRowSelect(conn);
	testSimpleDelete(conn);
	testSimpleInsert(conn);
	testTwoCommandsQuery(conn);
	testTypedRowSelect(conn);
	testHandleError(conn);
}

void testGenericRowSelect(Connection conn)
{
	writeln("QUERY: ", "SELECT * FROM tbl_people");
	auto query = conn.query("SELECT * FROM tbl_people");

	foreach (row; query.rows())
	{
		foreach (i, field; query.fields())
		{
			writeln(field.name, ": ", cast(char[]) row.columns[i]);
		}
	}

	query.close();
	writeln();
}

void testTwoCommandsQuery(Connection conn)
{
	writeln("QUERY: ", "SELECT * FROM tbl_people; SELECT * FROM tbl_people");
	auto query = conn.query("SELECT * FROM tbl_people; SELECT * FROM tbl_people");

	foreach (row; query.rows())
	{
		foreach (i, field; query.fields())
		{
			writeln(field.name, ": ", cast(char[]) row.columns[i]);
		}
	}

	foreach (row; query.rows())
	{
		foreach (i, field; query.fields())
		{
			writeln(field.name, ": ", cast(char[]) row.columns[i]);
		}
	}

	query.close();
	writeln();
}

void testTypedRowSelect(Connection conn)
{
	writeln("QUERY: ", "SELECT * FROM tbl_people");
	auto query = conn.query("SELECT * FROM tbl_people");

	foreach (person; query.fill!Person())
	{
		writeln(person);
	}

	query.close();
	writeln();
}

void testSimpleInsert(Connection conn)
{
	writeln("QUERY: ", "INSERT INTO tbl_people (name, password, email) VALUES ('test', '123', 'email@email.com')");

	auto query = conn.query("INSERT INTO tbl_people (name, password, email) VALUES ('test', '123', 'email@email.com')");
	query.close();

	writeln();
}

void testSimpleDelete(Connection conn)
{
	writeln("QUERY: ", "DELETE FROM tbl_people WHERE name = 'test'");

	auto query = conn.query("DELETE FROM tbl_people WHERE name = 'test'");
	query.close();

	writeln();
}

void testHandleError(Connection conn)
{
	writeln("QUERY: ", "INSERT INTO tbl_people (name, password, email) VALUES ('test', '123', 'email@email.com')");

	assertThrown!ErrorResponseException(() {
		// unique constraint problem
		auto query = conn.query("INSERT INTO tbl_people (name, password, email) VALUES ('test', '123', 'email@email.com')");
		query.close();
	}());

	testGenericRowSelect(conn);
}

void testPreparedStatement(Connection conn)
{
	writeln("QUERY: ", "SELECT * FROM tbl_people");

	auto stmt = conn.prepare("get_all_people", "SELECT * FROM tbl_people");
	stmt.bind();
	stmt.execute();

	foreach (row; stmt.rows())
	{
		//writeln(field.name, ": ", cast(char[]) row.columns[i]);
	}
}

struct Person
{
	int id;
	string name;
	string password;
	string email;
}
