import std.exception;
import std.stdio;
import psql;

debug(DoLog)
{
	alias log = writeln;
}
else
{
	void log(Args...)(Args args) {}
}

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
	testPreparedStatement(conn);
}

void testGenericRowSelect(Connection conn)
{
	log("QUERY: ", "SELECT * FROM tbl_people");
	auto query = conn.query("SELECT * FROM tbl_people");

	foreach (row; query.rows())
	{
		foreach (i, field; query.fields())
		{
			log(field.name, ": ", cast(char[]) row.columns[i]);
		}
	}

	query.close();
	log();
}

void testTwoCommandsQuery(Connection conn)
{
	log("QUERY: ", "SELECT * FROM tbl_people; SELECT * FROM tbl_people");
	auto query = conn.query("SELECT * FROM tbl_people; SELECT * FROM tbl_people");

	foreach (row; query.rows())
	{
		foreach (i, field; query.fields())
		{
			log(field.name, ": ", cast(char[]) row.columns[i]);
		}
	}

	foreach (row; query.rows())
	{
		foreach (i, field; query.fields())
		{
			log(field.name, ": ", cast(char[]) row.columns[i]);
		}
	}

	query.close();
	log();
}

void testTypedRowSelect(Connection conn)
{
	log("QUERY: ", "SELECT * FROM tbl_people");
	auto query = conn.query("SELECT * FROM tbl_people");

	foreach (person; query.fill!Person())
	{
		log(person);
	}

	query.close();
	log();
}

void testSimpleInsert(Connection conn)
{
	log("QUERY: ", "INSERT INTO tbl_people (name, password, email) VALUES ('test', '123', 'email@email.com')");

	auto query = conn.query("INSERT INTO tbl_people (name, password, email) VALUES ('test', '123', 'email@email.com')");
	query.close();

	log();
}

void testSimpleDelete(Connection conn)
{
	log("QUERY: ", "DELETE FROM tbl_people WHERE name = 'test'");

	auto query = conn.query("DELETE FROM tbl_people WHERE name = 'test'");
	query.close();

	log();
}

void testHandleError(Connection conn)
{
	log("QUERY: ", "INSERT INTO tbl_people (name, password, email) VALUES ('test', '123', 'email@email.com')");

	auto exception = collectException!ErrorResponseException(() {
		// unique constraint problem
		auto query = conn.query("INSERT INTO tbl_people (name, password, email) VALUES ('test', '123', 'email@email.com')");
		query.close();
	}());

	assert(exception);
	assert(exception.message.length > 0);
	assert(exception.detail.length > 0);

	testGenericRowSelect(conn);
}

void testPreparedStatement(Connection conn)
{
	log("QUERY: ", "SELECT * FROM tbl_people");

	conn.prepare("get_all_people", "SELECT * FROM tbl_people WHERE name = $1");

	auto result = conn.execute("get_all_people", "Yazan Dabain");
	foreach (person; result.fill!Person())
	{
		log(person);
	}

	result.close();

	log();
}

struct Person
{
	int id;
	string name;
	string password;
	string email;
}
