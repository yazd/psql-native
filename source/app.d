import std.stdio;
import psql;

void main()
{
	auto psql = new PSQL("codename", "yazan", "127.0.0.1", 5432);
	auto conn = psql.lockConnection();
	auto query = Query(conn, "SELECT * FROM tbl_people");

	foreach (row; query.rows())
	{
		foreach (i, field; query.fields())
		{
			writeln(field.name, ": ", cast(char[]) row.columns[i]);
		}
	}

	query = Query(conn, "SELECT * FROM tbl_people");
	foreach (person; query.fill!Person())
	{
		writeln(person);
	}
}

struct Person
{
	int id;
	string name;
	string email;
	string password;
}
