import std.stdio;
import psql;

void main()
{
	auto psql = new PSQL("codename", "yazan", "127.0.0.1", 5432);
	auto conn = psql.lockConnection();
	auto id   = 0;

	{
		writeln("QUERY ", ++id, ": ", "SELECT * FROM tbl_people");
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

	{
		writeln("QUERY ", ++id, ": ", "SELECT * FROM tbl_people; SELECT * FROM tbl_people");
		auto query = conn.query("SELECT * FROM tbl_people; SELECT * FROM tbl_people");

		foreach (row; query.rows())
		{
			foreach (i, field; query.fields())
			{
				writeln(field.name, ": ", cast(char[]) row.columns[i]);
			}
		}

		query.nextCommand();
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

	{
		writeln("QUERY ", ++id, ": ", "SELECT * FROM tbl_people");
		auto query = conn.query("SELECT * FROM tbl_people");

		foreach (person; query.fill!Person())
		{
			writeln(person);
		}

		query.close();
		writeln();
	}
}

struct Person
{
	int id;
	string name;
	string password;
	string email;
}
