import std.stdio;
import psql;

void main()
{
	auto psql = new PSQL("codename", "yazan", "127.0.0.1", 5432);
	auto conn = psql.lockConnection();
	auto query = Query(conn, "SELECT * FROM tbl_projects");
	writeln(query.fields);
}
