import std.exception;
import std.stdio;
import psql;

void main()
{
  auto psql = new PSQL("test", "yazan", "127.0.0.1", 5432);
  auto conn = psql.lockConnection();

  createTable(conn);
  fillTable(conn);
  readTable(conn);

  testGenericRowSelect(conn);
  testSimpleDelete(conn);
  //testSimpleInsert(conn);
  testTwoCommandsQuery(conn);
  testTypedRowSelect(conn);
  testHandleError(conn);
  testPreparedStatement(conn);
  testAnonPreparedStatement1(conn);
  testAnonPreparedStatement2(conn);
}

void createTable(Connection conn)
{
  conn.query(`
    DROP TABLE tbl_test;
    CREATE TABLE tbl_test (
      boolField bool,
      byteaField bytea,
      charField char,
/*    nameField name,               */
      int8Field int8,
      int2Field int2,
/*    int2vectorField int2vector,   */
      int4Field int4,
/*    regprocField regproc,         */
      textField text,
/*    oidField oid,                 */
/*    jsonField json,               */
/*    xmlField xml,                 */
      float4Field float4,
      float8Field float8
/*    jsonbField jsonb              */
    );
  `).close();
}

void fillTable(Connection conn)
{
  conn.prepare("insert_into_test", `
    INSERT INTO tbl_test
      (boolField, byteaField, charField, int8Field, int2Field, int4Field, textField, float4Field, float8Field)
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, $8, $9)
  `);

  conn.executePrepared("insert_into_test",
    true, cast(ubyte[])[1, 2, 3], ubyte('c'), long(42), short(41), int(43), "hello world", float(24.0), double(42.0)
  ).close();
}

void readTable(Connection conn)
{
  auto result = conn.query(`SELECT * FROM tbl_test`);
  foreach (row; result.rows())
  {
    foreach (i, field; result.fields())
    {
      writeln(field.name, ": ", cast(char[]) row.columns[i]);
    }
  }
  result.close();

  result = conn.query(`SELECT * FROM tbl_test`);
  foreach (row; result.fill!TestS())
  {
    writeln(row);
  }
  result.close();
}

void testGenericRowSelect(Connection conn)
{
  auto query = conn.query("SELECT * FROM tbl_test");
  foreach (row; query.rows())
  {
    foreach (i, field; query.fields())
    {
      writeln(field.name, ": ", cast(char[]) row.columns[i]);
    }
  }
  query.close();
}

void testTwoCommandsQuery(Connection conn)
{
  auto query = conn.query("SELECT * FROM tbl_test; SELECT * FROM tbl_test");

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
}

void testTypedRowSelect(Connection conn)
{
  auto query = conn.query("SELECT * FROM tbl_test");
  foreach (person; query.fill!TestS())
  {
    writeln(person);
  }
  query.close();
}

//void testSimpleInsert(Connection conn)
//{

//  auto query = conn.query("INSERT INTO tbl_test (name, password, email) VALUES ('test', '123', 'email@email.com')");
//  query.close();

//  writeln();
//}

void testSimpleDelete(Connection conn)
{
  auto query = conn.query("DELETE FROM tbl_test WHERE false");
  query.close();
}

void testHandleError(Connection conn)
{

  auto exception = collectException!ErrorResponseException(() {
    // unique constraint problem
    auto query = conn.query("INSERT INTO tbl_test (foo, bar) VALUES ('test', 'error')");
    query.close();
  }());

  assert(exception);
  assert(exception.message.length > 0);

  testGenericRowSelect(conn);
}

void testPreparedStatement(Connection conn)
{
  conn.prepare("prep_stmt_test", "SELECT * FROM tbl_test WHERE int4Field = $1");
  auto result = conn.executePrepared("prep_stmt_test", 42);
  foreach (test; result.fill!TestS())
  {
    writeln(test);
  }

  result.close();
}

void testAnonPreparedStatement1(Connection conn)
{
  auto result = conn.execute("SELECT * FROM tbl_test WHERE int4Field = $1", 42);
  foreach (test; result.fill!TestS())
  {
    writeln(test);
  }
  result.close();
}

void testAnonPreparedStatement2(Connection conn)
{
  auto result = conn.execute("SELECT * FROM tbl_test LIMIT 2");
  foreach (test; result.fill!TestS())
  {
    writeln(test);
  }
  result.close();
}

struct TestS
{
  bool boolfield;
  ubyte[] byteafield;
  ubyte charfield;
  long int8field;
  short int2field;
  int int4field;
  string textfield;
  float float4field;
  double float8field;
}
