module psql.common;

import
	std.typetuple;

// Datatypes

alias u16 = ushort;
alias i16 = short;
alias u32 = uint;
alias i32 = int;
alias u64 = ulong;
alias i64 = long;

enum u16size = cast(u32) u16.sizeof;
enum u32size = cast(u32) u32.sizeof;

// UDA stuff

alias getUDAs(T) = TypeTuple!(__traits(getAttributes, T));

template choose(UDA, UDAs...)
{
	static if (UDAs.length == 0)
	{
		alias choose = TypeTuple!();
	}
	else
	{
		static if (is(UDAs[0] : UDA))
		{
			enum choose = UDAs[0].init;
		}
		else static if (is(typeof(UDAs[0]) : UDA))
		{
			enum choose = UDAs[0];
		}
		else
		{
			alias choose = choose!(UDA, UDAs[1 .. $]);
		}
	}
}

enum hasUDA(T, UDA) = is(typeof(choose!(UDA, getUDAs!T)));
enum hasMember(T, string memberName) = is(typeof(__traits(getMember, T, memberName)));

// Exceptions

class ProtocolException : Exception
{
  @safe pure nothrow
  this(string message, string file =__FILE__, size_t line = __LINE__, Throwable next = null)
  {
    super(message, file, line, next);
  }
}

class UnhandledMessageException : ProtocolException
{
  @safe pure nothrow
  this(string message = "unhandled message", string file =__FILE__, size_t line = __LINE__, Throwable next = null)
  {
    super(message, file, line, next);
  }
}

class UnexpectedMessageException : ProtocolException
{
  @safe pure nothrow
  this(string message = "unexpected message", string file =__FILE__, size_t line = __LINE__, Throwable next = null)
  {
    super(message, file, line, next);
  }
}

class EmptyQueryMessageException : ProtocolException
{
  @safe pure nothrow
  this(string message = "unexpected message", string file =__FILE__, size_t line = __LINE__, Throwable next = null)
  {
    super(message, file, line, next);
  }
}

class ErrorResponseException : ProtocolException
{
  @safe pure nothrow
  this(string message = "error response", string file =__FILE__, size_t line = __LINE__, Throwable next = null)
  {
    super(message, file, line, next);
  }
}