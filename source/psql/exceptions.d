module psql.exceptions;

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
