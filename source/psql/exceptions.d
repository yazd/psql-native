module psql.exceptions;

class ProtocolException : Exception
{
  @safe pure nothrow
  this(string message, string file = __FILE__, size_t line = __LINE__, Throwable next = null)
  {
    super(message, file, line, next);
  }
}

class UnhandledMessageException : ProtocolException
{
  @safe pure nothrow
  this(string message = "unhandled message", string file = __FILE__, size_t line = __LINE__, Throwable next = null)
  {
    super(message, file, line, next);
  }
}

class UnexpectedMessageException : ProtocolException
{
  @safe pure nothrow
  this(string message = "unexpected message", string file = __FILE__, size_t line = __LINE__, Throwable next = null)
  {
    super(message, file, line, next);
  }
}

class EmptyQueryMessageException : ProtocolException
{
  @safe pure nothrow
  this(string message = "unexpected message", string file = __FILE__, size_t line = __LINE__, Throwable next = null)
  {
    super(message, file, line, next);
  }
}

class ErrorResponseException : ProtocolException
{
  package
  {
    string m_message; // always present
    string m_detail;
    string m_hint;
  }

  @safe pure nothrow
  this(string message = null, string detail = null, string hint = null, string file = __FILE__, size_t line = __LINE__, Throwable next = null)
  {
    m_message = message;
    m_detail = detail;
    m_hint = hint;

    super(getMessage(), file, line, next);
  }

  private
  string getMessage() pure @safe nothrow
  {
    string output = m_message;

    if (m_detail.length > 0)
    {
      output ~= "\n" ~ m_detail;
    }

    if (m_hint.length > 0)
    {
      output ~= "\n" ~ m_hint;
    }

    return output;
  }

  string message() const
  {
    return m_message;
  }

  string detail() const
  {
    return m_detail;
  }

  string hint() const
  {
    return m_hint;
  }
}
