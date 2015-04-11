module psql.connection;

import
  psql.common,
  psql.exceptions,
  psql.messages,
  psql.query;

import
  vibe.core.net;

import
  std.conv,
  std.exception,
  std.traits;

/**
 * Postgres connection.
 *
 * The connection goes through different phases before it becomes ready for querying.
 * Firstly, a TCP connection is made. An authentication cycle follows.
 * The postgres server backend then takes some time to be setup.
 * After that is done, the connection should be ready to send queries.
 */
final class Connection
{
  package
  {
    TCPConnection m_connection;
    ConnectionState m_state;

    string m_db;
    string m_username;
    string m_host;
    ushort m_port;
  }

  static immutable u32 protocolVersion = 0x00030000;

  /**
   * Constructs a Connection using the parameters provided.
   */
  this(string database, string username, string host, ushort port)
  {
    m_state = ConnectionState.setup;
    m_db = database;
    m_username = username;
    m_host = host;
    m_port = port;
  }

  /**
   * Initiates a TCP connection to the server.
   */
  void connect()
  {
    m_state = ConnectionState.connecting;
    m_connection = connectTCP(m_host, m_port);
    m_state = ConnectionState.connected;
  }

  /**
   * Handles the authentication cycle.
   */
  void authenticate()
  {
    m_state = ConnectionState.authenticating;

    // request
    {
      static immutable userMsg = "user";
      static immutable databaseMsg = "database";
      static immutable encodingMsg = "client_encoding";
      static immutable utf8Msg = "UTF8";

      u32 msgLength = cast(u32) (
        u32.sizeof + u32.sizeof
        + userMsg.length + 1
        + m_username.length + 1
        + databaseMsg.length + 1
        + m_db.length + 1
        + encodingMsg.length + 1
        + utf8Msg.length + 1
        + 1
      );

      send(msgLength);
      send(protocolVersion);
      sendz(userMsg);
      sendz(m_username);
      sendz(databaseMsg);
      sendz(m_db);
      sendz(encodingMsg);
      sendz(utf8Msg);
      send!ubyte(0);
      flush();
    }

    // response
    {
      char response = recv!ubyte();
      u32 msgLength = recv!u32();

      if (response != Backend.authenticationRequest)
      {
        throw new UnexpectedMessageException();
      }

      u32 auth = recv!u32();
      enforce(auth == 0, "authentication failure");
      m_state = ConnectionState.authenticated;
    }
  }

  /**
   * Waits until the server backend is setup and ready.
   */
  void waitForSetup()
  {
    m_state = ConnectionState.backendSetup;

    wait:
    while (true)
    {
      // wait for ReadyForQuery message or error
      char response = recv!ubyte();
      u32 msgLength = recv!u32();

      switch (response)
      {
        case Backend.backendKeyData:
          skipRecv(msgLength - u32size);
          break;

        case Backend.parameterStatus:
          skipRecv(msgLength - u32size);
          break;

        case Backend.readyForQuery:
          auto transactionStatus = handleReadyForQuery(msgLength);
          assert(transactionStatus == TransactionState.idle);
          break wait;

        case Backend.errorResponse:
          handleErrorResponse(msgLength);
          throw new Exception("error response received");

        case Backend.noticeResponse:
          handleNoticeResponse(msgLength);
          break;

        default:
          m_state = ConnectionState.invalid;
          assert(0);
      }
    }
  }

  /**
   * Sends a SQL command to the server. Multiple commands separated by semicolons can also be sent.
   */
  SimpleQueryResult query(const(char[]) command)
  {
    assert(m_state == ConnectionState.readyForQuery);

    auto result = SimpleQueryResult(this);
    result.sendCommand(command);
    return result;
  }

  /**
   * Prepares a SQL statement
   */
  void prepare(const(char[]) statementName, const(char[]) statement)
  {
    assert(m_state == ConnectionState.readyForQuery);
    m_state = ConnectionState.inQuery;

    // request
    {
      immutable u32 msgLength = cast(u32) (
        u32.sizeof +
        statementName.length + 1 +
        statement.length + 1 +
        u16.sizeof
      );

      with (m_connection)
      {
        send!ubyte(Frontend.parse);
        send(msgLength);
        sendz(statementName);
        sendz(statement);
        send!i16(0); // not going to prespecify types (at least for now)
        flush();
      }
    }

    sync();

    // response
    {
      with (m_connection)
      {
        char response;
        u32 msgLength;

        wait:
        while (true)
        {
          // wait for ParseComplete message or error
          response = recv!ubyte();
          msgLength = recv!u32();

          switch (response)
          {
            case Backend.parseComplete:
              skipRecv(msgLength - u32size);
              break;

            case Backend.readyForQuery:
              handleReadyForQuery(msgLength);
              break wait;

            case Backend.emptyQueryResponse:
              assert(msgLength == 4);
              throw new EmptyQueryMessageException();

            case Backend.errorResponse:
              handleErrorResponse(msgLength);
              break wait;

            case Backend.noticeResponse:
              handleNoticeResponse(msgLength);
              break;

            default:
              skipRecv(msgLength - u32size);
              throw new UnhandledMessageException();
          }
        }
      }
    }
  }

  /**
   * Creates an unnamed prepared statement and executes it
   */
  PreparedQueryResult execute(Args...)(string statement, Args args)
  {
    prepare("", statement);

    PreparedQueryResult result = PreparedQueryResult(this);
    result.sendBind("", "", args);
    result.sendDescribe("");
    result.sendExecute("");
    sync();
    return result;
  }

  /**
   * Executes a prepared statement.
   */
  PreparedQueryResult executePrepared(Args...)(string statementName, Args args)
  {
    PreparedQueryResult result = PreparedQueryResult(this);
    result.sendBind(statementName, "", args);
    result.sendDescribe(statementName);
    result.sendExecute("");
    sync();
    return result;
  }

  /**
   * Syncs.
   */
  package
  void sync()
  {
    // request
    {
      immutable u32 msgLength = cast(u32) (
        i32.sizeof
      );

      with (m_connection)
      {
        send!ubyte(Frontend.sync);
        send(msgLength);
        flush();
      }
    }
  }

  /**
   * Changes the connection state to readyForQuery and reads transaction state.
   */
  package
  TransactionState handleReadyForQuery(u32 length)
  {
    m_state = ConnectionState.readyForQuery;
    assert(length == 5);
    char indicator = recv!ubyte();
    return indicator.to!TransactionState;
  }

  /**
   * Reads the notice message sent by the server and provides it using a callback.
   *
   * TODO: provide notice using a callback.
   */
  package
  void handleNoticeResponse(u32 length)
  {
    skipRecv(length - u32size);
    // if (m_onNotice) m_onNotice();
  }

  /**
   * Changes the connection state to invalid, attempts to restore state for more queries.
   * If successful, changes connection state accordingly and throws an ErrorResponseException.
   */
  package
  void handleErrorResponse(u32 length)
  {
    m_state = ConnectionState.invalid;

    string message, detail, hint;
    {
      u32 sizeLeft = length - u32size;
      while (sizeLeft > 0)
      {
        char type = recv!ubyte();
        sizeLeft -= 1;

        switch (type)
        {
          case 'M': // message
            message = recv!string(sizeLeft);
            break;

          case 'D': // detail
            detail = recv!string(sizeLeft);
            break;

          case 'H': // hint
            hint = recv!string(sizeLeft);
            break;

          case '\0':
            break;

          default:
            skipString(sizeLeft);
        }
      }
    }

    {
      // cleanup error state
      wait:
      while (true)
      {
        // wait for ReadyForQuery message or error
        char response = recv!ubyte();
        u32 msgLength = recv!u32();

        switch (response)
        {
          case Backend.readyForQuery:
            auto transactionStatus = handleReadyForQuery(msgLength);
            assert(transactionStatus == TransactionState.idle);
            m_state = ConnectionState.readyForQuery;
            break wait;

          case Backend.noticeResponse:
            handleNoticeResponse(msgLength);
            break;

          default: // unexpected message
            m_state = ConnectionState.invalid;
            assert(0); // TODO: handle this in a better way
        }
      }
    }

    throw new ErrorResponseException(message, detail, hint);
  }

  /**
   * Sends a bool on the connection.
   */
  package
  void send(T)(T value) if (is(T == bool))
  {
    send!ubyte(value ? 1 : 0);
  }

  /**
   * Sends a numeric value using the proper endianness on the connection.
   */
  package
  void send(T)(T value) if (isNumeric!T)
  {
    import std.bitmanip;
    m_connection.write(value.nativeToBigEndian());
  }

  /**
   * Sends a string on the connection. It is not zero-terminated.
   *
   * See_Also:
   *  sendz
   */
  package
  void send(T)(T value) if (isSomeString!T)
  {
    import std.string;
    m_connection.write(value.representation);
  }

  /**
   * Sends a byte array on the connection.
   */
  package
  void send(const ubyte[] value)
  {
    m_connection.write(value);
  }

  /**
   * Sends a zero-terminated string on the connection.
   *
   * See_Also:
   *  send
   */
  package
  void sendz(T)(T value) if (isSomeString!T)
  {
    import std.string;
    m_connection.write(value.representation);
    m_connection.write(['\0']);
  }

  /**
   * Flushes the connection.
   */
  package
  void flush()
  {
    m_connection.flush();
  }

  /**
   * Receives a numeric value and returns it as a native datatype.
   */
  package
  T recv(T)() if (isNumeric!T)
  {
    import std.bitmanip;
    ubyte[T.sizeof] buf;
    m_connection.read(buf);
    return bigEndianToNative!T(buf);
  }

  /**
   * Receives raw bytes and fills them in `buffer`.
   */
  package
  void recv(ubyte[] buffer)
  {
    m_connection.read(buffer);
  }

  /**
   * Receives a zero-terminated string with the specified `maxLength`.
   */
  package
  T recv(T)(ref u32 maxLength) if (isSomeString!T)
  {
    import std.algorithm : countUntil;

    // reads until \0 byte
    while (m_connection.dataAvailableForRead())
    {
      const(ubyte[]) availableData = m_connection.peek();

      auto strLength = availableData.countUntil('\0');
      if (strLength >= 0)
      {
        enforceEx!ProtocolException(strLength < maxLength);
        char[] buffer = new char[strLength];
        ubyte[] buf = (cast(ubyte*) buffer.ptr)[0 .. strLength];
        m_connection.read(buf); // read string
        skipRecv(1); // skip zero
        maxLength -= strLength + 1;
        return assumeUnique(buffer);
      }
    }

    return null;
  }

  /**
   * Skips data upto and including a zero byte changing `length`.
   */
  package
  void skipString(ref u32 length)
  {
    import std.algorithm : countUntil;
    assert(length > 0, "length must be > 0");

    // reads until \0 byte
    while (m_connection.dataAvailableForRead())
    {
      const(ubyte[]) availableData = m_connection.peek();

      auto strLength = availableData.countUntil('\0');
      if (strLength >= 0)
      {
        skipRecv(cast(u32) (strLength + 1));
        length -= strLength + 1;
        return;
      }
    }

    return;
  }

  /**
   * Reads and skips the next `bytes` bytes on the connection.
   */
  package
  void skipRecv(u32 bytes)
  {
    ubyte[32] buf;
    while (bytes > 0)
    {
      u32 l = bytes > buf.length ? buf.length : bytes;
      m_connection.read(buf[0 .. l]);
      bytes -= l;
    }
  }
}

/**
 * Connection state
 */
enum ConnectionState
{
  setup, connecting, connected, authenticating, authenticated, backendSetup, readyForQuery, inQuery, closing, invalid,
}

/**
 * Transaction state
 */
enum TransactionState
{
  idle = 'I', inBlock = 'T', inFailedBlock = 'E'
}
