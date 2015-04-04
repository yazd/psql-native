module psql.messages;

enum Backend : ubyte
{
	authenticationRequest = 'R',
	backendKeyData = 'K',
	bindComplete = '2',
	closeComplete = '3',
	commandComplete = 'C',
	copyData = 'd',
	copyDone = 'c',
	copyInResponse = 'G',
	copyOutResponse = 'H',
	copyBothResponse = 'W',
	dataRow = 'D',
	emptyQueryResponse = 'I',
	errorResponse = 'E',
	functionCallResponse = 'V',
	noData = 'n',
	noticeResponse = 'N',
	notificationResponse = 'A',
	parameterDescription = 't',
	parameterStatus = 'S',
	parseComplete = '1',
	portalSuspended = 's',
	readyForQuery = 'Z',
	rowDescription = 'T',
}

enum Frontend : ubyte
{
	bind = 'B',
	cancelRequest = 16,
	close = 'C',
	copyData = 'd',
	copyDone = 'c',
	copyFail = 'f',
	describe = 'D',
	execute = 'E',
	flush = 'H',
	functionCall = 'F',
	parse = 'P',
	passwordMessage = 'p',
	query = 'Q',
	sSLRequest = 8,
	// StartupMessage, // startup message doesn't have an identifier
	sync = 'S',
	terminate = 'X',
}
