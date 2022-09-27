package nbd

// Ioctls
const (
	nbdSetSock       = 0xab00
	nbdSetBlkSize    = 0xab01
	nbdSetSize       = 0xab02
	nbdDoIt          = 0xab03
	nbdClearSock     = 0xab04
	nbdClearQue      = 0xab05
	nbdPrintDebug    = 0xab06
	nbdSetSizeBlocks = 0xab07
	nbdDisconnect    = 0xab08
	nbdSetTimeout    = 0xab09
	nbdSetFlags      = 0xab0a
)

// Negotiation magic
const (
	nbdMagic         = 0x4e42444d41474943
	nbdIHaveOpt      = 0x49484156454F5054
	nbdOptReplyMagic = 0x3e889045565a9
)

// Handshake flags
const (
	nbdFlagFixedNewstyle = 1 << 0
	nbdFlagsNoZeroes     = 1 << 1

	nbdFlagsCFixedNewstyle = 1 << 0
	nbdFlagCNoZeroes       = 1 << 1
)

// Transmission flags
const (
	nbdFlagHasFlags        = 1 << 0
	nbdFlagReadOnly        = 1 << 1
	nbdFlagSendFlush       = 1 << 2
	nbdFlagSendFua         = 1 << 3
	nbdFlagRotational      = 1 << 4
	nbdFlagSendTrim        = 1 << 5
	nbdFlagSendWriteZeroes = 1 << 6
	nbdFlagSendDf          = 1 << 7

	nbdClientFlagDestroyOnDisconnect = 1 << 0
	nbdClientFlagDisconnectOnClose   = 1 << 1
)

// Option types
const (
	nbdOptExportName      = 1
	nbdOptAbort           = 2
	nbdOptList            = 3
	nbdOptExport          = 4
	nbdOptStarttls        = 5
	nbdOptInfo            = 6
	nbdOptGo              = 7
	nbdOptStructuredReply = 8
)

// Option reply types
const (
	nbdRepAck    = 1
	nbdRepServer = 2
	nbdRepInfo   = 3

	nbdRepErrUnsup         = (1 << 31) + 1
	nbdRepErrPolicy        = (1 << 31) + 2
	nbdRepErrInvalid       = (1 << 31) + 3
	nbdRepErrPlatform      = (1 << 31) + 4
	nbdRepErrTlsReqd       = (1 << 31) + 5
	nbdRepErrUnknown       = (1 << 31) + 6
	nbdRepErrShutdown      = (1 << 31) + 7
	nbdRepErrBlockSizeReqd = (1 << 31) + 8
)

// Option info types
const (
	nbdInfoExport      = 0
	nbdInfoName        = 1
	nbdInfoDescription = 2
	nbdInfoBlockSize   = 3
)

// Command magic
const (
	nbdRequestMagic = 0x25609513
	nbdReplyMagic   = 0x67446698
)

// Command flags
const (
	nbdCmdFlagFua    = 1 << 0
	nbdCmdFlagNoHole = 1 << 1
	nbdCmdFlagDf     = 1 << 2
)

// Request types
const (
	nbdCmdRead        = 0
	nbdCmdWrite       = 1
	nbdCmdDisc        = 2
	nbdCmdFlush       = 3
	nbdCmdTrim        = 4
	nbdCmdCache       = 5
	nbdCmdWriteZeroes = 6
)

// Errors
const (
	nbdEperm     = 1   // Operation not permitted.
	nbdEio       = 5   // Input/output error.
	nbdEnomem    = 12  // Cannot allocate memory.
	nbdEinval    = 22  // Invalid argument.
	nbdEnospc    = 28  // No space left on device.
	nbdEoverflow = 75  // Defined in the experimental STRUCTURED_REPLY extension.
	nbdEshutdown = 108 // Server is in the process of being shut down.
)

// Netlink interface constants
const (
	nbdNlFamilyName = "nbd"
	nbdNlVersion    = 1

	nbdNlAttrIndex          = 1
	nbdNlAttrSizeBytes      = 2
	nbdNlAttrBlockSizeBytes = 3
	nbdNlAttrTimeout        = 4
	nbdNlAttrServerFlags    = 5
	nbdNlAttrClientFlags    = 6
	nbdNlAttrSockets        = 7

	nbdNlSockItem = 1

	nbdNlSockFd = 1

	nbdNlCmdConnect    = 1
	nbdNlCmdDisconnect = 2
)
