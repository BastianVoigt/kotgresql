package kodbc.postgresql

data class NegotiateProtocolVersionMessage(
    val newestMinorVersion: Int,
    val protocolOptionsNotRecognized: List<String>
)
