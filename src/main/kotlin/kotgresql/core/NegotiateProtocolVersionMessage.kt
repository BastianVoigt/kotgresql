package kotgresql.core

data class NegotiateProtocolVersionMessage(
  val newestMinorVersion: Int,
  val protocolOptionsNotRecognized: List<String>
)