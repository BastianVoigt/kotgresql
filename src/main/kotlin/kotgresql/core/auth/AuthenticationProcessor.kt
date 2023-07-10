package kotgresql.core.auth

import kotgresql.core.NegotiateProtocolVersionMessage
import kotgresql.core.PostgresErrorResponseException
import kotgresql.core.ProtocolErrorException
import kotgresql.core.impl.BufferedConnection
import java.nio.charset.Charset
import java.sql.SQLException

class AuthenticationProcessor(
  private val bufferedConnection: BufferedConnection,
  private val password: String,
  private val username: String,
  private val encoding: Charset
) {
  private val scram = ScramSha256AuthenticationProcessor(password, encoding)

  suspend fun authenticate() {
    while (true) {
      val firstByte = bufferedConnection.get().toInt()
      val length = bufferedConnection.getInt()
      when (firstByte) {
        118 -> {
          // NegotiateProtocolVersion
          val negotiateProtocolVersionMessage =
            readNegotiateProtocolVersionMessage(bufferedConnection, length - 4)
          throw SQLException("Protocol version not supported by server: $negotiateProtocolVersionMessage")
        }

        69 -> {
          // ErrorResponse
          val errorMessage = bufferedConnection.readErrorResponse(length - 4, Charsets.UTF_8)
          throw PostgresErrorResponseException(errorMessage)
        }

        82 -> {
          // Auth...
          handleAuthMessage(bufferedConnection, length - 4)
          return
        }

        else -> {
          throw ProtocolErrorException("Message type $firstByte not allowed in this context")
        }
      }
    }
  }

  private suspend fun readNegotiateProtocolVersionMessage(
    bufferedConnection: BufferedConnection,
    length: Int
  ): NegotiateProtocolVersionMessage {
    var remainingLength = length
    val newestMinorVersion = bufferedConnection.getInt()
    val numProtocolOptions = bufferedConnection.getInt()
    remainingLength -= 8
    val protocolOptions = mutableListOf<String>()
    for (i in 0 until numProtocolOptions) {
      val (value, valueLen) = bufferedConnection.readNullTerminatedString(remainingLength, Charsets.UTF_8)
      remainingLength -= valueLen
      protocolOptions.add(value)
      if (remainingLength <= 0) {
        throw ProtocolErrorException("Message not terminated correctly")
      }
    }
    if (remainingLength != 0) {
      throw ProtocolErrorException("Message not terminated correctly")
    }
    return NegotiateProtocolVersionMessage(newestMinorVersion, protocolOptions)
  }

  private suspend fun handleAuthMessage(bufferedConnection: BufferedConnection, length: Int) {
    var remainingLength = length
    val authType = bufferedConnection.getInt()
    remainingLength -= 4
    when (authType) {
      0 -> {
        // AuthenticationOk
        return
      }

      3 -> {
        // AuthenticationCleartextPassword
        sendPasswordMessage(password)
        authenticate()
      }

      5 -> {
        // AuthenticationMD5Password
        val salt = ByteArray(4)
        bufferedConnection.get(salt)
        remainingLength -= 4
        val hash = MD5AuthenticationProcessor(username, password, encoding).createClientFirstMessage(salt)
        sendPasswordMessage(hash)
        authenticate()
      }

      10 -> {
        // AuthenticationSASL
        val authenticationMechanisms = mutableListOf<String>()
        while (remainingLength > 1) {
          val (value, len) = bufferedConnection.readNullTerminatedString(remainingLength, encoding)
          authenticationMechanisms.add(value)
          remainingLength -= len
        }
        if (remainingLength == 1) {
          bufferedConnection.get()
        }
        if (!authenticationMechanisms.contains("SCRAM-SHA-256")) {
          throw ProtocolErrorException(
            "None of the SASL auth mechanisms suggested by the server are supported: $authenticationMechanisms"
          )
        }
        sendSaslInitialResponse("SCRAM-SHA-256")
        authenticate()
      }

      11 -> {
        // AuthenticationSASLContinue
        val saslData = ByteArray(remainingLength)
        bufferedConnection.get(saslData)
        val clientFinalMessage = scram.createClientFinalMessage(String(saslData))
        sendSaslResponse(clientFinalMessage)
        authenticate()
      }

      12 -> {
        // AuthenticationSASLFinal
        if (remainingLength > 0) {
          val content = ByteArray(remainingLength)
          bufferedConnection.get(content)
        }
        authenticate()
      }

      else -> {
        throw ProtocolErrorException("Unknown authentication method $authType")
      }
    }
  }

  private suspend fun sendSaslResponse(saslData: String) {
    val saslBytes = saslData.toByteArray(encoding)
    bufferedConnection.prepareForSending()
    bufferedConnection.put(112)
    bufferedConnection.putInt(saslBytes.size + 4)
    bufferedConnection.put(saslBytes)
    bufferedConnection.flush()
  }

  private suspend fun sendSaslInitialResponse(authMechanism: String) {
    val initialResponse = scram.createClientFirstMessage()
    val initialResponseBytes = initialResponse.toByteArray(encoding)
    bufferedConnection.prepareForSending()
    bufferedConnection.put(112)
    val authMechanismBytes = authMechanism.toByteArray(encoding)
    bufferedConnection.putInt(4 + authMechanismBytes.size + 1 + 4 + initialResponseBytes.size)
    bufferedConnection.put(authMechanismBytes)
    bufferedConnection.put(0)
    bufferedConnection.putInt(initialResponseBytes.size)
    bufferedConnection.put(initialResponseBytes)
    bufferedConnection.flush()
  }

  private suspend fun sendPasswordMessage(secret: String) {
    bufferedConnection.prepareForSending()
    bufferedConnection.put(112)
    val secretBytes = secret.toByteArray(encoding)
    bufferedConnection.putInt(4 + secretBytes.size + 1)
    bufferedConnection.put(secretBytes)
    bufferedConnection.put(0)
    bufferedConnection.flush()
  }
}