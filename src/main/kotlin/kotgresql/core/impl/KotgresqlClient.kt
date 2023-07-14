package kotgresql.core.impl

import kotgresql.core.KotgresConnectionFactory
import kotgresql.core.auth.AuthenticationProcessor
import java.net.InetSocketAddress
import kotlin.text.Charsets.UTF_8

class KotgresqlClient(
  private val host: String,
  private val port: Int = 5432,
  private val username: String,
  private val password: String,
  private val databaseName: String,
  private val bufferSize: Int = 4096
) : KotgresConnectionFactory {
  override suspend fun connect(): PostgresConnection {
    val bufferedConnection = BufferedConnection(bufferSize)
    bufferedConnection.connect(InetSocketAddress(host, port))
    sendStartupMessage(bufferedConnection)
    AuthenticationProcessor(bufferedConnection, password, username, UTF_8).authenticate()
    val postgresConnection = PostgresConnection(bufferedConnection)
    postgresConnection.init()
    return postgresConnection
  }

  private suspend fun sendStartupMessage(bufferedConnection: BufferedConnection) {
    bufferedConnection.prepareForSending()
    val usernameBytes = username.toByteArray()
    val databaseNameBytes = databaseName.toByteArray()
    bufferedConnection.putInt(
      4 + // length
        4 + // protocol version number
        4 + 1 + // "user"
        usernameBytes.size + 1 +
        8 + 1 + // "database"
        databaseNameBytes.size + 1 +
        1
    )
    bufferedConnection.putInt(3 shl 16) // protocol version 3.0
    bufferedConnection.put("user".toByteArray())
    bufferedConnection.put(0)
    bufferedConnection.put(usernameBytes)
    bufferedConnection.put(0)
    bufferedConnection.put("database".toByteArray())
    bufferedConnection.put(0)
    bufferedConnection.put(databaseNameBytes)
    bufferedConnection.put(0)
    bufferedConnection.put(0)
    bufferedConnection.flush()
  }
}
