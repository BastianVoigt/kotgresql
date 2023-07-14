package kotgresql.core

import kotgresql.core.impl.KotgresqlClient
import kotgresql.core.impl.PostgresConnection

interface KotgresConnectionFactory {
  suspend fun connect(): KotgresConnection
}

suspend inline fun <reified T> KotgresqlClient.withConnection(handler: (PostgresConnection) -> T): T {
  connect().use { connection ->
    return handler(connection)
  }
}