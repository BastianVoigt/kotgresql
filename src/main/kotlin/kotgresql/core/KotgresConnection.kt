package kotgresql.core

interface KotgresConnection : AutoCloseable {
  suspend fun executeQuery(sql: String): KotgresResultSet
  suspend fun executeUpdate(sql: String): Int
  suspend fun prepareStatement(sql: String): KotgresPreparedStatement
}

suspend inline fun <reified T> KotgresConnection.inTransaction(handler: (KotgresConnection) -> T): T {
  executeUpdate("BEGIN")
  try {
    val result = handler(this)
    executeUpdate("COMMIT")
    return result
  } catch (e: Throwable) {
    try {
      executeUpdate("ROLLBACK")
    } catch (re: KotgresException) {
      throw KotgresException("Failed to rollback transaction (${re.message})", e)
    }
    throw KotgresException("Transaction rolled back", e)
  }
}