package kotgresql.core

interface KotgresConnection : AutoCloseable {
  suspend fun executeQuery(sql: String): KotgresResultSet
  suspend fun executeUpdate(sql: String): Int
  suspend fun prepareStatement(sql: String): KotgresPreparedStatement
}