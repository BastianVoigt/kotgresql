package kotgresql.core

interface KotgresPreparedStatement : AutoCloseable {
  suspend fun executeQuery(vararg parameters: Any): KotgresResultSet
  suspend fun executeUpdate(vararg parameters: Any): Int
}