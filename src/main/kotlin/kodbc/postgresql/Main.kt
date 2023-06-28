package kodbc.postgresql

import kotlinx.coroutines.runBlocking


suspend fun aij() {
    val postgresClient = PostgresClient()
    postgresClient.connect(
        host = "127.0.0.1", username = "postgres", databaseName = "test", password = "asd"
    )
    postgresClient.executeQuery("select * from aij;").use { resultSet ->
        resultSet.map {
            val id = it.getInt(0)
            val name = it.getString(1)
            println("$id, $name")
        }
    }
    postgresClient.executeQuery("select count(*) from aij;").use { resultSet ->
        while(resultSet.hasNext()) {
            println("Count ${resultSet.next().getInt(1)}")
        }
    }
}

fun main() {
    runBlocking {
        aij()
    }
}