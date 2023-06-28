package kotgresql.core

import kotlinx.coroutines.runBlocking
import org.hamcrest.CoreMatchers
import org.hamcrest.MatcherAssert
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer

class Postgres12Test {
  companion object {
    private val postgreSQLContainer = PostgreSQLContainer("postgres:12.11")
    @JvmStatic
    @BeforeAll
    fun startup() {
      postgreSQLContainer.env = listOf(
        "POSTGRES_HOST_AUTH_METHOD=md5"
      )
      postgreSQLContainer.start()
    }

    @JvmStatic
    @AfterAll
    fun shutdown() {
      postgreSQLContainer.close()
    }
  }

  private val postgresClient = PostgresClient(
    host = postgreSQLContainer.host,
    port = postgreSQLContainer.getMappedPort(postgreSQLContainer.exposedPorts[0]),
    username = postgreSQLContainer.username,
    password = postgreSQLContainer.password,
    databaseName = postgreSQLContainer.databaseName
  )

  @Test
  fun `should execute simple queries and return correct results`() {
    runBlocking {
      postgresClient.connect().use { connection ->
        try {
          MatcherAssert.assertThat(
            connection.executeUpdate("create table aij(id serial primary key, name varchar);"),
            CoreMatchers.equalTo(0)
          )
          MatcherAssert.assertThat(
            connection.executeUpdate("insert into aij(name) values ('Hello World');"),
            CoreMatchers.equalTo(1)
          )
          MatcherAssert.assertThat(
            connection.executeUpdate("insert into aij(name) values ('Bye bye!');"),
            CoreMatchers.equalTo(1)
          )
          connection.executeQuery("select count(*) from aij where name='Hello World';")
            .use { resultSet ->
              MatcherAssert.assertThat(
                resultSet.map {
                  it.getLong(0)
                },
                CoreMatchers.equalTo(listOf(1L))
              )
            }
          MatcherAssert.assertThat(connection.executeUpdate("delete from aij;"), CoreMatchers.equalTo(2))
        } finally {
          MatcherAssert.assertThat(connection.executeUpdate("drop table aij;"), CoreMatchers.equalTo(0))
        }
      }
    }
  }

  @Test
  fun `should execute prepared statements with and without parameters`() {
    runBlocking {
      postgresClient.connect().use { connection ->
        try {
          connection.executeUpdate(
            """
                            create table aij(id serial primary key, name varchar);
                            insert into aij(name) values ('Hans Meiser');
                            insert into aij(name) values ('Brigitte Bardot');
                            insert into aij(name) values ('Sabine Leutheuser-Schnarrenberger');
                            insert into aij(name) values ('Popsipil');
                            insert into aij(name) values ('qwertz');
            """.trimIndent()
          )
          connection.prepareStatement("select id, name from aij where name=$1").use { stmt ->
            stmt.executeQuery("Popsipil").use { resultSet ->
              MatcherAssert.assertThat(
                resultSet.map { Pair(it.getInt(0), it.getString(1)) },
                CoreMatchers.equalTo(listOf(4 to "Popsipil"))
              )
            }
            stmt.executeQuery("Hans Meiser").use { resultSet ->
              MatcherAssert.assertThat(
                resultSet.map { Pair(it.getInt(0), it.getString(1)) },
                CoreMatchers.equalTo(listOf(1 to "Hans Meiser"))
              )
            }
          }
        } finally {
          connection.executeUpdate(
            "drop table aij;"
          )
        }
      }
    }
  }
}