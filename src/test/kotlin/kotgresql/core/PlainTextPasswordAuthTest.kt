package kotgresql.core

import kotgresql.core.impl.KotgresqlClient
import kotlinx.coroutines.test.runTest
import org.hamcrest.CoreMatchers
import org.hamcrest.MatcherAssert
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer

class PlainTextPasswordAuthTest {
  companion object {
    private val postgreSQLContainer = PostgreSQLContainer("postgres:15.3")

    @JvmStatic
    @BeforeAll
    fun startup() {
      postgreSQLContainer.env = listOf("POSTGRES_HOST_AUTH_METHOD=password")
      postgreSQLContainer.start()
    }

    @JvmStatic
    @AfterAll
    fun shutdown() {
      postgreSQLContainer.close()
    }
  }

  private val postgresClient = KotgresqlClient(
    host = postgreSQLContainer.host,
    port = postgreSQLContainer.getMappedPort(postgreSQLContainer.exposedPorts[0]),
    username = postgreSQLContainer.username,
    password = postgreSQLContainer.password,
    databaseName = postgreSQLContainer.databaseName
  )

  @Test
  fun `should execute simple queries and return correct results`() {
    runTest {
      postgresClient.withConnection { connection ->
        try {
          MatcherAssert.assertThat(
            connection.executeUpdate("create table aij(id serial primary key, name varchar);"),
            CoreMatchers.equalTo(0)
          )
          MatcherAssert.assertThat(
            connection.executeUpdate("insert into aij(name) values ('Hello World');"),
            CoreMatchers.equalTo(1)
          )
        } finally {
          MatcherAssert.assertThat(connection.executeUpdate("drop table aij;"), CoreMatchers.equalTo(0))
        }
      }
    }
  }
}