package kotgresql.core

import kotgresql.core.impl.KotgresqlClient
import kotlinx.coroutines.test.runTest
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer

class Postgres15Test {
  companion object {
    private val postgreSQLContainer = PostgreSQLContainer("postgres:15.3")

    @JvmStatic
    @BeforeAll
    fun startup() {
      postgreSQLContainer.env = listOf("POSTGRES_HOST_AUTH_METHOD=scram-sha-256")
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
          assertThat(
            connection.executeUpdate("create table aij(id serial primary key, name varchar);"),
            equalTo(0)
          )
          assertThat(
            connection.executeUpdate("insert into aij(name) values ('Hello World');"),
            equalTo(1)
          )
          assertThat(
            connection.executeUpdate("insert into aij(name) values ('Bye bye!');"),
            equalTo(1)
          )
          connection.executeQuery("select count(*) from aij where name='Hello World'; ")
            .use { resultSet ->
              assertThat(
                resultSet.map {
                  it.getLong(0)
                },
                equalTo(listOf(1L))
              )
            }
          assertThat(connection.executeUpdate("delete from aij;"), equalTo(2))
        } finally {
          assertThat(connection.executeUpdate("drop table aij;"), equalTo(0))
        }
      }
    }
  }

  @Test
  fun `should execute prepared statements with and without parameters`() {
    runTest {
      postgresClient.withConnection { connection ->
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
              assertThat(
                resultSet.map { Pair(it.getInt(0), it.getString(1)) },
                equalTo(listOf(4 to "Popsipil"))
              )
            }
            stmt.executeQuery("Hans Meiser").use { resultSet ->
              assertThat(
                resultSet.map { Pair(it.getInt(0), it.getString(1)) },
                equalTo(listOf(1 to "Hans Meiser"))
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

  @Test
  fun `should execute prepared update statement without results`() {
    runTest {
      postgresClient.withConnection { connection ->
        try {
          connection.executeUpdate(
            """
              create table aij(id serial primary key, name varchar);
              insert into aij(name) values ('Hans Meiser');
            """.trimIndent()
          )
          connection.prepareStatement("update aij set name=$1 where name=$2").use { stmt ->
            assertThat(
              stmt.executeUpdate("Popsipil", "Hans Meiser"),
              equalTo(1)
            )
            assertThat(
              stmt.executeUpdate("Noppes Pa", "Popsipil"),
              equalTo(1)
            )
          }
        } finally {
          connection.executeUpdate(
            "drop table aij;"
          )
        }
      }
    }
  }

  @Test
  fun `should return different data types and null values`() {
    runTest {
      postgresClient.withConnection { connection ->
        try {
          connection.executeUpdate(
            """
              create table aij(
                id serial primary key, 
                name varchar not null,
                age int,
                intelligence bigint,
                consent boolean not null
              );
              insert into aij(name,age,intelligence,consent) values ('Hans', 42, 12345678910, true);
              insert into aij(name, consent) values ('Mathilda', false);
            """.trimIndent()
          )
          connection.executeQuery(
            "select id, name, age, intelligence, consent from aij where id=1"
          ).use { resultSet ->
            assertThat(
              resultSet.map {
                listOf(it.getInt(0), it.getString(1), it.getInt(2), it.getLong(3), it.getBoolean(4))
              },
              equalTo(
                listOf(
                  listOf(1, "Hans", 42, 12345678910L, true)
                )
              )
            )
          }
          connection.executeQuery(
            "select id, name, age, intelligence, consent from aij where id=2"
          ).use { resultSet ->
            assertThat(
              resultSet.map {
                listOf(it.getInt(0), it.getString(1), it.getInt(2), it.getLong(3), it.getBoolean(4))
              },
              equalTo(
                listOf(
                  listOf(2, "Mathilda", null, null, false)
                )
              )
            )
          }
        } finally {
          connection.executeUpdate("drop table aij;")
        }
      }
    }
  }

  @Test
  fun `should return field types`() {
    runTest {
      postgresClient.withConnection { connection ->
        try {
          connection.executeUpdate(
            """
              create table aij(
                id serial primary key, 
                name varchar not null,
                age int,
                intelligence bigint,
                consent boolean not null,
                data jsonb not null
              );
              insert into aij(name,age,intelligence,consent,data) values ('Hans', 42, 12345678910, true, '{"Hello": "World"}'::jsonb);
            """.trimIndent()
          )
          connection.executeQuery("select * from aij").use { resultSet ->
            resultSet.map {
              it.getInt(0)
            }
            assertThat(resultSet.numFields(), equalTo(6))
            assertThat(resultSet.fieldType(0), equalTo(Int::class))
            assertThat(resultSet.fieldType(1), equalTo(String::class))
            assertThat(resultSet.fieldType(2), equalTo(Int::class))
            assertThat(resultSet.fieldType(3), equalTo(Long::class))
            assertThat(resultSet.fieldType(4), equalTo(Boolean::class))
            assertThat(resultSet.fieldType(5), equalTo(String::class))
          }
        } finally {
          try {
            connection.executeUpdate("drop table aij;")
          } catch (e: Exception) {
            e.printStackTrace()
          }
        }
      }
    }
  }

  @Test
  fun `should map query result to data class`() {
    data class MyData(
      val id: Int,
      val name: String,
      val age: Long
    )
    runTest {
      postgresClient.connect().use { connection ->
        try {
          connection.executeUpdate(
            """
              create table mytable(id serial primary key, name varchar not null, age bigint not null);
              insert into mytable(name,age) values ('Hans Meiser', 134567654);
              insert into mytable(name,age) values ('Sandra Maischberger', 34672634);
            """.trimIndent()
          )
          val result = connection.executeQuery("select id,name,age from mytable")
            .mapTo<MyData>()
          assertThat(
            result,
            equalTo(
              listOf(
                MyData(1, "Hans Meiser", 134567654L),
                MyData(2, "Sandra Maischberger", 34672634L),
              )
            )
          )
        } finally {
          try {
            connection.executeUpdate("drop table mytable")
          } catch (e: Exception) {
            e.printStackTrace()
          }
        }
      }
    }
  }

  private inline fun <reified T : Exception> assertThrows(handler: () -> Any): T {
    try {
      handler()
      throw AssertionError("Expected ${T::class.qualifiedName} was not thrown")
    } catch (exception: Throwable) {
      if (exception is T) {
        return exception
      }
      throw exception
    }
  }

  @Test
  fun `should rollback transaction when an exception is thrown`() {
    runTest {
      postgresClient.withConnection { connection ->
        connection.executeUpdate(
          """create table mytable(id serial primary key, name varchar not null, age bigint not null);"""
        )
        try {
          val exception = assertThrows<KotgresException> {
            connection.inTransaction {
              connection.executeUpdate("insert into mytable(name,age) values ('Bastian', 43)")
              throw Exception("Miserable failure")
            }
          }
          assertThat(exception.message, equalTo("Transaction rolled back"))
          assertThat(exception.cause?.message, equalTo("Miserable failure"))
          val count = connection.executeQuery("select count(*) from mytable").singleResult { it.getLong(0) }
          assertThat(count, equalTo(0))
        } finally {
          connection.executeUpdate("drop table mytable")
        }
      }
    }
  }
}