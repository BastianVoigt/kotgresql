package kotgresql.migrations

import kotgresql.core.impl.PostgresClient
import kotgresql.core.impl.withConnection
import kotgresql.core.inTransaction
import kotgresql.core.singleResult
import java.time.Instant

class Migrator(
  private val postgresClient: PostgresClient,
  private val schema: String = "public"
) {
  suspend fun migrate(migrations: List<Migration>) {
    postgresClient.withConnection { connection ->
      println("Check exists")
      val exists =
        connection.prepareStatement("select count(*) from pg_tables where schemaname=$1 and tablename=$2").use { stmt ->
          stmt.executeQuery(schema, "kotgresql_migrations").singleResult {
            it.getLong(0)
          }
        }
      if (exists == 0L) {
        println("Table no exista")
        connection.executeUpdate(
          """
          create table $schema.kotgresql_migrations(
            version int primary key, 
            filename varchar not null, 
            hash varchar not null,
            applied timestamp not null
          )
          """
        )
        println("Table created")
      }
      connection.inTransaction {
        // Lock table exclusively
        println("Locking")
        connection.executeUpdate("lock table $schema.kotgresql_migrations in exclusive mode nowait")
        println("Finding out latest version")
        val latestAppliedVersion = connection.executeQuery("select max(version) from $schema.kotgresql_migrations")
          .singleResult { it.getInt(0) } ?: 0

        println("Latest applied version: $latestAppliedVersion")
        migrations
          .filter { it.schemaVersion > latestAppliedVersion }
          .sortedBy { it.schemaVersion }
          .forEach { migration ->
            println("Applying ${migration.schemaVersion}")
            connection.executeUpdate(migration.sql)
            connection.prepareStatement(
              "insert into $schema.kotgresql_migrations (version,filename,hash,applied) values ($1, $2, $3, $4)"
            )
              .use { stmt ->
                stmt.executeUpdate(
                  migration.schemaVersion,
                  migration.schemaVersion.toString(),
                  migration.hashCode(),
                  Instant.now()
                )
              }
          }
      }
    }
  }
}