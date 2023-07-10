[![CircleCI](https://circleci.com/gh/BastianVoigt/kotgresql.svg?style=shield)](https://circleci.com/gh/BastianVoigt/kotgresql)

# KotgreSQL

A PostgreSQL client library written in pure Kotlin, made for use with Coroutines

## Why you should use it
### It's made for Kotlin and Coroutines

KotgreSQL's I/O is completely non-blocking (built with NIO) and suspend functions.
You can do all interactions with the database from Coroutines without ever 
blocking the thread.

### It's really easy to use

See the [examples](#examples) section below to get started

### It's small and simple
Look ma, no Netty, no Reactor framework, no Spring Data! Just plain NIO, 
no dependencies apart from kotlin-coroutines.
Only 127kB Jar File size (as of release 0.1).

### It's 100% PostgreSQL
While portable database frameworks are a great thing for management presentations, 
in practical applications it is much better to focus on the strenghts and features 
of one particular product, so you can use all its features exactly as intended 
without additional layers of bloat in between.

If you ever need to switch to a different DB, this will probably be a big migration
project anyway, regardless whether or not you use a (theoretically) portable 
Java framework.

This is why KotgreSQL is built and designed from the ground up for use with PostgreSQL only. 

## Features

* Supports PostgreSQL 12, 13, 14 and 15
* Ready for use with Amazon RDS for PostgreSQL
* Authentication methods: cleartext password, md5 and SCRAM-SHA256
* Simple queries and prepared statements

### On the roadmap

* Connection Pool (inspired by HikariCP)
* Migration tool (inspired by Flyway)

## Examples

### Create a client

```kotlin
val postgresClient = PostgresClient(
  host = "localhost", 
  database = "mydb", 
  username = "me", 
  password = "XXX"
)
```

### Simple query - map ResultSet to data class
```kotlin
 data class Bicycle(
     val id: Int,
     val name: String,
     val price: Int
 )

 postgresClient.withConnection { connection ->
     val bicycles = connection
         .executeQuery("select id,name,price from bicycles")
         .mapTo<Bicycle>()
 }
```

### Prepared statement with placeholders

```kotlin    
postgresClient.withConnection { connection ->
    connection.prepareStatement("""
        select id,name,price from bicycle 
        where name = $1 
        and price < $2
      """.trimIndent()
    ).use { stmt ->
        val bicycles = stmt.executeQuery("Topstone", 2000)
            .mapTo<Bicycle>()
    }
}
```

## FAQ

### What is KotgreSQL and why should I use it?

A Kotlin-native PostgreSQL client library made for use with Coroutines. You should use it when 
when you work with Coroutines and you want to avoid blocking the main thread, but you don't want
to use a big bloated framework such as Spring Data / R2DBC


### Can I use it with kotlin native?

No, it's made for the JVM platform only, as it relies heavily on NIO.