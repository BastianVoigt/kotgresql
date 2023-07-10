package kotgresql.core

import kotlin.reflect.KClass

interface KotgresResultSet : AutoCloseable {
  suspend fun hasNext(): Boolean
  fun next(): KotgresqlRow
  fun numFields(): Int
  fun fieldType(index: Int): KClass<*>
}

suspend inline fun <reified T> KotgresResultSet.map(rowMapper: (KotgresqlRow) -> T): List<T> {
  use {
    val result = mutableListOf<T>()
    while (hasNext()) {
      result.add(rowMapper(next()))
    }
    return result
  }
}

suspend inline fun <reified T> KotgresResultSet.singleResult(rowMapper: (KotgresqlRow) -> T): T? {
  use {
    var result: T? = null
    if (hasNext()) {
      result = rowMapper(next())
      if (hasNext()) {
        throw KotgresException("More than one result")
      }
    }
    return result
  }
}

suspend inline fun <reified T> KotgresResultSet.mapTo(): List<T> {
  use {
    val fieldTypes = IntRange(0, numFields() - 1)
      .map { i -> fieldType(i).java }.toTypedArray()
    val constructor = T::class.java.getConstructor(*fieldTypes)
    return map { row ->
      val data = row.toArray()
      constructor.newInstance(*data)
    }
  }
}