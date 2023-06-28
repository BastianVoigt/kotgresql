package kotgresql.core

interface KotgresqlRow {
  fun getInt(index: Int): Int?
  fun getString(index: Int): String?
  fun getLong(index: Int): Long?
  fun getBoolean(index: Int): Boolean?
  fun toArray(): Array<Any?>
}