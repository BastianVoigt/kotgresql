package kotgresql.core.impl

import kotgresql.core.FieldDescription
import kotgresql.core.FormatCode
import kotgresql.core.KotgresConnection
import kotgresql.core.KotgresPreparedStatement
import kotgresql.core.KotgresResultSet
import kotgresql.core.KotgresqlRow
import kotgresql.core.PostgresErrorResponseException
import kotgresql.core.ProtocolErrorException
import java.nio.charset.Charset
import java.sql.SQLException
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.reflect.KClass

class PostgresConnection(
  val bufferedConnection: BufferedConnection
) : KotgresConnection {
  companion object {
    private val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
  }
  private var backendProcessId: Int? = null
  private var backendSecretKey: Int? = null
  var encoding: Charset = Charsets.UTF_8
  var readyForQuery = false
  private val parameters = mutableMapOf<String, String>()
  private var transactionStatus: Int = 0

  suspend fun init() {
    receiveMessages { firstByte, length ->
      when (firstByte) {
        75 -> {
          // BackendKeyData
          if (length != 12) {
            throw ProtocolErrorException("Length of BackendKeyData message should be 12, not $length")
          }
          backendProcessId = bufferedConnection.getInt()
          backendSecretKey = bufferedConnection.getInt()
          false
        }

        83 -> {
          // ParameterStatus
          val (name, nameLen) = bufferedConnection.readNullTerminatedString(length - 4, encoding)
          val (value, valueLen) = bufferedConnection.readNullTerminatedString(
            length - 4 - nameLen,
            encoding
          )
          if (nameLen + valueLen != length - 4) {
            throw ProtocolErrorException("Message not correctly terminated")
          }
          if (name == "client_encoding") {
            encoding = Charset.forName(value)
          }
          parameters[name] = value
          false
        }

        90 -> {
          // ReadyForQuery
          transactionStatus = bufferedConnection.readReadyForQueryMessage(length - 4)
          readyForQuery = true
          true
        }

        else -> null
      }
    }
  }

  inner class PostgresResultSet(private val rowDescription: Array<FieldDescription>) : KotgresResultSet {
    private var data: Array<Any?> = arrayOfNulls<Any?>(rowDescription.size)
    private val row = Row()

    inner class Row : KotgresqlRow {
      override fun getInt(index: Int): Int? {
        return data[index] as Int?
      }

      override fun getString(index: Int): String? {
        return data[index] as String?
      }

      override fun getLong(index: Int): Long? {
        return data[index] as Long?
      }

      override fun getBoolean(index: Int): Boolean? {
        return data[index] as Boolean?
      }

      override fun toArray(): Array<Any?> {
        return data
      }
    }

    override fun numFields(): Int {
      return rowDescription.size
    }

    override fun fieldType(index: Int): KClass<*> {
      return when (rowDescription[index].dataTypeObjectId) {
        16 -> Boolean::class
        20 -> Long::class
        23 -> Int::class
        1043 -> String::class
        3802 -> String::class
        else -> throw Exception("Unknown type oid ${rowDescription[index].dataTypeObjectId}")
      }
    }

    override suspend fun hasNext(): Boolean {
      val nextRow = readNextRow()
      if (nextRow == null) {
        receiveMessages { firstByte, length ->
          when (firstByte) {
            90 -> {
              // ReadyForQuery
              transactionStatus = bufferedConnection.readReadyForQueryMessage(length - 4)
              readyForQuery = true
              true
            }

            else -> null
          }
        }
        return false
      }
      data = nextRow
      return true
    }

    private suspend fun readNextRow(): Array<Any?>? {
      var result: Array<Any?>? = null
      receiveMessages { firstByte, length ->
        when (firstByte) {
          67 -> {
            // CommandComplete
            var remainingLength = length - 4
            val (_, len) = bufferedConnection.readNullTerminatedString(
              remainingLength,
              encoding
            )
            remainingLength -= len
            true
          }

          68 -> {
            // DataRow
            var remainingLength = length - 4
            val numColumns = bufferedConnection.getShort()
            remainingLength -= 2
            result = arrayOfNulls<Any?>(numColumns.toInt())
            for (i in 0 until numColumns) {
              val fieldDescription = rowDescription[i]
              val colValueLength = bufferedConnection.getInt()
              remainingLength -= 4
              if (fieldDescription.formatCode == FormatCode.Text) {
                if (colValueLength != -1) {
                  val strValue = bufferedConnection.readFixedLengthString(colValueLength, encoding)
                  remainingLength -= colValueLength
                  result!![i] = when (fieldDescription.dataTypeObjectId) {
                    16 -> {
                      // Boolean
                      strValue == "t"
                    }

                    20 -> {
                      // INT8
                      strValue.toLong()
                    }

                    23 -> {
                      // INT4
                      strValue.toInt()
                    }

                    1043 -> {
                      // varchar
                      strValue
                    }

                    3802 -> {
                      // jsonb
                      strValue
                    }

                    else -> {
                      throw ProtocolErrorException("Unknown data type OID ${fieldDescription.dataTypeObjectId}")
                    }
                  }
                }
              } else {
                when (fieldDescription.dataTypeObjectId) {
                  else -> {
                    throw ProtocolErrorException("Unknown data type OID ${fieldDescription.dataTypeObjectId}")
                  }
                }
              }
            }
            if (remainingLength != 0) {
              throw ProtocolErrorException("Message not terminated correctly")
            }
            true
          }

          else -> null
        }
      }
      return result
    }

    override fun next(): Row {
      return row
    }

    override fun close() {
      if (!readyForQuery) {
        throw ProtocolErrorException("Resultset was not fully consumed")
      }
    }
  }

  inner class PostgresPreparedStatement(private val sql: String, private val name: String) :
    KotgresPreparedStatement {
    private var parsed: Boolean = false
    override suspend fun executeUpdate(vararg parameters: Any): Int {
      bufferedConnection.prepareForSending()
      if (!parsed) {
        sendParseMessageWithoutFlush(sql, preparedStatementName = name)
        parsed = true
      }
      sendBindMessageWithoutFlush("", name, *parameters)
      sendExecuteMessageWithoutFlush("")
      sendSyncMessage()
      bufferedConnection.flush()
      awaitParseCompleteAndBindComplete()
      return awaitCommandComplete()
    }

    override suspend fun executeQuery(vararg parameters: Any): KotgresResultSet {
      readyForQuery = false
      bufferedConnection.prepareForSending()
      if (!parsed) {
        sendParseMessageWithoutFlush(sql, preparedStatementName = name)
        parsed = true
      }
      sendBindMessageWithoutFlush("", name, *parameters)
      sendDescribeMessageWithoutFlush("")
      sendExecuteMessageWithoutFlush("")
      sendSyncMessage()
      bufferedConnection.flush()
      awaitParseCompleteAndBindComplete()
      return buildResultSet()
    }

    private suspend fun awaitParseCompleteAndBindComplete() {
      receiveMessages { firstByte, length ->
        when (firstByte) {
          49 -> {
            // ParseComplete
            if (length != 4) {
              throw ProtocolErrorException("Message not terminated correctly")
            }
            false
          }

          50 -> {
            // BindComplete
            if (length != 4) {
              throw ProtocolErrorException("Message not terminated correctly")
            }
            true
          }

          else -> null
        }
      }
    }

    override fun close() {
    }
  }

  private fun sendSyncMessage() {
    bufferedConnection.put(83)
    bufferedConnection.putInt(4)
  }

  private fun sendDescribeMessageWithoutFlush(portalName: String) {
    bufferedConnection.put(68)
    val portalNameBytes = portalName.toByteArray(encoding)
    bufferedConnection.putInt(4 + 1 + portalNameBytes.size + 1)
    bufferedConnection.put(80)
    bufferedConnection.put(portalNameBytes)
    bufferedConnection.put(0)
  }

  private fun sendExecuteMessageWithoutFlush(portalName: String) {
    bufferedConnection.put(69)
    val portalNameBytes = portalName.toByteArray(encoding)
    bufferedConnection.putInt(4 + portalNameBytes.size + 1 + 4)
    bufferedConnection.put(portalNameBytes)
    bufferedConnection.put(0)
    bufferedConnection.putInt(0)
  }

  private fun sendBindMessageWithoutFlush(
    destinationPortal: String,
    sourcePreparedStatement: String,
    vararg parameterValues: Any
  ) {
    bufferedConnection.put(66)
    val portalNameBytes = destinationPortal.toByteArray(encoding)
    val sourcePreparedStatementBytes = sourcePreparedStatement.toByteArray(encoding)
    var messageLength = 4 + portalNameBytes.size + 1 +
      sourcePreparedStatementBytes.size + 1 +
      2 + 2 + 2
    val parameterBytes = if (parameterValues.isNotEmpty()) {
      parameterValues.map {
        val bytes = when (it) {
          is String -> it.toByteArray(encoding)
          is Number -> it.toString().toByteArray(encoding)
          is Instant -> dateTimeFormat.format(OffsetDateTime.ofInstant(it, UTC)).toByteArray(encoding)

          else -> {
            throw SQLException("Unknown parameter type ${it.javaClass}")
          }
        }
        messageLength += bytes.size + 4
        bytes
      }
    } else null
    bufferedConnection.putInt(messageLength)
    bufferedConnection.put(portalNameBytes)
    bufferedConnection.put(0)
    bufferedConnection.put(sourcePreparedStatementBytes)
    bufferedConnection.put(0)
    bufferedConnection.putShort(0) // all text format
    bufferedConnection.putShort(parameterValues.size.toShort())
    parameterBytes?.forEach {
      bufferedConnection.putInt(it.size)
      bufferedConnection.put(it)
    }
    bufferedConnection.putShort(0)
  }

  override suspend fun executeUpdate(sql: String): Int {
    if (!readyForQuery) {
      throw SQLException("Connection not ready for query")
    }
    readyForQuery = false
    sendQueryMessage(sql)
    return awaitCommandComplete()
  }

  override suspend fun executeQuery(sql: String): KotgresResultSet {
    if (!readyForQuery) {
      throw SQLException("Connection not ready for query")
    }
    readyForQuery = false
    sendQueryMessage(sql)
    return buildResultSet()
  }

  private suspend fun awaitCommandComplete(): Int {
    var affectedRows = 0
    receiveMessages { firstByte, length ->
      when (firstByte) {
        67 -> {
          // CommandComplete
          var remainingLength = length - 4
          val (completedCommand, len) = bufferedConnection.readNullTerminatedString(
            remainingLength,
            encoding
          )
          remainingLength -= len
          if (remainingLength != 0) {
            throw ProtocolErrorException("Message not terminated correctly")
          }
          val space = completedCommand.indexOf(' ')
          affectedRows = if (space == -1) {
            0
          } else {
            when (completedCommand.substring(0, space)) {
              "INSERT" -> {
                val secondSpace = completedCommand.indexOf(' ', space + 1)
                completedCommand.substring(secondSpace + 1).toInt()
              }

              "UPDATE", "DELETE" -> {
                completedCommand.substring(space + 1).toInt()
              }

              else -> {
                0
              }
            }
          }
          false
        }

        90 -> {
          // ReadyForQuery
          transactionStatus = bufferedConnection.readReadyForQueryMessage(length - 4)
          readyForQuery = true
          true
        }

        else -> null
      }
    }
    return affectedRows
  }

  private suspend fun buildResultSet(): PostgresResultSet {
    var rowDescription: Array<FieldDescription?>? = null
    receiveMessages { firstByte, length ->
      when (firstByte) {
        84 -> {
          // RowDescription
          var remainingLength = length - 4
          val numFields = bufferedConnection.getShort()
          remainingLength -= 2
          rowDescription = arrayOfNulls<FieldDescription?>(numFields.toInt())
          for (i in 0 until numFields) {
            val (fieldName, fieldNameLen) = bufferedConnection.readNullTerminatedString(
              remainingLength,
              encoding
            )
            remainingLength -= fieldNameLen
            if (remainingLength < 18) {
              throw ProtocolErrorException("Message not formatted correctly")
            }
            val fieldDescription = FieldDescription(
              name = fieldName,
              tableObjectId = bufferedConnection.getInt(),
              columnAttributeNumber = bufferedConnection.getShort(),
              dataTypeObjectId = bufferedConnection.getInt(),
              dataTypeSize = bufferedConnection.getShort(),
              typeModifier = bufferedConnection.getInt(),
              formatCode = if (bufferedConnection.getShort().toInt() == 0) {
                FormatCode.Text
              } else {
                FormatCode.Binary
              }
            )
            remainingLength -= 18
            rowDescription!![i] = fieldDescription
          }
          true
        }

        67 -> {
          // CommandComplete
          var remainingLength = length - 4
          val (completedCommand, len) = bufferedConnection.readNullTerminatedString(
            remainingLength,
            encoding
          )
          remainingLength -= len
          if (remainingLength != 0) {
            throw ProtocolErrorException("Message not terminated correctly")
          }
          throw SQLException("Query has no results, please use executeUpdate(): $completedCommand")
        }

        else -> null
      }
    }
    @Suppress("UNCHECKED_CAST")
    return PostgresResultSet(rowDescription!! as Array<FieldDescription>)
  }

  override suspend fun prepareStatement(sql: String): KotgresPreparedStatement {
    val statementName = UUID.randomUUID().toString()
    return PostgresPreparedStatement(sql, statementName)
  }

  private fun sendParseMessageWithoutFlush(sql: String, preparedStatementName: String) {
    bufferedConnection.put(80)
    val nameBytes = preparedStatementName.toByteArray(encoding)
    val sqlBytes = sql.toByteArray(encoding)
    val messageLength = 4 + sqlBytes.size + 1 + nameBytes.size + 1 + 2
    bufferedConnection.putInt(messageLength)
    if (preparedStatementName.isNotEmpty()) {
      bufferedConnection.put(nameBytes)
    }
    bufferedConnection.put(0)
    bufferedConnection.put(sqlBytes)
    bufferedConnection.put(0)
    bufferedConnection.putShort(0)
  }

  private suspend fun sendQueryMessage(query: String) {
    bufferedConnection.prepareForSending()
    bufferedConnection.put(81)
    val queryBytes = query.toByteArray(encoding)
    val messageLength = 4 + queryBytes.size + 1
    bufferedConnection.putInt(messageLength)
    bufferedConnection.put(queryBytes)
    bufferedConnection.put(0)
    bufferedConnection.flush()
  }

  private suspend fun receiveMessages(handler: suspend (Int, Int) -> Boolean?) {
    while (true) {
      val firstByte = bufferedConnection.get().toInt()
      val length = bufferedConnection.getInt()
      when (firstByte) {
        69 -> {
          // ErrorMessage
          val errorMessage = bufferedConnection.readErrorResponse(length - 4, encoding)
          readyForQuery = true
          throw PostgresErrorResponseException(errorMessage)
        }

        else -> {
          val result = handler(firstByte, length)
          if (result == null) {
            val content = ByteArray(length - 4)
            bufferedConnection.get(content)
            throw ProtocolErrorException("Unknown message type $firstByte")
          }
          if (result == true) {
            return
          }
        }
      }
    }
  }

  override fun close() {
    bufferedConnection.close()
  }
}