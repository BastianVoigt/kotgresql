package kotgresql.core

import kotgresql.core.nio.AsyncSocket
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.Charset
import java.sql.SQLException

class BufferedConnection(bufferSize: Int = 4096) : AutoCloseable {
  private val socket = AsyncSocket()
  private val buf = ByteBuffer.allocateDirect(bufferSize)!!

  init {
    buf.order(ByteOrder.BIG_ENDIAN)
  }

  suspend fun connect(inetSocketAddress: InetSocketAddress) {
    socket.connect(inetSocketAddress)
  }

  suspend fun get(value: ByteArray) {
    if (buf.remaining() < value.size) {
      readChunk(value.size)
    }
    buf.get(value)
  }

  private suspend fun readChunk(minLength: Int) {
    if (buf.remaining() < minLength) {
      buf.compact()
      var read = 0
      do {
        val r = socket.read(buf)
        if (r == -1) {
          throw SQLException("Connection closed")
        }
        read += r
      } while (read < minLength)
      buf.flip()
    }
  }

  suspend fun get(): Byte {
    readChunk(1)
    return buf.get()
  }

  suspend fun getInt(): Int {
    readChunk(4)
    return buf.getInt()
  }

  suspend fun readFixedLengthString(length: Int, encoding: Charset): String {
    readChunk(length)
    if (buf.remaining() < length) {
      throw Exception("Buffer too small")
    }
    val bytes = ByteArray(length)
    buf.get(bytes)
    return String(bytes, encoding)
  }

  suspend fun readNullTerminatedString(maxLength: Int, encoding: Charset): Pair<String, Int> {
    var canBeCompacted = buf.position() > 0
    buf.mark()
    // search for terminating zero
    var byte: Int = 1
    for (i in 0 until maxLength) {
      if (buf.remaining() == 0) {
        if (canBeCompacted) {
          buf.compact()
          socket.read(buf)
          buf.flip()
          canBeCompacted = false
        } else {
          throw Exception("Buffer capacity ${buf.capacity()} too small for string")
        }
      }
      byte = buf.get().toInt()
      if (byte == 0) {
        break
      }
    }
    if (byte != 0) {
      throw ProtocolErrorException("No null-terminator found for string in allowed maxLength")
    }
    val endOfString = buf.position()
    buf.reset()
    val len = endOfString - buf.position() - 1
    val stringBytes = ByteArray(len)
    buf.get(stringBytes)
    buf.get()
    return Pair(String(stringBytes, 0, len, encoding), len + 1)
  }

  fun putInt(value: Int) {
    buf.putInt(value)
  }

  fun put(value: ByteArray) {
    buf.put(value)
  }

  fun put(value: Byte) {
    buf.put(value)
  }

  fun putShort(value: Short) {
    buf.putShort(value)
  }

  suspend fun flush() {
    buf.flip()
    socket.write(buf)
    prepareForReceiving()
  }

  fun prepareForSending() {
    buf.clear()
  }

  private fun prepareForReceiving() {
    buf.position(0)
    buf.limit(0)
  }

  suspend fun getShort(): Short {
    readChunk(2)
    return buf.getShort()
  }

  suspend fun readErrorResponse(length: Int, encoding: Charset): ErrorResponse {
    val builder = ErrorResponse.Builder()
    var remainingLength = length
    while (true) {
      val fieldIdentifier = get().toInt()
      remainingLength -= 1
      if (fieldIdentifier == 0) {
        if (remainingLength != 0) {
          throw ProtocolErrorException("Message not terminated correctly")
        }
        return builder.build()
      }
      val (value, valueLen) = readNullTerminatedString(remainingLength, encoding)
      remainingLength -= valueLen
      when (fieldIdentifier) {
        67 -> builder.sqlstateCode = value
        68 -> builder.details = value
        70 -> builder.file = value
        72 -> builder.hint = value
        76 -> builder.lineNumber = value
        77 -> builder.humanReadableErrorMessage = value
        82 -> builder.routine = value
        83 -> builder.severityLocalized = value
        86 -> builder.severity = value
        87 -> builder.where = value
        80 -> builder.position = value.toInt()
        99 -> builder.columnName = value
        100 -> builder.dataTypeName = value
        110 -> builder.constraintName = value
        112 -> builder.internalPosition = value.toInt()
        113 -> builder.internalQuery = value
        115 -> builder.schemaName = value
        116 -> builder.tableName = value

        else -> {
          throw ProtocolErrorException("Unknown fieldIdentifier: $fieldIdentifier = $value")
        }
      }
    }
  }

  suspend fun readReadyForQueryMessage(length: Int): Int {
    if (length != 1) {
      throw ProtocolErrorException("Length of ReadyForQuery message should be 5, not ${length + 4}")
    }
    return get().toInt()
  }

  override fun close() {
    socket.close()
  }
}