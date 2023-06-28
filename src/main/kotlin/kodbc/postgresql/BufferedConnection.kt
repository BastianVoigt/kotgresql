package kodbc.postgresql

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.Charset

class BufferedConnection(bufferSize: Int = 4096) {
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
            buf.compact()
            socket.read(buf)
            buf.flip()
        }
        buf.get(value)
    }

    suspend fun get(): Byte {
        if (buf.remaining() < 1) {
            buf.compact()
            socket.read(buf)
            buf.flip()
        }
        return buf.get()
    }

    suspend fun getInt(): Int {
        if (buf.remaining() < 4) {
            buf.compact()
            socket.read(buf)
            buf.flip()
        }
        return buf.getInt()
    }

    suspend fun readFixedLengthString(length: Int, encoding: Charset): String {
        if (buf.remaining() < length) {
            buf.compact()
            socket.read(buf)
            buf.flip()
        }
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
        if (buf.remaining() < 2) {
            buf.compact()
            socket.read(buf)
            buf.flip()
        }
        return buf.getShort()
    }
}