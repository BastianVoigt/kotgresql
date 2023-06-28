package kodbc.postgresql

import java.net.InetSocketAddress
import java.nio.charset.Charset
import kotlin.text.Charsets.UTF_8

class PostgresClient {
    val bufferedConnection = BufferedConnection(4096)
    var backendProcessId: Int? = null
    var backendSecretKey: Int? = null
    var encoding: Charset = UTF_8
    var readyForQuery = false

    suspend fun readErrorMessage(length: Int): ErrorMessage {
        val builder = ErrorMessage.Builder()
        var remainingLength = length
        while (true) {
            val fieldIdentifier = bufferedConnection.get().toInt()
            remainingLength -= 1
            if (fieldIdentifier == 0) {
                if (remainingLength != 0) {
                    throw ProtocolErrorException("Message not terminated correctly")
                }
                return builder.build()
            }
            val (value, valueLen) = bufferedConnection.readNullTerminatedString(remainingLength, encoding)
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

    inner class ResultSet(private val rowDescription: Array<FieldDescription?>) : AutoCloseable {
        private var data: Array<Any?> = arrayOfNulls<Any?>(rowDescription.size)
        private val row = Row()

        inner class Row {
            fun getInt(i: Int): Int {
                return data[i]!! as Int
            }

            fun getString(i: Int): String {
                return data[i]!! as String
            }
        }

        suspend fun <T> map(rowMapper: (Row) -> T): List<T> {
            val result = mutableListOf<T>()
            while (hasNext()) {
                result.add(rowMapper(next()))
            }
            return result
        }

        suspend fun hasNext(): Boolean {
            val nextRow = readNextRow()
            if (nextRow == null) {
                while (!readyForQuery) {
                    val firstByte = bufferedConnection.get().toInt()
                    val length = bufferedConnection.getInt()
                    println("First Byte $firstByte len $length")
                    when (firstByte) {
                        90 -> {
                            // ReadyForQuery
                            readReadyForQuery(length - 4)
                            readyForQuery = true
                        }

                        69 -> {
                            // ErrorMessage
                            val errorMessage = readErrorMessage(length - 4)
                            throw SQLException(errorMessage)
                        }

                        else -> {
                            val content = ByteArray(length - 4)
                            bufferedConnection.get(content)
                        }
                    }
                }
                return false
            }
            return true
        }

        private suspend fun readNextRow(): Array<Any?>? {
            while (true) {
                val firstByte = bufferedConnection.get().toInt()
                val length = bufferedConnection.getInt()
                println("First Byte $firstByte len $length")
                var remainingLength = length - 4
                when (firstByte) {
                    67 -> {
                        // CommandComplete
                        val (completedCommand, len) = bufferedConnection.readNullTerminatedString(
                            remainingLength,
                            encoding
                        )
                        println("CommandComplete: $completedCommand")
                        remainingLength -= len
                        return null
                    }

                    68 -> {
                        // DataRow
                        val numColumns = bufferedConnection.getShort()
                        remainingLength -= 2
                        println("DataRow $numColumns")
                        val result = arrayOfNulls<Any?>(numColumns.toInt())
                        for (i in 0 until numColumns) {
                            val fieldDescription = rowDescription[i]!!
                            val colValueLength = bufferedConnection.getInt()
                            println("ColValueLength $colValueLength")
                            remainingLength -= 4
                            if (fieldDescription.formatCode == FormatCode.Binary) {
                                when (fieldDescription.dataTypeObjectId) {
                                    23 -> {
                                        // INT4
                                        if (colValueLength == 1) {
                                            result[i] = bufferedConnection.get().toInt()
                                        } else if (colValueLength == 2) {
                                            result[i] = bufferedConnection.getShort().toInt()
                                        } else if (colValueLength == 4) {
                                            result[i] = bufferedConnection.getInt()
                                        } else{
                                            throw Exception("BADBAD")
                                        }
                                        remainingLength -= colValueLength
                                        println("Int ${result[i]}")
                                    }

                                    1043 -> {
                                        // varchar
                                        result[i] = bufferedConnection.readFixedLengthString(colValueLength, encoding)
                                        remainingLength -= colValueLength
                                        println("Varchar ${result[i]}")
                                    }

                                    else -> {
                                        throw ProtocolErrorException("Unknown data type OID ${fieldDescription.dataTypeObjectId}")
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
                    }

                    90 -> {
                        // ReadyForQuery
                        readReadyForQuery(length - 4)
                        readyForQuery = true
                        return null
                    }

                    69 -> {
                        // ErrorMessage
                        val errorMessage = readErrorMessage(length - 4)
                        throw SQLException(errorMessage)
                    }

                    else -> {
                        throw ProtocolErrorException("Received unknown message $firstByte")
                    }
                }
                if (remainingLength != 0) {
                    throw ProtocolErrorException("Message not formatted correctly")
                }
            }
        }

        fun next(): Row {
            return row
        }

        override fun close() {
            if (!readyForQuery) {
                throw ProtocolErrorException("Resultset was not fully consumed")
            }
        }
    }

    private suspend fun readReadyForQuery(length: Int) {
        if (length != 1) {
            throw ProtocolErrorException("Length of ReadyForQuery message should be 5, not ${length+4}")
        }
        val transactionStatus = bufferedConnection.get().toInt()
        println("ReadyForQuery - tx status $transactionStatus")
    }

    suspend fun connect(
        host: String,
        port: Int = 5432,
        username: String,
        password: String,
        databaseName: String
    ) {
        bufferedConnection.connect(InetSocketAddress(host, port))
        sendStartupPacket(username, databaseName)
        var status = 0
        while (true) {
            val firstByte = bufferedConnection.get().toInt()
            val length = bufferedConnection.getInt()
            when (status) {
                0 -> {
                    when (firstByte) {
                        118 -> {
                            val negotiateProtocolVersionMessage = readNegotiateProtocolVersionMessage(length - 4)
                            println("Negotiate: $negotiateProtocolVersionMessage")
                        }

                        69 -> {
                            val errorMessage = readErrorMessage(length - 4)
                            throw SQLException(errorMessage)
                        }

                        82 -> {
                            if (handleAuthMessage(length)) {
                                status = 1
                            }
                        }

                        else -> {
                            throw ProtocolErrorException("Message type $firstByte not allowed in this context")
                        }
                    }
                }

                1 -> {
                    when (firstByte) {
                        75 -> {
                            if (length != 12) {
                                throw ProtocolErrorException("Length of BackendKeyData message should be 12, not $length")
                            }
                            backendProcessId = bufferedConnection.getInt()
                            backendSecretKey = bufferedConnection.getInt()
                            println("BackendKeyData: $backendProcessId $backendSecretKey")
                        }

                        83 -> {
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
                            println("Parameter $name = $value")
                        }

                        90 -> {
                            if (length != 5) {
                                throw ProtocolErrorException("Length of ReadyForQuery message should be 5, not $length")
                            }
                            val transactionStatus = bufferedConnection.get().toInt()
                            println("ReadyForQuery - tx status $transactionStatus")
                            readyForQuery = true
                            return
                        }

                        else -> {
                            throw ProtocolErrorException("Message type $firstByte not allowed in this context")
                        }
                    }
                }

                else -> throw ProtocolErrorException("Bad status")
            }
        }
    }

    private suspend fun readNegotiateProtocolVersionMessage(length: Int): NegotiateProtocolVersionMessage {
        var remainingLength = length
        val newestMinorVersion = bufferedConnection.getInt()
        val numProtocolOptions = bufferedConnection.getInt()
        remainingLength -= 8
        val protocolOptions = mutableListOf<String>()
        for (i in 0 until numProtocolOptions) {
            val (value, valueLen) = bufferedConnection.readNullTerminatedString(remainingLength, encoding)
            remainingLength -= valueLen
            protocolOptions.add(value)
            if (remainingLength <= 0) {
                throw ProtocolErrorException("Message not terminated correctly")
            }
        }
        if (remainingLength != 0) {
            throw ProtocolErrorException("Message not terminated correctly")
        }
        return NegotiateProtocolVersionMessage(newestMinorVersion, protocolOptions)
    }

    private suspend fun sendStartupPacket(username: String, databaseName: String) {
        bufferedConnection.prepareForSending()
        val msgLength = 4 + // length
                4 + // protocol version number
                4 + 1 + // "user"
                username.length + 1 +
                8 + 1 + // "database"
                databaseName.length + 1 +
                1 // terminating zero
        bufferedConnection.putInt(msgLength)
        bufferedConnection.putInt(3 shl 16) // protocol version 3.0
        bufferedConnection.put("user".toByteArray())
        bufferedConnection.put(0)
        bufferedConnection.put(username.toByteArray())
        bufferedConnection.put(0)
        bufferedConnection.put("database".toByteArray())
        bufferedConnection.put(0)
        bufferedConnection.put(databaseName.toByteArray())
        bufferedConnection.put(0)
        bufferedConnection.put(0)
        bufferedConnection.flush()
    }

    private suspend fun handleAuthMessage(length: Int): Boolean {
        val authType = bufferedConnection.getInt()
        when (authType) {
            0 -> {
                println("Auth successful")
                return true
            }

            2 -> {
                println("Kerberos v5")
                return false
            }

            3 -> {
                println("Cleartext password")
                return false
            }

            5 -> {
                val salt = ByteArray(4)
                bufferedConnection.get(salt)
                println("MD5 Password, salt $salt")
                return false
            }

            8 -> {
                println("SCM Credential")
                return false
            }

            else -> {
                println(authType)
                return false
            }
        }
    }

    suspend fun executeQuery(query: String): ResultSet {
        if (!readyForQuery) {
            throw SQLException(
                ErrorMessage(
                    "FATAL",
                    "",
                    "FATAL",
                    "Connection not ready for query",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
        }
        readyForQuery = false
        sendQueryMessage(query)
        val firstByte = bufferedConnection.get().toInt()
        val length = bufferedConnection.getInt()
        var remainingLength = length - 4
        when (firstByte) {
            84 -> {
                // RowDescription
                val numFields = bufferedConnection.getShort()
                remainingLength -= 2
                println("RowDescription $numFields fields")
                val rowDescription = arrayOfNulls<FieldDescription?>(numFields.toInt())
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
                        formatCode = if (bufferedConnection.getShort().toInt() == 1) {
                            FormatCode.Text
                        } else {
                            FormatCode.Binary
                        }
                    )
                    remainingLength -= 18
                    println("   $fieldDescription")
                    rowDescription[i] = fieldDescription
                }
                return ResultSet(rowDescription)
            }

            69 -> {
                // ErrorMessage
                val errorMessage = readErrorMessage(length - 4)
                readyForQuery = true
                throw SQLException(errorMessage)
            }

            else -> {
                val content = ByteArray(remainingLength)
                bufferedConnection.get(content)
                throw ProtocolErrorException("Unknown message $firstByte")
            }
        }
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
}