package kotgresql.core.auth

import java.nio.charset.Charset
import java.security.MessageDigest

class MD5AuthenticationProcessor(
  private val username: String,
  private val password: String,
  private val encoding: Charset
) {
  private fun concat(a: ByteArray, b: ByteArray): ByteArray {
    val result = ByteArray(a.size + b.size)
    a.copyInto(result, 0, 0, a.size)
    b.copyInto(result, a.size, 0, b.size)
    return result
  }

  private fun md5Hex(text: ByteArray): String {
    val messageDigest = MessageDigest.getInstance("MD5")
    val digest = messageDigest.digest(text)
    return digest.joinToString(separator = "") { eachByte -> "%02x".format(eachByte) }
  }

  fun createClientFirstMessage(randomSalt: ByteArray): String {
    return "md5" + md5Hex(
      concat(
        md5Hex(
          concat(
            password.toByteArray(encoding),
            username.toByteArray(encoding)
          )
        ).toByteArray(encoding),
        randomSalt
      )
    )
  }
}