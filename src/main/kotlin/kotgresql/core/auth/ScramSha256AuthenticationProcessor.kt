package kotgresql.core.auth

import java.nio.charset.Charset
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.*
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.experimental.xor

class ScramSha256AuthenticationProcessor(private val password: String, private val encoding: Charset) {
  private val hmacAlgorithm = "HmacSHA256"

  private val clientNonce: String = run {
    val r = ByteArray(18)
    SecureRandom.getInstanceStrong().nextBytes(r)
    Base64.getUrlEncoder().encodeToString(r)
  }

  fun createClientFirstMessage(): String {
    return "n,,n=,r=$clientNonce"
  }

  private fun hash(str: ByteArray): ByteArray {
    val messageDigest = MessageDigest.getInstance("SHA-256")
    return messageDigest.digest(str)
  }

  private fun hmac(key: ByteArray, str: ByteArray): ByteArray {
    val mac = Mac.getInstance(hmacAlgorithm)
    mac.init(SecretKeySpec(key, hmacAlgorithm))
    mac.update(str)
    return mac.doFinal()
  }

  private val int1AsBytes = byteArrayOf(0, 0, 0, 1)

  private fun generateSaltedPassword(
    password: String,
    salt: ByteArray,
    iterationsCount: Int
  ): ByteArray {
    val mac = Mac.getInstance(hmacAlgorithm)
    mac.init(SecretKeySpec(password.toByteArray(), hmacAlgorithm))
    mac.update(salt)
    mac.update(int1AsBytes)
    val result = mac.doFinal()
    var previous: ByteArray? = null
    for (i in 1 until iterationsCount) {
      mac.update(previous ?: result)
      previous = mac.doFinal()
      for (x in result.indices) {
        result[x] = (result[x].toInt() xor previous[x].toInt()).toByte()
      }
    }
    return result
  }

  fun createClientFinalMessage(serverFirstMessage: String): String {
    val msg = serverFirstMessage.split(",").associate {
      val idx = it.indexOf('=')
      Pair(it.substring(0, idx), it.substring(idx + 1))
    }
    val iterationCount = msg["i"]!!.toInt()
    val salt = Base64.getDecoder().decode(msg["s"])
    val serverNonce = msg["r"]
    val saltedPassword = generateSaltedPassword(
      password,
      salt,
      iterationCount
    )
    val clientKey = hmac(saltedPassword, "Client Key".toByteArray())
    val storedKey = hash(clientKey)
    val channelBinding = "c=${base64String("n,,".toByteArray())}"
    val nonce = "r=$serverNonce"
    val clientFinalMessageWithoutProof = "$channelBinding,$nonce"
    val authMessage = "n=,r=$clientNonce,$serverFirstMessage,$clientFinalMessageWithoutProof"
    val clientSignature = hmac(storedKey, authMessage.toByteArray())
    val clientProof = clientKey.clone()
    for (i in clientProof.indices) {
      clientProof[i] = clientProof[i] xor clientSignature[i]
    }
    return clientFinalMessageWithoutProof + ",p=${base64String(clientProof)}"
  }

  private fun base64String(saltedPassword: ByteArray): String = Base64.getEncoder().encodeToString(saltedPassword)
}