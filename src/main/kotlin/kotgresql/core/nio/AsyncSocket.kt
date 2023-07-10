package kotgresql.core.nio

import kotlinx.coroutines.suspendCancellableCoroutine
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.CompletionHandler
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

class AsyncSocket {
  private val socket = AsynchronousSocketChannel.open()

  suspend fun connect(remote: SocketAddress) {
    suspendCancellableCoroutine { cont ->
      socket.connect(
        remote, this,
        object : CompletionHandler<Void, AsyncSocket> {
          override fun completed(result: Void?, attachment: AsyncSocket) {
            cont.resume(Unit)
          }
          override fun failed(exc: Throwable, attachment: AsyncSocket) {
            cont.resumeWithException(exc)
          }
        }
      )
    }
  }

  suspend fun write(buf: ByteBuffer): Int {
    return suspendCancellableCoroutine { cont ->
      socket.write(
        buf, this,
        object : CompletionHandler<Int, AsyncSocket> {
          override fun completed(result: Int, attachment: AsyncSocket) {
            cont.resume(result)
          }

          override fun failed(exc: Throwable, attachment: AsyncSocket) {
            cont.resumeWithException(exc)
          }
        }
      )
    }
  }

  suspend fun read(buf: ByteBuffer): Int {
    return suspendCancellableCoroutine { cont ->
      socket.read(
        buf, this,
        object : CompletionHandler<Int, AsyncSocket> {
          override fun completed(result: Int, attachment: AsyncSocket) {
            cont.resume(result)
          }

          override fun failed(exc: Throwable, attachment: AsyncSocket) {
            cont.resumeWithException(exc)
          }
        }
      )
    }
  }

  fun close() {
    socket.close()
  }
}