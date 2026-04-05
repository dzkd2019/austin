package com.java3y.austin.support.pending

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import kotlin.time.Duration.Companion.milliseconds

suspend fun <T> Channel<T>.consumeBatches(
    maxSize: Int,
    timeThresholdMillis: Long,
    onBatchReady: suspend (List<T>) -> Unit
) {
    val buffer = mutableListOf<T>()

    for (item in this) {
        buffer.add(item)

        try {
            withTimeout(timeThresholdMillis.milliseconds) {
                while (buffer.size < maxSize) {
                    while (buffer.size < maxSize) {
                        val nextItem = receiveCatching().getOrNull() ?: break
                        buffer.add(nextItem)
                    }
                }
            }
        } catch (_: TimeoutCancellationException) {

        }
        if(buffer.isNotEmpty()) {
            onBatchReady(buffer)
            buffer.clear()
        }
    }
}

suspend fun <T> retry(
    times: Int,
    initialDelay: Long,
    factor: Double = 2.0,
    block: suspend () -> T
): T {
    var currentDelay = initialDelay
    repeat(times - 1) {
        try {
            return block()
        } catch (e: Exception) {
            delay(currentDelay.milliseconds)
            currentDelay = (currentDelay * factor).toLong()
        }
    }

    return block()
}