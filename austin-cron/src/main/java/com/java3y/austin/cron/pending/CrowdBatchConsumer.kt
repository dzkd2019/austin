package com.java3y.austin.cron.pending

import cn.hutool.core.util.IdUtil
import com.java3y.austin.cron.constants.PendingConstant
import com.java3y.austin.service.api.domain.BatchSendRequest
import com.java3y.austin.service.api.domain.MessageParam
import com.java3y.austin.service.api.enums.BusinessCode
import com.java3y.austin.service.api.service.SendService
import com.java3y.austin.support.constans.MdcConstant
import com.java3y.austin.support.pending.consumeBatches
import com.java3y.austin.support.pending.retry
import com.java3y.austin.support.vo.CrowdInfoVo
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class CrowdBatchConsumer(
    private val sendService: SendService
) {
    private val log = LoggerFactory.getLogger(CrowdBatchConsumer::class.java)

    private val channel = Channel<CrowdInfoVo>(capacity = 2000)

    private val componentScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @PostConstruct
    fun init() {
        componentScope.launch {
            log.info("Crowd Batch Consumer starting...")

            channel.consumeBatches(
                maxSize = PendingConstant.NUM_THRESHOLD,
                timeThresholdMillis = PendingConstant.TIME_THRESHOLD
            ) { batch ->

                val first = batch.first()
                val mdcMap = mapOf<String, String>(
                    MdcConstant.MDC_TEMPLATE_ID to first.messageTemplateId.toString(),
                    MdcConstant.XXL_JOB_ID to first.xxlJobId.toString(),
                    MdcConstant.MDC_TRACE_ID to IdUtil.fastSimpleUUID(),
                    MdcConstant.MDC_BUSINESS_ID to IdUtil.fastSimpleUUID()
                )

                withContext(Dispatchers.IO) {
                    try {
                        doHandle(batch)
                    } catch (e: Exception) {
                        log.error("批量发送消息出现异常", e)
                    }
                }
            }
        }

        log.info("Crowd Batch Consumer finished.")
    }

    @PreDestroy
    fun destroy() {
        componentScope.cancel()
    }

    suspend fun pending(task: CrowdInfoVo) {
        channel.send(task)
    }

    private suspend fun doHandle(batch: List<CrowdInfoVo>) {
        val messageParams = batch
            .groupBy({ it.params }, { it.receiver })
            .map {(params, receivers) ->
                MessageParam.builder()
                    .variables(params)
                    .receiver(receivers.joinToString (","))
                    .build()
            }

        if (messageParams.isEmpty()) return

        val batchSendRequest = BatchSendRequest.builder()
            .code(BusinessCode.COMMON_SEND.code)
            .messageParamList(messageParams)
            .messageTemplateId(batch.first().messageTemplateId)
            .build()

        retry(times = 3, initialDelay = 1000L) {
            sendService.batchSend(batchSendRequest)
        }
    }
}