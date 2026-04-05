package com.java3y.austin.cron.service.impl

import cn.hutool.core.text.CharSequenceUtil
import com.java3y.austin.cron.pending.CrowdBatchConsumer
import com.java3y.austin.cron.utils.FileUtils
import com.java3y.austin.cron.utils.ReadFileUtils
import com.java3y.austin.support.cache.MessageTemplateCaching
import com.java3y.austin.support.vo.CrowdInfoVo
import com.xxl.job.core.context.XxlJobHelper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

class TaskHandlerImplKt(
    private val cache: MessageTemplateCaching,
    private val crowdBatchConsumer: CrowdBatchConsumer
) {
    suspend fun handle(messageTemplateId: Long) = coroutineScope {
        val messageTemplate = cache.getMessageTemplate(messageTemplateId).get()

        if (CharSequenceUtil.isBlank(messageTemplate.cronCrowdPath)) {
            return@coroutineScope
        }

        val jobId = XxlJobHelper.getJobId()

        withContext(Dispatchers.IO) {
            ReadFileUtils.getCsvRow(messageTemplate.cronCrowdPath) { row ->
                val receiver = row.fieldMap?.get(ReadFileUtils.RECEIVER_KEY)
                if (receiver.isNullOrBlank()) {
                    return@getCsvRow
                }

                val params = ReadFileUtils.getParamFromLine(row.fieldMap)
                val crowdInfoVo = CrowdInfoVo.builder()
                    .receiver(receiver)
                    .xxlJobId(jobId)
                    .params(params)
                    .messageTemplateId(messageTemplateId)
                    .build()

                launch { crowdBatchConsumer.pending(crowdInfoVo) }
            }
        }
    }

    suspend fun handle2(messageTemplateId: Long) = coroutineScope {
        val messageTemplate = cache.getMessageTemplate(messageTemplateId).get()

        if (CharSequenceUtil.isBlank(messageTemplate.cronCrowdPath)) {
            return@coroutineScope
        }

        val jobId = XxlJobHelper.getJobId()

        withContext(Dispatchers.IO) {
            FileUtils.readCsvAsFlow(messageTemplate.cronCrowdPath)
                .collect { row ->
                    val receiver = row.fieldMap?.get(ReadFileUtils.RECEIVER_KEY)
                    if (!receiver.isNullOrBlank()) {
                        val params = FileUtils.getParamFromLine(row.fieldMap)
                        val crowdInfoVo = CrowdInfoVo.builder()
                            .receiver(receiver)
                            .xxlJobId(jobId)
                            .params(params)
                            .messageTemplateId(messageTemplateId)
                            .build()

                        crowdBatchConsumer.pending(crowdInfoVo)
                    }
                }
        }
    }
}