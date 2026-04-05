package com.java3y.austin.cron.utils

import cn.hutool.core.text.csv.CsvReadConfig
import cn.hutool.core.text.csv.CsvRow
import cn.hutool.core.text.csv.CsvRowHandler
import cn.hutool.core.text.csv.CsvUtil
import com.java3y.austin.cron.csv.CountFileRowHandler
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import org.slf4j.LoggerFactory
import java.io.InputStreamReader
import java.nio.file.Files
import java.nio.file.Paths

object FileUtils {
    private val log = LoggerFactory.getLogger(FileUtils::class.java)

    const val RECEIVER_KEY = "userId"

    fun getCsvRow(path: String, handler: CsvRowHandler) {
        try {
            val isr = InputStreamReader(Files.newInputStream(Paths.get(path)), "UTF-8")
            CsvUtil.getReader(isr, CsvReadConfig().setContainsHeader(true))
                .use { reader -> reader.read(handler) }
        } catch (e: Exception) {
            log.error("Exception while reading csv file", e)
        }
    }

    fun countCsvRow(path: String, handler: CountFileRowHandler): Long {
        try {
            val isr = InputStreamReader(Files.newInputStream(Paths.get(path)), "UTF-8")
            CsvUtil.getReader(isr, CsvReadConfig().setContainsHeader(true))
                .use { reader -> reader.read(handler) }
        } catch (e: Exception) {
            log.error("Exception while counting csv file", e)
        }
        return handler.rowSize
    }

    fun getParamFromLine(fieldMap: Map<String, String>?): Map<String, String> {
        if (fieldMap.isNullOrEmpty()) return emptyMap()

        return fieldMap.filterKeys { it != RECEIVER_KEY }
    }

    fun readCsvAsFlow(path: String): Flow<CsvRow> = flow {
        val isr = InputStreamReader(Files.newInputStream(Paths.get(path)), "UTF-8")
        CsvUtil.getReader(isr, CsvReadConfig().setContainsHeader(true))
            .use { reader ->
                for (row in reader) {
                    emit(row)
                }
            }
    }.flowOn(Dispatchers.IO)
}