package com.java3y;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.java3y.austin.AustinApplication;
import com.java3y.austin.support.dao.ChannelAccountDao;
import com.java3y.austin.support.dao.MessageTemplateDao;
import com.java3y.austin.support.domain.MessageTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@SpringBootTest(classes = AustinApplication.class)
public class PrepareTestDataTest {
    @Autowired
    private MessageTemplateDao messageTemplateDao;

    @Autowired
    private ChannelAccountDao channelAccountDao;

    private static final String PATH = "/home/mrawa/Documents/test_data/";

    private static final String UPLOAD_PREFIX = "/home/mrawa/code/austin/data/upload/crowd/";

    private static final WeightedRandomUtils<Integer> msgTypeRandom = new WeightedRandomUtils<>();
    private static final WeightedRandomUtils<Integer> shieldTypeRandom = new WeightedRandomUtils<>();

    @Test
    public void batchInsertData() throws FileNotFoundException {
        Path directoryPath = Paths.get(PATH);
        msgTypeRandom.add(50, 30)
                .add(30, 10)
                .add(20, 20);

        shieldTypeRandom.add(50, 10)
                .add(20, 20)
                .add(30, 30);

        var cronConf = readCronConf();

        List<MessageTemplate> templates = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(directoryPath)) {
            paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".txt"))
                    .forEach(path -> {
                        if (path.getFileName().toString().startsWith("email")) {
                            templates.add(processEmail(path, cronConf));
                        } else {
                            templates.add(processPhone(path, cronConf));
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        messageTemplateDao.saveAll(templates);
    }

    private MessageTemplate processEmail(Path path, Map<String, String> cronConf) {
        try {
            String name = FileUtil.getPrefix(path.toFile());
            String content = Files.readString(path);
            String prefix = StrUtil.removeSuffix(name, "-template");
            String title = NameUtils.getEmailName();

            return MessageTemplate.builder()
                    .creator("mrawa")
                    .updator("mrawa")
                    .auditor("mrawa")
                    .auditStatus(10)
                    .team("mrawa")
                    .proposer("mrawa")
                    .cronCrowdPath(UPLOAD_PREFIX + prefix + ".csv")
                    .isDeleted(0)
                    .expectPushTime(cronConf.get(prefix))
                    .sendAccount(1)
                    .templateType(10)
                    .msgContent(JSON.toJSONString(new EmailContent(content, title, "")))
                    .name(title)
                    .sendChannel(40)
                    .idType(50)
                    .msgStatus(20)
                    .msgType(msgTypeRandom.next())
                    .shieldType(shieldTypeRandom.next())
                    .created((int) System.currentTimeMillis())
                    .updated((int) System.currentTimeMillis())
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private MessageTemplate processPhone(Path path, Map<String, String> cronConf) {
        try {
            String name = FileUtil.getPrefix(path.toFile());
            String prefix = StrUtil.removeSuffix(name, "-template");
            String content = Files.readString(path);
            return MessageTemplate.builder()
                    .creator("mrawa")
                    .updator("mrawa")
                    .auditor("mrawa")
                    .auditStatus(10)
                    .team("mrawa")
                    .proposer("mrawa")
                    .cronCrowdPath(UPLOAD_PREFIX + prefix + ".csv")
                    .isDeleted(0)
                    .expectPushTime(cronConf.get(prefix))
                    .sendAccount(0)
                    .templateType(10)
                    .msgContent(JSON.toJSONString(new SmsContent(content, "")))
                    .name(NameUtils.getPhoneName())
                    .sendChannel(30)
                    .idType(30)
                    .msgStatus(20)
                    .msgType(msgTypeRandom.next())
                    .shieldType(shieldTypeRandom.next())
                    .created((int) System.currentTimeMillis())
                    .updated((int) System.currentTimeMillis())
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> readCronConf() throws FileNotFoundException {
        Path path = Paths.get(PATH + "cron.conf");

        Map<String, String> cronConf = new HashMap<>();
        FileUtil.readLines(path.toFile(), StandardCharsets.UTF_8)
                .stream()
                .filter(line -> !line.trim().startsWith("#") && !line.trim().isEmpty())
                .forEach(line -> {
                    String[] split = line.split("\\|");
                    cronConf.put(split[0], split[1]);
                });
        return cronConf;
    }

    private static class NameUtils {
        private static int emailIndex = 0;
        private static int phoneIndex = 0;

        public static String getEmailName() {
            return "测试用邮箱模板-" + emailIndex++;
        }

        public static String getPhoneName() {
            return "测试用短信模板-" + phoneIndex++;
        }
    }
}
