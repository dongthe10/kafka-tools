import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 获取指定区间指定topic的消息
 *
 * kafkaAdminClient操控kafka消息
 */
public class KafkaConsumerByTime {

    private static KafkaConsumer<String, String> consumer;

    private static final String CONFIG_PATH_KEY  = "config";

    private static final String CONFIG_PATH  = "./config";

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerByTime.class);

    public static void main(String[] args) throws Exception {
        //传入参数判断
        if (args.length < 3) {
            throw new Exception("parameter args exception!");
        }
        String filterValue = "";
        if (args.length > 3) {
            filterValue = args[3];
        }
        long fetchStartTime = 0;
        long fetchEndTime = 0;

        try {
            fetchStartTime = dateToStamp(args[1]);
            fetchEndTime = dateToStamp(args[2]);
        } catch (ParseException e) {
            throw new Exception(e.toString());
        }
        if (fetchStartTime == 0 || fetchEndTime == 0) {
            throw new Exception("startTime|endTime error!");
        }

        //kafkaConsumer
        Properties props = new Properties();
        // 服务器地址从文件中读取
        String configPath = Optional.ofNullable(System.getProperty(CONFIG_PATH_KEY)).orElse(CONFIG_PATH);
        props.put("bootstrap.servers", readServerAddress(configPath));  //连接地址
        props.put("group.id", "custom-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");
        consumer = new KafkaConsumer(props);

        //根据时间段及过滤条件获取指定数据
        readMsgByTime(args[0], fetchStartTime, fetchEndTime, filterValue);
        log.info("search message finish!");
    }

    /**
     * 从指定文件中读取
     */
    private static String readServerAddress(String configPath) throws IOException, URISyntaxException {
        try {
            return Files.readAllLines(new File(configPath).toPath()).get(0);
        } catch (RuntimeException e) {
            log.error("读取文件异常");
            throw e;
        }
    }

    /**
     *
     * @param topic topic名称
     * @param fetchStartTime 开始时间戳
     * @param fetchEndTime 结束时间戳
     * @param filterValue 过滤数量
     */
    private static void readMsgByTime(String topic, long fetchStartTime, long fetchEndTime, String filterValue) {
        //根据起始时间获取每个分区的起始offset
        Map<TopicPartition, Long> map = new HashMap<>();
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        for (PartitionInfo par : partitions) {
            map.put(new TopicPartition(topic, par.partition()), fetchStartTime);
        }
        // 获取消息timeStamp大于等于fetchStartTime中最早偏移量
        Map<TopicPartition, OffsetAndTimestamp> parMap = consumer.offsetsForTimes(map);

        //遍历每个分区，将不同分区的数据写入不同文件中
        boolean notNeedFilter = filterValue.trim().length() == 0;
        boolean needFilter = true, isBreak = false;
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : parMap.entrySet()) {
            //topic-partition,eg:testTopic-0
            TopicPartition key = entry.getKey();
            //eg:(timestamp=1584684100465, offset=32382989)
            OffsetAndTimestamp value = entry.getValue();
            //根据消费里的timestamp确定offset
            if (value != null) {
                long offset = value.offset();
                consumer.assign(Arrays.asList(key));//订阅主题中指定的分区key.partition()
                consumer.seek(key, offset);
            } else {
                continue;
            }

            //拉取消息
            isBreak = false;
            FileWriter fw = null;
            try {
                fw = new FileWriter(new File("./" + key.toString() + ".txt"), true);
            } catch (IOException e) {
                e.printStackTrace();
            }
            while (true) {
                ConsumerRecords<String, String> poll = consumer.poll(1000);
                StringBuffer stringBuffer = new StringBuffer(20000);
                for (ConsumerRecord<String, String> record : poll) {
                    needFilter = notNeedFilter || (filterValue.trim().length() > 0 && record.value().contains(filterValue));
                    if (record.timestamp() <= fetchEndTime && needFilter) {
                        stringBuffer.append("offset:").append(record.offset())
                                .append(" timestamp:").append(parseStamp(record.timestamp()))
                                .append(" value:").append(record.value())
                                .append("\r\n");
                    } else if (record.timestamp() > fetchEndTime) {
                        isBreak = true;
                    }
                }
                if (poll.isEmpty()) {
                    isBreak = true;
                }
                try {
                    fw.write(stringBuffer.toString());
                    stringBuffer.setLength(0);
                    fw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (isBreak) {
                    break;
                }
            }
            try {
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 将时间转换为时间戳
     * @param s 日期字符串
     * @return
     * @throws ParseException
     */
    private static Long dateToStamp(String s) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        return date.getTime();
    }

    /**
     * 将时间戳转换成日期字符串
     * @param timestamp 时间戳
     * @return
     */
    private static String parseStamp(long timestamp) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-d HH:mm:ss");
        LocalDateTime localDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneOffset.ofHours(8)).toLocalDateTime();
        return localDateTime.format(formatter);
    }

}
