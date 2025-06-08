package com.example.featurestoreforjoycastle;

import com.alibaba.fastjson.JSON;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConsumerMultiThread {
    private static final String GROUPID = "codingce_consumer_a";
    private static final Set<String> EVENT_TYPES = new HashSet<>(Arrays.asList("InAppPurchase", "SessionEnd"));
    private static final List<String> eventFields = Arrays.asList(
            "EventID",
            "PlayerID",
            "EventTimestamp",
            "EventType",
            "EventDetails",
            "DeviceType",
            "Location"); // 示例字段
    private static final int BATCH_SIZE = 1000;
    private static final Map<String, List<List<Object>>> buffers = new HashMap<>();

    // Reusable DB connection
    private static final ExecutorService executorService = Executors.newFixedThreadPool(8); // 创建一个固定大小的线程池

    private static HikariDataSource dataSource;

    public static void main(String[] args) throws SQLException {
        long startTime = System.currentTimeMillis();

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        p.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(p);

        // HikariCP 配置
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:sqlite:game_events.db");
        config.setMaximumPoolSize(4); // 设置连接池的最大连接数
        config.setAutoCommit(true); // 自动提交
        dataSource = new HikariDataSource(config);

        try (Connection conn = dataSource.getConnection()) {
            for (String eventType : eventFields) {
                String table = "dwd_" + eventType;

                String createTableSQL = "CREATE TABLE IF NOT EXISTS " + table + " (" +
                        "EventID TEXT, " +
                        "PlayerID TEXT, " +
                        "EventTimestamp TEXT, " +
                        "EventType TEXT, " +
                        "EventDetails TEXT, " +
                        "DeviceType TEXT, " +
                        "Location TEXT" +
                        ")";
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(createTableSQL);
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
        }
        // Reuse database connection
        try {
            // 订阅消息
            kafkaConsumer.subscribe(Collections.singletonList("game_events"));

            // 调用 poll() 让消费者分配分区
            kafkaConsumer.poll(100);
            List<PartitionInfo> partitions = kafkaConsumer.partitionsFor("game_events");
            for (PartitionInfo partitionInfo : partitions) {
                TopicPartition partition = new TopicPartition("game_events", partitionInfo.partition());
                // 手动将偏移量设置为 0
                kafkaConsumer.seek(partition, 0);
            }

            // 消费数据
            do {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);

                if (records.isEmpty()) {
                    long endTime = System.currentTimeMillis();
                    long elapsedTime = endTime - startTime;
                    // 5.78s
                    System.out.println("Elapsed time: " + elapsedTime + " milliseconds");
                    break;
                }

                // 处理每一条消息
                for (ConsumerRecord<String, String> record : records) {
                    Map data = JSON.parseObject(record.value(), Map.class);
                    String eventType = data.get("EventType").toString();

                    if (EVENT_TYPES.contains(eventType)) {
                        String eventDetails = (String) data.get("EventDetails");

                        // 使用正则表达式提取金额或停留时长
                        Pattern pattern = Pattern.compile("(\\d+\\.\\d+|\\d+)");
                        Matcher matcher = pattern.matcher(eventDetails);

                        if (matcher.find()) {
                            data.put("EventDetails", Float.parseFloat(matcher.group(0)));
                        }
                    }

                    // 如果数据中缺少任何必要字段，跳过
                    if (!data.keySet().containsAll(eventFields)) {
                        continue;
                    }

                    // 按字段顺序提取数据并存储到缓冲区
                    List<Object> row = new ArrayList<>();
                    for (String field : eventFields) {
                        row.add(data.get(field));
                    }

                    buffers.computeIfAbsent(eventType, k -> new ArrayList<>()).add(row);

                    // 如果缓冲区大小达到批量大小，执行插入
                    if (buffers.get(eventType).size() >= BATCH_SIZE) {
                        List<List<Object>> batch = new ArrayList<>(buffers.get(eventType));
                        buffers.get(eventType).clear();
                        // 使用线程池提交任务进行批量插入
                        executorService.submit(() -> {
                            insertBatch(batch, eventType);
                        });
                    }


                }

            } while (true);

        } finally {
            executorService.shutdown(); // 关闭线程池
        }
    }

    // 模拟批量插入的方法
    public static void insertBatch(List<List<Object>> rows, String eventType) {
        if (rows == null || rows.isEmpty()) {
            return;
        }

        String table = "dwd_" + eventType;
        try (Connection conn = dataSource.getConnection()) { // 每个线程独享连接
            // 开始事务
            conn.setAutoCommit(false);

            // 使用 PreparedStatement 插入数据
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO " + table + " (EventID, PlayerID, EventTimestamp, EventType, EventDetails, DeviceType, Location) VALUES (?, ?, ?, ?, ?, ?, ?)")) {

                // 批量添加数据
                for (List<Object> row : rows) {
                    pstmt.setString(1, row.get(0).toString());
                    pstmt.setString(2, row.get(1).toString());
                    pstmt.setString(3, row.get(2).toString());
                    pstmt.setString(4, row.get(3).toString());
                    pstmt.setString(5, row.get(4).toString());
                    pstmt.setString(6, row.get(5).toString());
                    pstmt.setString(7, row.get(6).toString());
                    pstmt.addBatch(); // 添加到批量中
                }
                pstmt.executeBatch(); // 执行批量插入
                // 提交事务
                conn.commit();

            } catch (SQLException e) {
                conn.rollback(); // 如果出现错误，回滚事务
                e.printStackTrace();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
