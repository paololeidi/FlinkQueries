package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileWriter;
import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryComposition {
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final boolean QUERY3_ON_KSQLDB = true;
    private static final boolean QUERY1 = true;
    private static final boolean QUERY2 = true;
    private static final boolean QUERY3 = false;
    public static final String OUTPUT_FILE_NAME = "Files/Output/Networks/spark-flink-flink.csv";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "5000 ms");

        KafkaSource<String> stressSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVER)
                .setGroupId("my-group")
                .setTopics("stress")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stressStream = env.fromSource(stressSource, WatermarkStrategy.forMonotonousTimestamps(),"Kafka Source");

        DataStream<StressEvent> rowStressStream = stressStream.map(str -> {
            String[] values = str.split(",");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime localDateTime = LocalDateTime.parse(values[0], formatter);
            ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.of("Europe/Paris"));
            Instant ts = zonedDateTime.toInstant();
            int id = Integer.parseInt(values[1]);
            String status = values[2];
            int stress_level = Integer.parseInt(values[3]);
            return new StressEvent(id, status, stress_level, ts);
        });

        Schema stressSchema = Schema.newBuilder()
                .columnByExpression("ts", "CAST(eventTime AS TIMESTAMP_LTZ(3))") // eventTime is the field of the StressEventClass
                .columnByExpression("proc_time", "PROCTIME()")
                .watermark("ts", "ts - INTERVAL '2' SECOND")
                .build();
        Table stressInputTable = tableEnv.fromDataStream(rowStressStream, stressSchema);
        stressInputTable.printSchema();
        tableEnv.createTemporaryView("Stress", stressInputTable);

        KafkaSource<String> weightSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVER)
                .setGroupId("my-group")
                .setTopics("weight")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> weightStream = env.fromSource(weightSource, WatermarkStrategy.forMonotonousTimestamps(),"Kafka Source");

        DataStream<WeightEvent> rowWeightStream = weightStream.map(str -> {
            String[] values = str.split(",");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime localDateTime = LocalDateTime.parse(values[0], formatter);
            ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.of("Europe/Paris"));
            Instant ts = zonedDateTime.toInstant();
            int id = Integer.parseInt(values[1]);
            double weight = Math.round(Double.parseDouble(values[2]) * 100.0) / 100.0;
            return new WeightEvent(id, weight, ts);
        });

        Schema weightSchema = Schema.newBuilder()
                .columnByExpression("ts", "CAST(eventTime AS TIMESTAMP_LTZ(3))")
                .columnByExpression("proc_time", "PROCTIME()")
                .watermark("ts", "ts - INTERVAL '2' SECOND")
                .build();
        Table weightInputTable = tableEnv.fromDataStream(rowWeightStream, weightSchema);
        weightInputTable.printSchema();
        tableEnv.createTemporaryView("Weight", weightInputTable);

        Table table1 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, max(stressLevel) as max_stress
                FROM table(TUMBLE(table Stress, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table2 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, max(stressLevel) as max_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table3 = tableEnv.sqlQuery("""
                SELECT window_start, window_end,  max(stressLevel) as max_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '1' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table4 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, max(stressLevel) as max_stress
                FROM table(TUMBLE(table Stress, descriptor(ts), INTERVAL '2' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table5 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, max(stressLevel) as max_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table6 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, max(stressLevel) as max_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '1' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);


        Table table7 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, min(stressLevel) as min_stress
                FROM table(TUMBLE(table Stress, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table8 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, min(stressLevel) as min_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table9 = tableEnv.sqlQuery("""
                SELECT window_start, window_end,  min(stressLevel) as min_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '1' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table10 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, min(stressLevel) as min_stress
                FROM table(TUMBLE(table Stress, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table11 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, min(stressLevel) as min_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table12 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, min(stressLevel) as min_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '1' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table13 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, avg(weight) as avg_weight
                FROM table(TUMBLE(table Weight, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table14 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, avg(weight) as avg_weight
                FROM table(HOP(table Weight, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table15 = tableEnv.sqlQuery("""
                SELECT window_start, window_end,  avg(weight) as avg_weight
                FROM table(HOP(table Weight, descriptor(ts), INTERVAL '1' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table16 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, avg(weight) as avg_weight
                FROM table(TUMBLE(table Weight, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table17 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, avg(weight) as avg_weight
                FROM table(HOP(table Weight, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table18 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, avg(weight) as avg_weight
                FROM table(HOP(table Weight, descriptor(ts), INTERVAL '1' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table19 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, count(*) as numberOfEvents
                FROM table(TUMBLE(table Weight, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table20 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, count(*) as numberOfEvents
                FROM table(HOP(table Weight, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table21 = tableEnv.sqlQuery("""
                SELECT window_start, window_end,  count(*) as numberOfEvents
                FROM table(HOP(table Weight, descriptor(ts), INTERVAL '1' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table22 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, count(*) as numberOfEvents
                FROM table(TUMBLE(table Weight, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table23 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, count(*) as numberOfEvents
                FROM table(HOP(table Weight, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table24 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, count(*) as numberOfEvents
                FROM table(HOP(table Weight, descriptor(ts), INTERVAL '1' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);


        Table join = tableEnv.sqlQuery("""
                SELECT CAST(s.ts as TIMESTAMP), s.id, s.status, s.stressLevel, CAST(w.ts as TIMESTAMP), w.weight
                FROM Stress s, Weight w
                WHERE s.id = w.id
                AND w.ts BETWEEN s.ts - INTERVAL '5' SECONDS AND s.ts + INTERVAL '5' SECOND
                """);

        // Writing output into files/kafka
        DataStream<Row> resultStream = tableEnv.toDataStream(table4);
        DataStream<String> flattenResultStream = resultStream.map(Row::toString);
        //resultStream.print();
        Iterator<String> myOutput = DataStreamUtils.collect(flattenResultStream);

        //writeToFile("Files/Output/output"+String.valueOf(i)+".csv",myOutput);
        //writeToFile("Files/Output/output4.csv",myOutput);
        ExecutorService executorService = Executors.newFixedThreadPool(4);

        if (QUERY1){
            executorService.submit(() -> {
                for (Iterator<String> it = myOutput; it.hasNext(); ) {
                    String line = it.next();
                    System.out.println("Line: "+line);
                    line = formatString(line);
                    System.out.println("Formatted line: "+line);
                    writeToKafka("output", line);
                }

            });
        }


        KafkaSource<String> outputSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVER)
                .setGroupId("my-group")
                .setTopics("output")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> outputStream = env.fromSource(outputSource, WatermarkStrategy.forMonotonousTimestamps(),"Kafka Source");

        DataStream<OutputEvent> outputEventStream = outputStream.map(str -> {
            String[] values = str.split(",");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime windowOpenLDT = LocalDateTime.parse(values[0], formatter);
            ZonedDateTime windowOpenZDT = windowOpenLDT.atZone(ZoneId.of("Europe/Paris"));
            Instant windowOpen = windowOpenZDT.toInstant();
            LocalDateTime windowCloseLDT = LocalDateTime.parse(values[1], formatter);
            ZonedDateTime windowCloseZDT = windowCloseLDT.atZone(ZoneId.of("Europe/Paris"));
            Instant windowClose = windowCloseZDT.toInstant();
            int id = Integer.parseInt(values[2]);
            int maxStress = Integer.parseInt(values[3]);
            return new OutputEvent(windowOpen, windowClose, id, maxStress);
        });

        Schema outputSchema = Schema.newBuilder()
                .columnByExpression("ts", "CAST(windowClose AS TIMESTAMP_LTZ(3))")
                .columnByExpression("proc_time", "PROCTIME()")
                .watermark("ts", "ts - INTERVAL '2' SECOND")
                .build();
        Table outputInputTable = tableEnv.fromDataStream(outputEventStream, outputSchema);
        outputInputTable.printSchema();
        tableEnv.createTemporaryView("Output", outputInputTable);


        //Table table4_2 = tableEnv.sqlQuery("SELECT * FROM Output");


        // Second level queries
        Table table4_2 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, max(maxStressLevel) as max_stress
                FROM table(HOP(table Output, descriptor(ts), INTERVAL '2' seconds, INTERVAL '4' seconds))
                GROUP BY window_start, window_end, id
                """);

        table4_2.printSchema();

        // Writing output into files/kafka
        DataStream<Row> resultStream2 = tableEnv.toDataStream(table4_2);
        DataStream<String> sinkStream2 = resultStream2.map(Row::toString);

        Iterator<String> myOutput2;
        if (!QUERY3_ON_KSQLDB)
            myOutput2 = DataStreamUtils.collect(sinkStream2);
        else {
            DataStream<String> jsonSinkStream2 = sinkStream2.map(new MapFunction<String, String>() {
                private final ObjectMapper objectMapper = new ObjectMapper();

                @Override
                public String map(String value) throws Exception {
                    // Parse the original string into a Row or a custom structure
                    // Assuming the original string is comma-separated for this example
                    String[] fields = value.split(",");

                    // Extract the required fields
                    String windowOpen = fields[0].trim();
                    String windowClose = fields[1].trim();
                    String id = fields[2].trim();
                    String maxStress = fields[3].trim();

                    // Create an instance of JsonRow
                    JsonRow jsonRow = new JsonRow(windowOpen, windowClose, id, maxStress);

                    // Convert the JsonRow instance to a JSON string
                    System.out.println("Json line: " + objectMapper.writeValueAsString(jsonRow));
                    return objectMapper.writeValueAsString(jsonRow);
                }
            });
            myOutput2 = DataStreamUtils.collect(jsonSinkStream2);
        }

        //writeToFile("Files/Output/output"+String.valueOf(i)+".csv",myOutput);
        //writeToFile("Files/Output/output4_2.csv",myOutput2);

        if (QUERY2){
            executorService.submit(() -> {
                for (Iterator<String> it = myOutput2; it.hasNext(); ) {
                    String line = it.next();
                    System.out.println("Line: "+line);
                    line = formatString(line);
                    System.out.println("Formatted line: "+line);
                    writeToKafka("output2", line);
                }
            });
        }

        KafkaSource<String> outputSource2 = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVER)
                .setGroupId("my-group")
                .setTopics("output2")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> outputStream2 = env.fromSource(outputSource2, WatermarkStrategy.forMonotonousTimestamps(),"Kafka Source");

        DataStream<OutputEvent> outputEventStream2 = outputStream2.map(str -> {
            String[] values = str.split(",");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime windowOpenLDT = LocalDateTime.parse(values[0], formatter);
            ZonedDateTime windowOpenZDT = windowOpenLDT.atZone(ZoneId.of("Europe/Paris"));
            Instant windowOpen = windowOpenZDT.toInstant();
            LocalDateTime windowCloseLDT = LocalDateTime.parse(values[1], formatter);
            ZonedDateTime windowCloseZDT = windowCloseLDT.atZone(ZoneId.of("Europe/Paris"));
            Instant windowClose = windowCloseZDT.toInstant();
            int id = Integer.parseInt(values[2]);
            int maxStress = Integer.parseInt(values[3]);
            return new OutputEvent(windowOpen, windowClose, id, maxStress);
        });

        Table outputInputTable2 = tableEnv.fromDataStream(outputEventStream2, outputSchema);
        outputInputTable2.printSchema();
        tableEnv.createTemporaryView("Output2", outputInputTable2);


        //Table table4_2 = tableEnv.sqlQuery("SELECT * FROM Output");


        // Second level queries
        Table table4_3 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, max(maxStressLevel) as max_stress
                FROM table(TUMBLE(table Output2, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);


        table4_3.printSchema();


        // Writing output into files/kafka
        DataStream<Row> resultStream3 = tableEnv.toDataStream(table4_3);
        DataStream<String> sinkStream3 = resultStream3.map(Row::toString);
        //resultStream.print();
        Iterator<String> myOutput3 = DataStreamUtils.collect(sinkStream3);

        if (QUERY3){
            executorService.submit(() -> {
                for (Iterator<String> it = myOutput3; it.hasNext(); ) {
                    String line = it.next();
                    System.out.println("Line: "+line);
                    line = formatString(line);
                    System.out.println("Formatted line: "+line);
                    writeToKafka("output3", line);
                    writeToFile(OUTPUT_FILE_NAME, line);
                }

            });
        }
    }

    private static void writeToFile(String filename, String o) {
        try {
            FileWriter csvWriter = new FileWriter(filename,true);
            csvWriter.append(o); // Writing the transformed string to the CSV file
            csvWriter.append("\n");
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the file: " + e.getMessage());
        }
    }

    private static void writeToKafka(String topic, String line) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic,line);
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static String formatString(String line) {
        line = line.replace("+", "")
                .replace(" ", "")
                .replace("I", "")
                .replace("T", " ")
                .replace("[", "")
                .replace("]", "");
        String[] fields = line.split(",");
        Pattern timestampPattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}");

        for (int i = 0; i < fields.length; i++) {
            Matcher matcher = timestampPattern.matcher(fields[i]);
            if (matcher.matches()) {
                System.out.println("change: " + fields[i]);
                fields[i] = fields[i] + ":00";
                System.out.println("in: " + fields[i]);
            }
        }

        StringJoiner joiner = new StringJoiner(",");
        for (String field : fields) {
            joiner.add(field);
        }
        return joiner.toString();
    }

    public static class StressEvent {
        public Integer id;
        public String status;
        public Integer stressLevel;

        public Instant eventTime;

        // default constructor for DataStream API
        public StressEvent() {}

        // fully assigning constructor for Table API
        public StressEvent(Integer id, String status, Integer stressLevel, Instant eventTime){
            this.id = id;
            this.status = status;
            this.stressLevel = stressLevel;
            this.eventTime = eventTime;
        }

    }

    public static class WeightEvent {
        public Integer id;
        public Double weight;
        public Instant eventTime;

        // default constructor for DataStream API
        public WeightEvent() {}

        // fully assigning constructor for Table API
        public WeightEvent(Integer id, Double weight, Instant eventTime){
            this.id = id;
            this.weight = weight;
            this.eventTime = eventTime;
        }
    }

    public static class OutputEvent {

        public Instant windowOpen;
        public Instant windowClose;
        public Integer id;
        public Integer maxStressLevel;



        // default constructor for DataStream API
        public OutputEvent() {}

        // fully assigning constructor for Table API
        public OutputEvent(Instant windowOpen, Instant windowClose, Integer id, Integer maxStressLevel){
            this.windowOpen = windowOpen;
            this.windowClose = windowClose;
            this.id = id;
            this.maxStressLevel = maxStressLevel;
        }

    }

    static class JsonRow {
        public String windowOpen;
        public String windowClose;
        public String id;
        public String maxStress;

        public JsonRow(String windowOpen, String windowClose, String id, String maxStress) {
            this.windowOpen = windowOpen;
            this.windowClose = windowClose;
            this.id = id;
            this.maxStress = maxStress;
        }
    }

}
