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
import org.apache.flink.table.api.*;
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

public class Main {
    private static final String BOOTSTRAP_SERVER = "localhost:19092";
    private static final boolean KSQLDB_SINK = true;

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





        ArrayList<Table> tableList = new ArrayList<>();

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
                SELECT window_start, window_end, id, max(stressLevel) as max_stress
                FROM table(TUMBLE(table Stress, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table4 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, max(stressLevel) as max_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        tableEnv.executeSql("""
                CREATE VIEW query5 AS
                SELECT window_start, window_end, id, max(stressLevel) as max_stress
                FROM table(SESSION(table Stress, descriptor(ts), INTERVAL '3' seconds))
                GROUP BY window_start, window_end, id
                """);

        // create an output Table
        final Schema schema = Schema.newBuilder()
                .column("window_start", DataTypes.TIMESTAMP())
                .column("window_end", DataTypes.TIMESTAMP())
                .column("id", DataTypes.INT())
                .column("max_stress", DataTypes.INT())
                .build();

        tableEnv.createTemporaryTable("CsvSinkTable", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", "/Files")
                .format(FormatDescriptor.forFormat("csv")
                        .option("field-delimiter", ",")
                        .build())
                .build());

        Table table5 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, max(stressLevel) as max_stress
                FROM table(SESSION(table Stress, descriptor(ts), INTERVAL '3' seconds))
                GROUP BY window_start, window_end, id
                """);

        TablePipeline pipeline = table5.insertInto("CsvSinkTable");

        // pipeline.execute();


        Table table6 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, min(stressLevel) as min_stress
                FROM table(TUMBLE(table Stress, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table7 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, min(stressLevel) as min_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table8 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, min(stressLevel) as min_stress
                FROM table(TUMBLE(table Stress, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table9 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, min(stressLevel) as min_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table10 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, min(stressLevel) as min_stress
                FROM table(SESSION(table Stress, descriptor(ts), INTERVAL '3' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table11 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, avg(weight) as avg_weight
                FROM table(TUMBLE(table Weight, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table12 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, avg(weight) as avg_weight
                FROM table(HOP(table Weight, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table13 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, avg(weight) as avg_weight
                FROM table(TUMBLE(table Weight, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table14 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, avg(weight) as avg_weight
                FROM table(HOP(table Weight, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table15 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, avg(weight) as avg_weight
                FROM table(SESSION(table Weight, descriptor(ts), INTERVAL '3' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table16 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, count(*) as numberOfEvents
                FROM table(TUMBLE(table Weight, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table17 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, count(*) as numberOfEvents
                FROM table(HOP(table Weight, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);

        Table table18 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, count(*) as numberOfEvents
                FROM table(TUMBLE(table Weight, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table19 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, count(*) as numberOfEvents
                FROM table(HOP(table Weight, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);

        Table table20 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, count(*) as numberOfEvents
                FROM table(SESSION(table Weight, descriptor(ts), INTERVAL '3' seconds))
                GROUP BY window_start, window_end, id
                """);


        Table join = tableEnv.sqlQuery("""
                SELECT CAST(s.ts as TIMESTAMP), s.id, s.status, s.stressLevel, CAST(w.ts as TIMESTAMP), w.weight
                FROM Stress s, Weight w
                WHERE s.id = w.id
                AND w.ts BETWEEN s.ts - INTERVAL '5' SECONDS AND s.ts + INTERVAL '5' SECOND
                """);
        tableList.add(join);

        // Writing output into files/kafka

        DataStream<Row> resultStream = tableEnv.toDataStream(table5);
        DataStream<String> flattenResultStream = resultStream.map(Row::toString);
        //resultStream.print();
        Iterator<String> myOutput = DataStreamUtils.collect(flattenResultStream);

        writeToFile("Files/Output/output5.csv",myOutput);


    }

    private static void writeToFile(String filename, Iterator<String> myOutput) {
        for (Iterator<String> it = myOutput; it.hasNext(); ) {
            String o = it.next();
            o = formatString(o);
            System.out.println(o);

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
                //fields[i] = fields[i] + ":00";
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

}
