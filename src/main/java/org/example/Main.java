package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.FileWriter;
import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "5000 ms");

        KafkaSource<String> stressSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
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
                .columnByExpression("ts", "CAST(eventTime AS TIMESTAMP_LTZ(3))")
                .columnByExpression("proc_time", "PROCTIME()")
                .watermark("ts", "ts - INTERVAL '2' SECOND")
                .build();
        Table stressInputTable = tableEnv.fromDataStream(rowStressStream, stressSchema);
        stressInputTable.printSchema();
        tableEnv.createTemporaryView("Stress", stressInputTable);

        KafkaSource<String> weightSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
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
            double weight = Double.parseDouble(values[2]);
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
        tableList.add(table1);

        Table table2 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, max(stressLevel) as max_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);
        tableList.add(table2);

        Table table3 = tableEnv.sqlQuery("""
                SELECT window_start, window_end,  max(stressLevel) as max_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '1' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end
                """);
        tableList.add(table3);

        Table table4 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, max(stressLevel) as max_stress
                FROM table(TUMBLE(table Stress, descriptor(ts), INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);
        tableList.add(table4);

        Table table5 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, max(stressLevel) as max_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '5' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);
        tableList.add(table5);

        Table table6 = tableEnv.sqlQuery("""
                SELECT window_start, window_end, id, max(stressLevel) as max_stress
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '1' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);
        tableList.add(table6);

        Table join = tableEnv.sqlQuery("""
                SELECT CAST(s.ts as TIMESTAMP), s.id, s.status, s.stressLevel, CAST(w.ts as TIMESTAMP), w.weight
                FROM Stress s, Weight w
                WHERE s.id = w.id
                AND w.ts BETWEEN s.ts AND s.ts + INTERVAL '10' SECOND
                """);
        tableList.add(join);

        /*
        Table join = tableEnv.sqlQuery("""
                SELECT *
                FROM table(HOP(table Stress, descriptor(ts), INTERVAL '1' seconds, INTERVAL '10' seconds))
                GROUP BY window_start, window_end, id
                """);
        tableList.add(table6);

         */

/*
        for (int i = 1; i < tableList.size(); i++) {
            DataStream<Row> resultStream = tableEnv.toDataStream(tableList.get(i));
            DataStream<String> outputStream = resultStream.map(Row::toString);
            //resultStream.print();
            Iterator<String> myOutput = DataStreamUtils.collect(outputStream);

            writeToFile("output"+i+".csv",myOutput);
        }

 */

        // no ciclo for per questioni di timing con il risultato delle query
        int i=3;
        DataStream<Row> resultStream = tableEnv.toDataStream(tableList.get(i-1));
        DataStream<String> outputStream = resultStream.map(Row::toString);
        //resultStream.print();
        Iterator<String> myOutput = DataStreamUtils.collect(outputStream);

        writeToFile("Files/Output/output"+String.valueOf(i)+".csv",myOutput);
        //writeToFile("Files/Output/join.csv",myOutput);

        /*final FileSink<String> sink = FileSink
                .forRowFormat(new Path("output"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(15))
                                .withInactivityInterval(Duration.ofMinutes(5))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                                .build())
                .build();

        outputStream.sinkTo(sink);

        env.execute("test");
         */
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
             // Adding a newline after each entry
        }
    }

    private static String formatString(String o) {
        o = o.replace("+", "")
                .replace(" ", "")
                .replace("I", "")
                .replace("T", " ")
                .replace("[", "")
                .replace("]", "");
        return o;
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
