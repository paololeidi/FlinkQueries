package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.FileWriter;
import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "5000 ms");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("my-group")
                .setTopics("stress")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(),"Kafka Source");

        DataStream<Event> rowStream = dataStream.map(str -> {
            String[] values = str.split(",");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime localDateTime = LocalDateTime.parse(values[0], formatter);
            ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.of("Europe/Paris"));
            Instant ts = zonedDateTime.toInstant();
            int id = Integer.parseInt(values[1]);
            String status = values[2];
            int stress_level = Integer.parseInt(values[3]);
            return new Event(id, status, stress_level, ts);
        });


        Table inputTable = tableEnv.fromDataStream(rowStream);
        inputTable.printSchema();



        Schema schema = Schema.newBuilder()
                .columnByExpression("ts", "CAST(eventTime AS TIMESTAMP_LTZ(3))")
                .columnByExpression("proc_time", "PROCTIME()")
                .watermark("ts", "ts - INTERVAL '2' SECOND")
                .build();
        Table inputTable2 = tableEnv.fromDataStream(rowStream, schema);
        inputTable2.printSchema();
        tableEnv.createTemporaryView("InputTable", inputTable2);

        //Query 1:  Write a continuous query that emits the max stress for each arm.
        Table windowedTable_1 = tableEnv.sqlQuery("SELECT id, max(stressLevel) as max_stress from InputTable group by id");


        //Query 2:  Write a continuous query that emits the max stress for each arm every 10 seconds
        Table windowedTable_2 = tableEnv.sqlQuery("SELECT window_start, window_end, id, max(stressLevel) as max_stress" +
                "FROM table(TUMBLE(table InputTable, descriptor(ts), INTERVAL '10' seconds)) group by window_start, window_end, id");

        /*
        //Query 3: A continuous query that emits the average stress level between a pick (status==goodGrasped) and a place (status==placingGood).
        Table windowedTable_3 = tableEnv.sqlQuery("SELECT A.id,A.ts, A.window_start, A.window_end, (A.stress_level+B.stress_level)/2 as Avg_stress_level " +
                "from (select * from table(HOP(table InputTable, descriptor(ts), INTERVAL '5' seconds, interval '10' seconds))) A " +
                "full join (select * from table(HOP(table InputTable, descriptor(ts), INTERVAL '5' seconds, interval '10' seconds))) B " +
                "on A.id = B.id and A.window_start = B.window_start and A.window_end = B.window_end " +
                "where A.status = 'good grasped' and B.status = 'placing a good' and A.ts < B.ts");

         */


        //Query 3: A continuous query that returns the robotic arms that:
        //in less than 10 second,
        //picked a good while safely operating,
        //moved it while the controller was raising a warning, and
        //placed it while safely operating again.

        //Query 4: A continuous query that monitors the results of the previous one (i.e., Q3)
        //and counts how many times each robotic arm is present in the stream over a window of 10 seconds updating the counting every 2 seconds.


        DataStream<Row> resultStream = tableEnv.toDataStream(windowedTable_2);
        DataStream<String> outputStream = resultStream.map(Row::toString);
        //resultStream.print();
        Iterator<String> myOutput = DataStreamUtils.collect(outputStream);

        for (Iterator<String> it = myOutput; it.hasNext(); ) {
            String o = it.next();
            o = o.replace("+", "")
                    .replace(" ", "")
                    .replace("I", "")
                    .replace("T", " ")
                    .replace("[", "")
                    .replace("]", "");
            System.out.println(o);

            try {
                FileWriter csvWriter = new FileWriter("output.csv",true);
                csvWriter.append(o); // Writing the transformed string to the CSV file
                csvWriter.append("\n");
                csvWriter.flush();
                csvWriter.close();
            } catch (IOException e) {
                System.out.println("An error occurred while writing to the file: " + e.getMessage());
            }
             // Adding a newline after each entry
        }

        final FileSink<String> sink = FileSink
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
    }

    public static class Event {
        public Integer id;
        public String status;
        public Integer stressLevel;

        public Instant eventTime;

        // default constructor for DataStream API
        public Event() {}

        // fully assigning constructor for Table API
        public Event(Integer id, String status, Integer stressLevel, Instant eventTime){
            this.id = id;
            this.status = status;
            this.stressLevel = stressLevel;
            this.eventTime = eventTime;
        }
    }

}
