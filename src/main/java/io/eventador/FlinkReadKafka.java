package io.eventador;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;



public class FlinkReadKafka {

    public static final String TOPIC_ARG = "topic";


    public static void main(String[] args) throws Exception {
        // Read parameters from command line


        final ParameterTool params = ParameterTool.fromArgs(args);

        if(params.getNumberOfParameters() < 3) {
            System.out.println("\nUsage: FlinkReadKafka --topic <topic> --bootstrap.servers <kafka brokers> --group.id <groupid>");
            return;
        }

        String topic = params.get(FlinkReadKafka.TOPIC_ARG);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(300000); // 300 seconds
        env.getConfig().setGlobalJobParameters(params);



        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(),params.getProperties()));

        // Print Kafka messages to stdout - will be visible in logs
        messageStream.print();

        env.execute("FlinkReadKafka");
    }
}


