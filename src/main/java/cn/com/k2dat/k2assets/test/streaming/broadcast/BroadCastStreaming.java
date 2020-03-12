package cn.com.k2dat.k2assets.test.streaming.broadcast;

import cn.com.k2dat.k2assets.models.AlertRule;
import cn.com.k2dat.k2assets.models.MetricEvent;
import cn.com.k2dat.k2assets.schema.MetricsEventSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class BroadCastStreaming {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromSystemProperties();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(1); // 设置并行度

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "flink-partiton-group");
        props.put("auto.offset.reset", "latest");

        /**
         * kafka 数据源
         */
        DataStreamSource<MetricEvent> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<>(
                "flink-partition",
                new MetricsEventSchema(),
                props
        ));


        //广播流数据

        /**
         * //定时从数据库中查出告警规则数据, 作为广播流数据
         */
        DataStreamSource<List<AlertRule>> alarmDataStream = env.addSource(new RichSourceFunction<List<AlertRule>>() {

            private ParameterTool parameterTool = null;
            private boolean isRunning = true;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                // 1. 获取动态参数
                parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                // 2. 打开数据库连接 (需要的访问数据库时)

            }

            @Override
            public void run(SourceContext<List<AlertRule>> ctx) throws Exception {

                // 生成数据
                while (isRunning) {
                    List<AlertRule> list = new ArrayList<>();
                    AlertRule alertRule = new AlertRule();
                    alertRule.id = 1;
                    alertRule.name = "UC100";
                    alertRule.measurement = "测量大小";
                    alertRule.thresholds = "50";

                    list.add(alertRule);

                    ctx.collect(list);

                    list.clear();
                }


            }

            @Override
            public void cancel() {

                isRunning = false;

            }
        }).setParallelism(1);

        /**
         * 定义广播规则
         */
        MapStateDescriptor<String, AlertRule> ALERT_RULE = new MapStateDescriptor<>(
                "alert_rule",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(AlertRule.class));

        /**
         * 连接广播流
         */
        dataStreamSource.connect(alarmDataStream.broadcast(ALERT_RULE))
                .process(new BroadcastProcessFunction<MetricEvent, List<AlertRule>, MetricEvent>() {
                    @Override
                    public void processElement(MetricEvent value, ReadOnlyContext ctx, Collector<MetricEvent> out) throws Exception {
                        ReadOnlyBroadcastState<String, AlertRule> broadcastState = ctx.getBroadcastState(ALERT_RULE);

                        // 根据name，查找告警规则中的数据是否存在
                        if (broadcastState.contains(value.name)) {
                            AlertRule alertRule = broadcastState.get(value.name);
                            // 取出指标数据中的阀值
                            double used = (double) value.fields.get(alertRule.measurement);
                            // 比较阀值
                            if (used > Double.valueOf(alertRule.thresholds)) {
                                System.out.println("AlertRule =  " +alertRule+", MetricEvent = " + value);
                                out.collect(value);
                            }
                        }

                    }

                    @Override
                    public void processBroadcastElement(List<AlertRule> value, Context ctx, Collector<MetricEvent> out) throws Exception {

                        if (value == null || value.size() == 0) {
                            return;
                        }
                        // 添加告警规则数据到BroadcastState
                        BroadcastState<String, AlertRule> broadcastState = ctx.getBroadcastState(ALERT_RULE);
                        for (AlertRule alertRule : value) {
                            broadcastState.put(alertRule.name, alertRule);
                        }
                    }
                });

        env.execute("broadCastStreaming");
    }
}
