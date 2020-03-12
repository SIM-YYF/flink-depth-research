package cn.com.k2dat.k2assets.test.streaming.state;

import cn.com.k2dat.k2assets.models.UP;
import cn.com.k2dat.k2assets.schema.UpEventSchema;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

/**
 * keyState
 */
public class KeyStateExm {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置并行度

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "flink-partiton-group");
        props.put("auto.offset.reset", "latest");
        DataStreamSource<UP> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<UP>(
                "flink-partition",
                new UpEventSchema(),
                props
        ));

        /**
         * 状态存活时间的配置
         *
         * 注意事项 TODO
         * 1. 目前仅支持参考 processing time 的 TTL
         *
         * 2. 使用启用 TTL 的描述符去尝试恢复先前未使用 TTL 配置的状态可能会导致兼容性失败或者 StateMigrationException 异常。
         *
         * 3. TTL 配置并不是 Checkpoint 和 Savepoint 的一部分，而是 Flink 如何在当前运行的 Job 中处理它的方式。
         *
         */
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(5)) // 代表着状态存活时间
                ////配置状态 TTL 更新类型： OnCreateAndWrite: 仅限创建和写入访问时更新。OnReadAndWrite: 除了创建和写入访问，还支持在读取时更新
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                //是否在读取访问时返回过期值: NeverReturnExpired: 永远不会返回过期值. ReturnExpiredIfNotCleanedUp: 如果仍然可用则返回
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                // 禁用后台清理。默认：后台清理
                .disableCleanupInBackground()
                // 清理策略
//                .cleanupFullSnapshot()
//                .cleanupIncrementally()
                .build();

        DataStream<Tuple2<String, Long>> dataStreamKeyByRegion = dataStreamSource
                .keyBy("region")
                .flatMap(new RichFlatMapFunction<UP, Tuple2<String, Long>>() {

                    private transient ValueState<Tuple2<String, Long>> regionNumState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        // 统计区域数量
                        ValueStateDescriptor<Tuple2<String, Long>> upValueStateDescriptor = new ValueStateDescriptor<Tuple2<String, Long>>(
                                "region_num",
                                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                                }));

                        // 启用TTL(存活时间)
                        upValueStateDescriptor.enableTimeToLive(ttlConfig);

                        regionNumState = getRuntimeContext().getState(upValueStateDescriptor);


                        // listState
                        ListStateDescriptor<List<Tuple2<String, Long>>> listStateDescriptor = new ListStateDescriptor<List<Tuple2<String, Long>>>(
                                "list_regions",
                                TypeInformation.of(new TypeHint<List<Tuple2<String, Long>>>() {
                                })
                        );
                        getRuntimeContext().getListState(listStateDescriptor);


                        MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<String, Long>(
                                "map_state",
                                TypeInformation.of(new TypeHint<String>() {
                                }),
                                TypeInformation.of(new TypeHint<Long>() {
                                })
                        );
                        getRuntimeContext().getMapState(mapStateDescriptor);


                    }

                    @Override
                    public void flatMap(UP value, Collector<Tuple2<String, Long>> out) throws Exception {

                        Tuple2<String, Long> region = regionNumState.value();

                        if (region == null) {
                            region = new Tuple2<>();
                        }
                        // 区域相等，累加计数
                        if (value.region.equals(region.f0)) {
                            region.f1 += 1;
                        } else {
                            region.f0 = value.region;
                            region.f1 = 1L;
                        }

                        regionNumState.update(region);

                        if (region.f1 >= 4) {
                            out.collect(region);
                            regionNumState.clear();
                        }

                    }
                });


        dataStreamKeyByRegion.print();

        env.execute("key state");
    }
}
