package cn.com.k2dat.k2assets.test.streaming.waterwark;

import cn.com.k2dat.k2assets.models.TUP;
import cn.com.k2dat.k2assets.models.UP;
import cn.com.k2dat.k2assets.schema.UpEventSchema;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import scala.Tuple2;
import scala.collection.immutable.List;

import java.awt.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 窗口的基本使用和窗口的聚合分类
 */
public class WindowExam {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置并行度
        // 设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


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
         * 窗口基本使用:
         * window:是一种可以把无限的数据切割为有限数据块的手段
         * window分为： 时间驱动【time window】 和 数据驱动 【count window】
         * window 类型：
         *          1. 滚动窗口 【tumbling windows】数据没有重叠
         *          2. 滑动窗口  【sliding windows】数据有重叠
         *          3. 会话窗口  【session windows】
         *
         * API : timeWindow 和 timeWindowAll
         *   区别： timeWindow 是基于KeyByStream,进行切割数据的
         *         timeWindowAll 是基于DataStream，进行切割数据的
         *
         *
         *窗口聚合分类：
         *          增量聚合：窗口中每进入一条数据，进行计算一次
         *                   .timeWindow(Time.seconds(30), Time.seconds(5))
         *                   .reduce(reduceFunction)
         *                   /.aggregate(aggregateFunction)
         *                   /.max()
         *
         *
         *          全量聚合： 等属于窗口的数据到齐，才开始进行聚合计算【可以实现对窗口中的数据进行排序】
         *                  .timeWindow(Time.seconds(30), Time.seconds(5))
         *                  /.apply(windowFunction)
         *                  /.process(processWindowFunction)
         *
         *                  注意：processWindowFunction 比 windowFunction 提供更多的 上下文信息
         *
         *
         */

        DataStream<TUP> dataStreamTUP = dataStreamSource.map(new MapFunction<UP, TUP>() {
            @Override
            public TUP map(UP value) throws Exception {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = simpleDateFormat.parse(value.accessTime);
                long accessTime = date.getTime();
                return new TUP(value.id, accessTime, value.userId, value.region);
            }
        });

        DataStream<ArrayList<Tuple2<String, Long>>> dataStreamByUserId = dataStreamTUP.keyBy("userId") // userId 进行分组
                //设置滑动窗口
                // 第一个参数，窗口长度为：30s。 第二个参数：间隔5s，获取30s内的数据
                .timeWindow(Time.seconds(5), Time.seconds(1))
                // 增量聚合
                .aggregate(new AggregateFunction<TUP, ArrayList<TUP>, ArrayList<Tuple2<String, Long>>>() {

                    TreeMap<String, ArrayList<TUP>> treeMap = new TreeMap<>();

                    @Override
                    public ArrayList<TUP> createAccumulator() {
                        return new ArrayList<>();
                    }

                    @Override
                    public ArrayList<TUP> add(TUP value, ArrayList<TUP> accumulator) {
                        accumulator.add(value);
                        return accumulator;
                    }

                    @Override
                    public ArrayList<Tuple2<String, Long>> getResult(ArrayList<TUP> accumulator) {
                        ArrayList<Tuple2<String, Long>> results = new ArrayList<>();
                        // 根据userId 统计数量
                        for (TUP up : accumulator) {
                            if (treeMap.containsKey(up.userId)) {
                                ArrayList<TUP> list = treeMap.get(up.userId);
                                list.add(up);
                            } else {
                                ArrayList<TUP> list = new ArrayList<>();
                                list.add(up);
                                treeMap.put(up.userId, list);
                            }
                        }

                        Set<Map.Entry<String, ArrayList<TUP>>> entries = treeMap.entrySet();
                        for (Map.Entry<String, ArrayList<TUP>> entry : entries) {
                            results.add(new Tuple2<>(entry.getKey(), (long) entry.getValue().size()));
                        }
                        return results;
                    }

                    @Override
                    public ArrayList<TUP> merge(ArrayList<TUP> a, ArrayList<TUP> b) {
                        return null;
                    }
                });

        dataStreamByUserId.print();


        env.execute(WindowExam.class.getName());

    }
}
