package cn.com.k2dat.k2assets.test.streaming.waterwark;

import cn.com.k2dat.k2assets.models.TUP;
import cn.com.k2dat.k2assets.models.UP;
import cn.com.k2dat.k2assets.schema.UpEventSchema;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import javax.sound.midi.Soundbank;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 水印或者水平线基本使用以及处理数据的乱序
 */
public class WaterMarkExam {


    /**
     * 通常在接收source数据后，应该立刻生成watermark；但是也可以在source后，通过简单的map或者filter操作后，
     * 再生成watermark
     * <p>
     * 注意： 如果指定多个watermark ，后面的会覆盖前面的watermark
     * <p>
     * <p>
     * <p>
     * watermark 生成方式：
     * <p>
     * 1. with Periodic watermark ：（这种方式使用比较常见）
     * 1-1： 周期性的触发watermark 的生成和发送。默认为100ms
     * 1-2： 间隔N秒自动向流里注入WATERMARK， 时间间隔由ExecutionConfig.setAutoWatermarkInterval()决定
     * 每次调用getCurrentWatermark方法，如果得到WATERMARK不为空并且比之前大就注入流中
     * <p>
     * 1-3： 可以定义一个最大的乱序时间。这种比较常见
     * 1-4： 声明watermark 需要实现AssignerWithPeriodicWatermarks接口
     * 2. with Punctuated watermark：
     * 2-1： 基于某些事件触发watermark生成和发送
     * 2-2： 基于事件向流中注入watermark， 每个元素都有机会判断是否生成了一个watermark，
     * 如果得到WATERMARK不为空并且比之前大就注入流中
     * 2-3： 声明watermark 需要实现AssignerWithPunctuatedWatermarks接口
     *
     *
     * 注意：TODO 在多并行度情况下：watermark对齐会取所有channel最小的watermark。
     * <p>
     * <p>
     * TODO window 窗口触发机制：
     * 系统事先，会按照自然时间将window划分，如果window的窗口大小为3s，那么1m中会把window窗口划分为：
     * [00:00:00 - 00:00:03)
     * [00:00:03 - 00:00:06)
     * [00:00:06 - 00:00:09)
     * [00:00:09 - 00:00:12)
     * ....
     * [00:00:57 - 00:00:60)
     * <p>
     * TODO window 窗口中的数据触发条件：
     * 1. watermark >= window_end_time
     * 2. 在[ window_start_time 和 window_end_time ) 有数据。注意，左闭右开区间
     * <p>
     * <p>
     * <p>
     * <p>
     * TODO 延迟数据的处理：
     * 1. 使用allowedLateness(Time.seconds(2)),指定运行数据的延迟时间
     * 就是在运行延迟的时间，还可以触发窗口。
     * 触发条件： watermark < window_end_time + allowedLateness时间内
     * 2. 通过sideOutputLateData(OutputTag) 分流延迟数据
     * 使用SingleOutputStreamOperator.getSideOutput(outputTag); 来获取分流数据
     */

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


        DataStream<TUP> dataStreamTUP = dataStreamSource.map(new MapFunction<UP, TUP>() {
            @Override
            public TUP map(UP value) throws Exception {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = simpleDateFormat.parse(value.accessTime);
                long accessTime = date.getTime();
                return new TUP(value.id, accessTime, value.userId, value.region);
            }
        });


        // 声明周期性 watermark
        DataStream<TUP> dataStreamWatermarks = dataStreamTUP.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<TUP>() {

            long currentMaxTimestamps = 0L;

            /**
             * 运行数据最大乱序时间,为1s
             *
             * TODO : maxOutOfOrderness 设置大小注意事项
             *   maxOutOfOrderness 太小，导致数据乱序或者丢弃比较多，影响数据准确度
             *   maxOutOfOrderness 太大，影响数据实时性，增加作业的复杂度
             *
             */

            final long maxOutOfOrderness = 1000L;

            /**
             * 生成 水印，并注入到数据中
             * @return
             */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamps - maxOutOfOrderness);
            }

            /**
             * 从数据中提取watermark
             * @param element
             * @param previousElementTimestamp
             * @return
             */
            @Override
            public long extractTimestamp(TUP element, long previousElementTimestamp) {
                long accessTime = element.accessTime;
                currentMaxTimestamps = Math.max(accessTime, currentMaxTimestamps);
                return accessTime;
            }
        });

        OutputTag<TUP> outputTag = new OutputTag<>("late-data-with-window");
        SingleOutputStreamOperator<ArrayList<TUP>> singleOutputStreamOperatorUserId = dataStreamWatermarks.keyBy("userId")
                // 间隔1s，计算一次最近10s内的窗口数据
                .timeWindow(Time.seconds(10), Time.seconds(1))
//                .allowedLateness(Time.seconds(2)) // 对late数据设置运行延迟时间
                .sideOutputLateData(outputTag) // 分流延迟数据到OutputTag中
                // 全量聚合（当窗口中的数据都到达之后，进行计算）
                .apply(new WindowFunction<TUP, ArrayList<TUP>, Tuple, TimeWindow>() {

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<TUP> input, Collector<ArrayList<TUP>> out) throws Exception {

                        //当前窗口的key，其值为keyBy指定列的值
                        String key = tuple.toString();
                        System.out.println("该窗口的key = " + key);

                        System.out.println("window start time = " + sdf.format(window.getStart()) + ", window end time = " + sdf.format(window.getEnd()));
                        ArrayList<TUP> tups = Lists.newArrayList(input.iterator());
                        Collections.sort(tups);
                        for (TUP tup : tups) {
                            System.out.println("排序之后的数据 = " + tup.toString());
                        }

                        System.out.println("-----------------------------------------------------------------------------");

                    }
                });


        // 获取延迟数据
        DataStream<TUP> sideOutput = singleOutputStreamOperatorUserId.getSideOutput(outputTag);
        sideOutput.print();

        singleOutputStreamOperatorUserId.print();


        env.execute(WaterMarkExam.class.getName());
    }


}
