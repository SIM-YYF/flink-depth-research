package cn.com.k2dat.k2assets.test.streaming.watermark2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author: ywf
 * @date: 2020-05-31
 * @description:
 */
public class WindowExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置并行度
        // 设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> textStream = env.socketTextStream("localhost", 9999, "\n");

        DataStream<Tuple2<String, Long>> andWatermarks = textStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {

                return new Tuple2<>(value.split(",")[0], Long.parseLong(value.split(",")[1]));
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            final Long maxOutofOrderness = 10000L;
            long currentMaxTimeStamp = 0L;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimeStamp - maxOutofOrderness);
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                String key = element._1();
                long timestamp = element._2();
                currentMaxTimeStamp = Math.max(timestamp, currentMaxTimeStamp);
                System.out.println("key:" + key + "," +
                        "event-time:[" + timestamp + "|" + sdf.format(timestamp) + "], " +
                        "currentMaxTimestamp:[" + currentMaxTimeStamp + "|" +
                        sdf.format(currentMaxTimeStamp) + "]," +
                        "watermark:[" + getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()) + "]");


                return timestamp;
            }
        });


        DataStream<String> apply = andWatermarks.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value._1;
            }
        })
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//                .allowedLateness(Time.seconds(2))
                .apply(new WindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        String key = s;
                        List<Long> arrarList = new ArrayList<Long>();
                        Iterator<Tuple2<String, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            arrarList.add(next._2);
                        }
                        Collections.sort(arrarList);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = "key: " + key + ", " + arrarList.size() + ", " +
                                "first_time = " + sdf.format(arrarList.get(0)) + ", " +
                                "last_time = " + sdf.format(arrarList.get(arrarList.size() - 1)) + ", " +
                                "window_time= [ " + sdf.format(window.getStart()) + "," + sdf.format(window.getEnd()) + " ]";


                        out.collect(result);

                    }
                });



        apply.print();

        env.execute("xxx");
    }
}
