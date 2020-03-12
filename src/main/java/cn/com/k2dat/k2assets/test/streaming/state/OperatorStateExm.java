package cn.com.k2dat.k2assets.test.streaming.state;

import cn.com.k2dat.k2assets.models.UP;
import cn.com.k2dat.k2assets.schema.UpEventSchema;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class OperatorStateExm {


    static class MyRichSinkFunction extends RichSinkFunction<UP> implements CheckpointedFunction {
        private transient ListState<UP> unionListState;

        private transient ListState<UP> listState;

        private int subtaskIndex = 0;

        private List<UP> bufferedElements = new ArrayList<>();



        private final int threshold;


        public MyRichSinkFunction(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        }


        @Override
        public void invoke(UP value, Context context) throws Exception {
            this.bufferedElements.add(value);
            if (bufferedElements.size() == threshold) {
                for (UP element : bufferedElements) {
                    //将数据发到外部系统

                    System.out.println("==== 发送数据到外部系统 =>" + element.toString());

                }
                bufferedElements.clear();
            }

        }

        /**
         * 当请求 checkpoint 快照时，将调用此方法
         * 有请求执行 checkpoint 的时候，snapshotState() 方法就会被调用
         * 每次checkpoint时，将数据保存到状态中
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            unionListState.clear();
            for (UP bufferedElement : bufferedElements) {
                unionListState.add(bufferedElement);
            }

            listState.clear();
            for (UP bufferedElement : bufferedElements) {
                listState.add( bufferedElement);
            }

            System.out.println("snapshotState  subtask: " + subtaskIndex + " --  CheckPointId: " + context.getCheckpointId());

        }

        /**
         * 在分布式执行期间创建并行功能实例时，将调用此方法。 函数通常在此方法中设置其状态存储数据结构
         * initializeState() 方法会在每次初始化用户定义的函数时或者从更早的 checkpoint 恢复的时候被调用，
         * 因此 initializeState() 不仅是不同类型的状态被初始化的地方，而且还是 state 恢复逻辑的地方。
         * 从状态中，恢复数据
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

            // 通过 getUnionListState 获取 ListState
            unionListState = context.getOperatorStateStore().getUnionListState(
                    new ListStateDescriptor<>("unionListState",
                            TypeInformation.of(new TypeHint<UP>() {
                            })));

            // 通过 getListState 获取 ListState
            listState = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("listState",
                            TypeInformation.of(new TypeHint<UP>() {
                            })));

            System.out.println("subtask: " + subtaskIndex + "  start restore state");


            // 从状态中恢复数据
            if (context.isRestored()) {
                for (UP up : unionListState.get()) {
                    bufferedElements.add(up);
                    System.out.println("restore UnionListState  currentSubtask: " + subtaskIndex + " restoreSubtask ");
                }

                for (UP up : listState.get()) {
                    bufferedElements.add(up);
                    System.out.println("restore ListState  currentSubtask: " + subtaskIndex + " restoreSubtask ");
                }
            }

            System.out.println("subtask: " + subtaskIndex + "  complete restore");

        }


    }


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


        dataStreamSource.addSink(new MyRichSinkFunction(5));

        env.execute("OperatorStateExm");
    }
}
