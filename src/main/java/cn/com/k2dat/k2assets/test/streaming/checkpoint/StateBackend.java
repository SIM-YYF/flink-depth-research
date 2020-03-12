package cn.com.k2dat.k2assets.test.streaming.checkpoint;

import cn.com.k2dat.k2assets.models.UP;
import cn.com.k2dat.k2assets.schema.UpEventSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class StateBackend {
    public static void main(String[] args) throws Exception {

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1); // 设置并行度

            // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
            env.enableCheckpointing(1000);
            // 高级选项：
            // 设置模式为exactly-once （这是默认值）
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
            // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
            env.getCheckpointConfig().setCheckpointTimeout(60000);
            // 同一时间只允许进行一个检查点
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
            //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
            //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


            //env.setStateBackend(new MemoryStateBackend());
            //env.setStateBackend(new FsStateBackend("hdfs://ywfdembp:9000/flink/checkpoints"));

        /**
         *
         *
         * 将状态数据保存 RocksDB 中，同时同步数据到远程的hdfs文件中
         * 调试运行，默认开启两个checkpoint检查点，来保存状态数据
         * 在flink-conf.yaml文件中，配置项: state.checkpoints.num-retained: 10 开启多个checkpoint检查点
         * 通过命令： hdfs dfs -ls 来验证是否开启了多个checkpoint 检查点
         *
         * $ hdfs dfs -ls -R /flink/checkpoints/89dc9d416922dfec31e41129087f21b1
         * 2020-03-11 17:28:48,819 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
         * drwxr-xr-x   - ywf supergroup          0 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-221
         * -rw-r--r--   3 ywf supergroup        274 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-221/_metadata
         * -rw-r--r--   3 ywf supergroup       1241 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-221/d2c80683-9f4c-4abe-9264-de737f7327e9
         * drwxr-xr-x   - ywf supergroup          0 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-222
         * -rw-r--r--   3 ywf supergroup       1241 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-222/1cdf8343-a020-46db-bd86-8388ecf8f4cf
         * -rw-r--r--   3 ywf supergroup        274 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-222/_metadata
         * drwxr-xr-x   - ywf supergroup          0 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-223
         * -rw-r--r--   3 ywf supergroup        274 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-223/_metadata
         * -rw-r--r--   3 ywf supergroup       1241 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-223/f0230b78-759d-4377-8344-bd4c67673047
         * drwxr-xr-x   - ywf supergroup          0 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-224
         * -rw-r--r--   3 ywf supergroup        274 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-224/_metadata
         * -rw-r--r--   3 ywf supergroup       1241 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-224/ebe6b8a9-7c54-4266-8374-7f989574e61e
         * drwxr-xr-x   - ywf supergroup          0 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-225
         * -rw-r--r--   3 ywf supergroup       1241 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-225/729107d4-e4bb-4aab-b69b-8bf2ee976595
         * -rw-r--r--   3 ywf supergroup        274 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-225/_metadata
         * drwxr-xr-x   - ywf supergroup          0 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-226
         * -rw-r--r--   3 ywf supergroup        274 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-226/_metadata
         * -rw-r--r--   3 ywf supergroup       1241 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-226/b44f51d7-6b29-4f8f-b888-4328fb9444ad
         * drwxr-xr-x   - ywf supergroup          0 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-227
         * -rw-r--r--   3 ywf supergroup        274 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-227/_metadata
         * -rw-r--r--   3 ywf supergroup       1241 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-227/c4975796-84b8-479f-b0e5-7d489d3b8b88
         * drwxr-xr-x   - ywf supergroup          0 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-228
         * -rw-r--r--   3 ywf supergroup       1241 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-228/0e263980-efab-4a3b-ae14-aa41266c4cd1
         * -rw-r--r--   3 ywf supergroup        274 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-228/_metadata
         * drwxr-xr-x   - ywf supergroup          0 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-229
         * -rw-r--r--   3 ywf supergroup       1241 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-229/0f3aa573-5790-49ac-8b74-bc3de1f3485c
         * -rw-r--r--   3 ywf supergroup        274 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-229/_metadata
         * drwxr-xr-x   - ywf supergroup          0 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-230
         * -rw-r--r--   3 ywf supergroup       1241 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-230/21e8a551-00c7-4d3b-aaac-c1affda3ef1e
         * -rw-r--r--   3 ywf supergroup        274 2020-03-11 17:28 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/chk-230/_metadata
         * drwxr-xr-x   - ywf supergroup          0 2020-03-11 17:24 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/shared
         * drwxr-xr-x   - ywf supergroup          0 2020-03-11 17:24 /flink/checkpoints/89dc9d416922dfec31e41129087f21b1/taskowned
         */
        env.setStateBackend(new RocksDBStateBackend("hdfs://ywfdembp:9000/flink/checkpoints",true));


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

            dataStreamSource.keyBy("userId").map(new MapFunction<UP, UP>() {
                @Override
                public UP map(UP value) throws Exception {
                    return value;
                }
            }).setParallelism(2).print();

            env.execute("StateBackend");

    }
}
