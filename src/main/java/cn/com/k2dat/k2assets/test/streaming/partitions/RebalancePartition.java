/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.com.k2dat.k2assets.test.streaming.partitions;

import cn.com.k2dat.k2assets.models.UP;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.IOException;
import java.util.Properties;

public class RebalancePartition {

    /**
     * 自定义序列化类 --> 从kafka中的json数据转换为pojo对象
     */
    public static class UpEventSchema implements DeserializationSchema<UP> {

        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public UP deserialize(byte[] message) throws IOException {
            return mapper.readValue(new String(message), UP.class);
        }

        @Override
        public boolean isEndOfStream(UP nextElement) {
            return false;
        }

        @Override
        public TypeInformation<UP> getProducedType() {
            return TypeInformation.of(UP.class);
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

        // 设置并行度为2
        DataStream<UP> dataStream = dataStreamSource.map(new MapFunction<UP, UP>() {
            @Override
            public UP map(UP value) throws Exception {
                System.out.println("--当前线程=>" + Thread.currentThread().getId() + "; 输出内容 =>" + value.userId);
                return value;
            }
        }).setParallelism(1);

        /**
         * 对数据进行再平衡，重分区，消除数据倾斜
         */
        DataStream<UP> dataStreamRrebalance = dataStream.rebalance().map(new MapFunction<UP, UP>() {
            @Override
            public UP map(UP value) throws Exception {
                System.out.println("--rebalance: 当前线程=>" + Thread.currentThread().getId() + "; 输出内容 =>" + value.userId);
                return value;
            }
        }).setParallelism(4);


        // execute program
        env.execute("Flink Streaming Java API Skeleton");


    }
}
