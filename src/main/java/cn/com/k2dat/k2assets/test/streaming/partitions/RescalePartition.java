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

public class RescalePartition {

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

        DataStream<UP> dataStream = dataStreamSource.map(new MapFunction<UP, UP>() {
            @Override
            public UP map(UP value) throws Exception {
                System.out.println("--当前线程=>" + Thread.currentThread().getId() + "; 输出内容 =>" + value.userId);
                return value;
            }
        }).setParallelism(2).keyBy("userId").map(new MapFunction<UP, UP>() {
            @Override
            public UP map(UP value) throws Exception {
                System.out.println("--keyBy: 当前线程=>" + Thread.currentThread().getId() + "; 输出内容 =>" + value.userId);
                return value;
            }
        });


        /**
         * 重新缩放分区
         * 1. 如果上游有2个并发， 下游有4个并发：
         *      那么，上游中的一个并发的数据会平均分配给下游中2个并发中，上游中的另外一个并发数据会平均分配给下游中的另外2个并发中。
         *
         * 2. 如果上游4个并发，下游2个并发：
         *      那么，上游中的两个并发的数据会平均分配给下游中的1个并发中，上游中的另外2个并发数据会平均分配给下游的另外1个并发中
         *
         * ------- 第一种情况 ------
         * --当前线程=>67; 输出内容 =>UC200
         * --当前线程=>67; 输出内容 =>UC200
         * --当前线程=>67; 输出内容 =>UC200
         * --当前线程=>67; 输出内容 =>UC200
         *
         * --当前线程=>66; 输出内容 =>UC100
         * --当前线程=>66; 输出内容 =>UC100
         * --当前线程=>66; 输出内容 =>UC100
         * --当前线程=>66; 输出内容 =>UC100
         *
         * --rescale: 当前线程=>71; 输出内容 =>UC200
         * --rescale: 当前线程=>71; 输出内容 =>UC200
         * --rescale: 当前线程=>70; 输出内容 =>UC200
         * --rescale: 当前线程=>70; 输出内容 =>UC200
         *
         * --rescale: 当前线程=>69; 输出内容 =>UC100
         * --rescale: 当前线程=>69; 输出内容 =>UC100
         * --rescale: 当前线程=>68; 输出内容 =>UC100
         * --rescale: 当前线程=>68; 输出内容 =>UC100
         *
         *
         *
         *  ------ 第二种情况 ------
         * --当前线程=>81; 输出内容 =>UC200
         * --当前线程=>81; 输出内容 =>UC400
         * --当前线程=>90; 输出内容 =>UC400
         * --当前线程=>90; 输出内容 =>UC200
         *
         * --当前线程=>93; 输出内容 =>UC300
         * --当前线程=>93; 输出内容 =>UC100
         * --当前线程=>94; 输出内容 =>UC100
         * --当前线程=>94; 输出内容 =>UC300
         *
         *
         *
         * --rescale: 当前线程=>97; 输出内容 =>UC200
         * --rescale: 当前线程=>97; 输出内容 =>UC400
         * --rescale: 当前线程=>97; 输出内容 =>UC200
         * --rescale: 当前线程=>97; 输出内容 =>UC400
         *
         * --rescale: 当前线程=>98; 输出内容 =>UC100
         * --rescale: 当前线程=>98; 输出内容 =>UC300
         * --rescale: 当前线程=>98; 输出内容 =>UC100
         * --rescale: 当前线程=>98; 输出内容 =>UC300
         *
         *
         */
        dataStream.rescale().map(new MapFunction<UP, UP>() {
            @Override
            public UP map(UP value) throws Exception {
                System.out.println("--rescale: 当前线程=>" + Thread.currentThread().getId() + "; 输出内容 =>" + value.userId);
                return value;
            }
        }).setParallelism(4);


        // execute program
        env.execute("Flink Streaming Java API Skeleton");


    }
}
