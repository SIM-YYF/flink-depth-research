package cn.com.k2dat.k2assets.schema;

import cn.com.k2dat.k2assets.models.MetricEvent;
import cn.com.k2dat.k2assets.models.UP;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * 自定义序列化类
 */
public class MetricsEventSchema implements DeserializationSchema<MetricEvent>, SerializationSchema<MetricEvent> {

    private static final ObjectMapper mapper = new ObjectMapper();


    /**
     * 反序列
     *
     * @param message
     * @return
     * @throws IOException
     */
    @Override
    public MetricEvent deserialize(byte[] message) throws IOException {
        return mapper.readValue(new String(message), MetricEvent.class);
    }

    /**
     * 序列化
     *
     * @param metricEvent
     * @return
     */
    @Override
    public byte[] serialize(MetricEvent metricEvent) {
        try {
            return mapper.writeValueAsBytes(metricEvent);
        } catch (JsonProcessingException e) {
            e.printStackTrace();

        }
        return null;
    }

    @Override
    public boolean isEndOfStream(MetricEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<MetricEvent> getProducedType() {
        return TypeInformation.of(MetricEvent.class);
    }
}
