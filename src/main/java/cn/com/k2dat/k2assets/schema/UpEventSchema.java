package cn.com.k2dat.k2assets.schema;

import cn.com.k2dat.k2assets.models.UP;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * 自定义序列化类
 */
public class UpEventSchema implements DeserializationSchema<UP> {

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
