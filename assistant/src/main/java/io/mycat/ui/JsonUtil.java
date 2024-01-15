package io.mycat.ui;

import io.vertx.core.json.Json;

public class JsonUtil {
    public static <T> T clone(T o) {
        String encode = Json.encode(o);
        return (T) Json.decodeValue(encode, o.getClass());
    }
}
