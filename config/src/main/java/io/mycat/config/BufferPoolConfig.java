package io.mycat.config;

import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

@Data
public class BufferPoolConfig {
    Map<String, Object> args;

    private static final Map<String, Object> defaultConfig = defaultValue();


    @NotNull
    public static Map defaultValue() {
        HashMap defaultConfig = new HashMap<>();
        long pageSize = 1024 * 1024 * 2;
        defaultConfig.put("pageSize", pageSize);
        defaultConfig.put("chunkSize", 8192);
        defaultConfig.put("pageCount", 1);
        return defaultConfig;
    }
}
