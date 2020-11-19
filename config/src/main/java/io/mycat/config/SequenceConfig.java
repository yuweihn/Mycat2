package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

@Data
@EqualsAndHashCode
public class SequenceConfig {
    private String name;

    private String startWith;
    private String incrementBy;
    private String minValue;
    private String maxValue;
    private boolean noMaxValue;
    private boolean noMinValue;
    private Boolean withCache;
    private Boolean cycle;
    private Boolean cache;
    private Long cacheValue;
    private Boolean order;

    private boolean simple;
    private boolean group;
    private boolean time;

    private Long unitCount;
    private Long unitIndex;

    private Long step;

    private String clazz;

    private String targetName;

    public SequenceConfig() {
    }
}