package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.concurrent.TimeUnit;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class TimerConfig {
    private long initialDelay = 1 * 1000L;
    private long period = 10 * 1000L;
    private String timeUnit = TimeUnit.MILLISECONDS.name();
}

