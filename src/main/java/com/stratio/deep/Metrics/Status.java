package com.stratio.deep.Metrics;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ajnavarro on 21/02/14.
 */
public enum Status {
    UNKNOWN(-1), OK(0), WARNING(1), CRITICAL(2);

    private int value;

    Status(final int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    private static final Map<Integer, Status> intToTypeMap = new HashMap<>();
    static {
        for (Status status : Status.values()) {
            intToTypeMap.put(status.value, status);
        }
    }

    public static Status getFromValue(int value) {
        return intToTypeMap.get(Integer.valueOf(value));
    }
}
