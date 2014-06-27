package com.stratio.deep.Metrics;

import java.util.List;

public class Metric {

    private String scriptName;
    private String message;

    private Status status;

    private List<MetricValue> metricValues;

    public Metric() {
    }

    public Metric(String scriptName, String message, Status status,
            List<MetricValue> metricValues) {
        super();
        this.scriptName = scriptName;
        this.message = message;
        this.status = status;
        this.metricValues = metricValues;
    }

    public String getScriptName() {
        return scriptName;
    }

    public void setScriptName(String scriptName) {
        this.scriptName = scriptName;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public List<MetricValue> getMetricValues() {
        return metricValues;
    }

    public void setMetricValues(List<MetricValue> metricValues) {
        this.metricValues = metricValues;
    }

    @Override public String toString() {
        return "Metric{" +
                "scriptName='" + scriptName + '\'' +
                ", message='" + message + '\'' +
                ", status=" + status +
                ", metricValues=" + metricValues +
                '}';
    }
}
