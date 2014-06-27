package com.stratio.deep.Metrics;

public class MetricValue {

	private String metricKey;
	private String value;
	private String warningRange;
	private String criticalRange;
	private String minValue;
	private String maxValue;

	public MetricValue() {
	}

	public MetricValue(String metricKey, String value, String warningRange,
			String criticalRange, String minValue, String maxValue) {
		super();
		this.metricKey = metricKey;
		this.value = value;
		this.warningRange = warningRange;
		this.criticalRange = criticalRange;
		this.minValue = minValue;
		this.maxValue = maxValue;
	}

	public String getMetricKey() {
		return metricKey;
	}

	public void setMetricKey(String metricKey) {
		this.metricKey = metricKey;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getWarningRange() {
		return warningRange;
	}

	public void setWarningRange(String warningRange) {
		this.warningRange = warningRange;
	}

	public String getCriticalRange() {
		return criticalRange;
	}

	public void setCriticalRange(String criticalRange) {
		this.criticalRange = criticalRange;
	}

	public String getMinValue() {
		return minValue;
	}

	public void setMinValue(String minValue) {
		this.minValue = minValue;
	}

	public String getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(String maxValue) {
		this.maxValue = maxValue;
	}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricValue that = (MetricValue) o;

        if (criticalRange != null ? !criticalRange.equals(that.criticalRange) : that.criticalRange != null)
            return false;
        if (maxValue != null ? !maxValue.equals(that.maxValue) : that.maxValue != null) return false;
        if (metricKey != null ? !metricKey.equals(that.metricKey) : that.metricKey != null) return false;
        if (minValue != null ? !minValue.equals(that.minValue) : that.minValue != null) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;
        if (warningRange != null ? !warningRange.equals(that.warningRange) : that.warningRange != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = metricKey != null ? metricKey.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (warningRange != null ? warningRange.hashCode() : 0);
        result = 31 * result + (criticalRange != null ? criticalRange.hashCode() : 0);
        result = 31 * result + (minValue != null ? minValue.hashCode() : 0);
        result = 31 * result + (maxValue != null ? maxValue.hashCode() : 0);
        return result;
    }

    @Override public String toString() {
        return "MetricValue{" +
                "metricKey='" + metricKey + '\'' +
                ", value='" + value + '\'' +
                ", warningRange='" + warningRange + '\'' +
                ", criticalRange='" + criticalRange + '\'' +
                ", minValue='" + minValue + '\'' +
                ", maxValue='" + maxValue + '\'' +
                '}';
    }
}
