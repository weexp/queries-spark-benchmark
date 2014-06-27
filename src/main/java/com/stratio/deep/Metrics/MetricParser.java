package com.stratio.deep.Metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hugo on 7/03/14.
 */
public class MetricParser {
    //private static final Logger log = LoggerFactory.getLogger(MetricParser.class);

    private MetricParser() {
    }

    private static final Pattern metricPattern = Pattern.compile("(.*) : (.*) - (.*)\\|(.*)");

    private static final Pattern metricValuePattern = Pattern
            .compile("(.*)=([a-zA-Z0-9\\.,:]*)?;?([a-zA-Z0-9\\.,:]*)?;?([a-zA-Z0-9\\.,:]*)?;?([a-zA-Z0-9\\.,:]*)?;?([a-zA-Z0-9\\.,:]*)?");

    public static Metric generateResult(String metricMessage, int metricStatus) {
        return generateResult(metricMessage, Status.getFromValue(metricStatus));
    }

    /**
     * 
     * @param metricMessage
     *            a nagios nrpe return message
     * @param metricStatus
     *            a nagios status (OK,WARNING, etc.)
     * @return a @Metric filled object
     */
    public static Metric generateResult(String metricMessage, Status metricStatus) {
        Metric metric = new Metric();
        metric.setStatus(metricStatus);

        if (metricMessage == null) {
            metricMessage = "";
        }

        Matcher metricMatcher = metricPattern.matcher(metricMessage);
        if (metricMatcher.matches()) {
            //log.debug("Is a correct metric");
            metric.setScriptName(metricMatcher.group(1));
            metric.setMessage(metricMatcher.group(3));

            String[] metricValuesString = metricMatcher.group(4).split(" ");
           // log.debug("There are {} values in this metric", metricValuesString.length);
            List<MetricValue> metricValues = new ArrayList<>();
            for (String metricValueString : metricValuesString) {
                Matcher metricValueMatcher = metricValuePattern.matcher(metricValueString);
                MetricValue metricValue;
                if (metricValueMatcher.matches()) {
                    metricValue = new MetricValue(metricValueMatcher.group(1), metricValueMatcher.group(2),
                            metricValueMatcher.group(3), metricValueMatcher.group(4), metricValueMatcher.group(5),
                            metricValueMatcher.group(6));

                    //log.debug("Adding a new metric value with key {} ", metricValue.getMetricKey());

                } else {
                    //log.warn("Metric value incorrect in metric {}. METRIC RAW -> {}", metric.getScriptName(),
                      //      metricValueString);
                    metricValue = new MetricValue();
                }
                metricValues.add(metricValue);
            }
            metric.setMetricValues(metricValues);
        } else {
            // Text metric
            metric.setMetricValues(new ArrayList<MetricValue>());
            metric.setMessage(metricMessage);
        }
        //log.debug("All metrics processed");
        return metric;
    }

}
