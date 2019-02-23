package ru.hh.hadoop.metrics2.sink;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.*;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class InfluxDBSink implements MetricsSink {
    private static final String INFLUDB_URL = "influxdb_url";
    private static final String INFLUDB_DATABSE = "influxdb_database";
    private static final String INFLUDB_USERNAME = "influxdb_username";
    private static final String INFLUDB_PASSWORD = "influxdb_password";
    private static final String MEASUREMENT_PREFIX = "measurement_prefix";
    private static final String RETENTION_POLICY = "defaultPolicy";

    private InfluxDB influxDB;
    private String influxdbUrl = null;
    private String influxdbDatabase = null;
    private String influxdbUsername = null;
    private String influxdbPassword = null;
    private String measurementPrefix = null;


    public void putMetrics(MetricsRecord record) {
        StringBuilder measurementName = new StringBuilder();

        BatchPoints batchPoints = BatchPoints
                .database(influxdbDatabase)
                .retentionPolicy(RETENTION_POLICY)
                .build();

        Map<String, String> tags = new HashMap<>();

        for (MetricsTag tag : record.tags()) {
            if (tag.value() != null) {
                tags.put(tag.name(), tag.value());
            }
        }

        measurementName.append(measurementPrefix).append("_").append(record.name());

        for (AbstractMetric metric : record.metrics()) {
            Point point = Point.measurement(measurementName.toString())
                    .time(record.timestamp(), TimeUnit.MILLISECONDS)
                    .addField(metric.name(), metric.value())
                    .tag(tags)
                    .build();

            batchPoints.point(point);
        }

        try {
            influxDB.write(batchPoints);
        } catch (Exception e) {
            throw new MetricsException("Error sending metrics", e);
        }
    }

    public void flush() {}

    public void init(SubsetConfiguration conf) {
        influxdbUrl = conf.getString(INFLUDB_URL);
        influxdbDatabase = conf.getString(INFLUDB_DATABSE);
        influxdbUsername = conf.getString(INFLUDB_USERNAME);
        influxdbPassword = conf.getString(INFLUDB_PASSWORD);

        measurementPrefix = conf.getString(MEASUREMENT_PREFIX);
        if (measurementPrefix == null) {
            measurementPrefix = "";
        }

        try {
            influxDB = InfluxDBFactory.connect(
                    influxdbUrl,
                    influxdbUsername,
                    influxdbPassword
            );

            influxDB.createDatabase(influxdbDatabase);
            influxDB.createRetentionPolicy(
                    RETENTION_POLICY, influxdbDatabase, "30d", 1, true);
        } catch (Exception e) {
            throw new MetricsException("Error creating connection, " + influxdbUrl, e);
        }
    }
}
