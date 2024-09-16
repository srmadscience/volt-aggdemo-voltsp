package org.voltdb.aggdemo.connection;

import org.voltdb.aggdemo.DataGeneratorConfig;
import org.voltdb.aggdemo.MediationMessage;

import java.rmi.dgc.DGC;
import java.util.function.Consumer;

public abstract class ConnectionFactory {

    public abstract Connection create(String commaDelimitedHostnames);

    public static ConnectionFactory create(DataGeneratorConfig config) {
        if (config.isUseKafka()) {
            return new KafkaConnectionFactory();
        }

        return new VoltDBConnectionFactory();
    }
}
