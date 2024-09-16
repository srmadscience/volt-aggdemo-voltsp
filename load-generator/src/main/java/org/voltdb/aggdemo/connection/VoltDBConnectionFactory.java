package org.voltdb.aggdemo.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;

public class VoltDBConnectionFactory extends ConnectionFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(VoltDBConnectionFactory.class);

    @Override
    public Connection create(String commaDelimitedHostnames) {
        Client client;
        ClientConfig config;

        try {
            LOGGER.info("Logging into VoltDB");

            config = new ClientConfig();
            config.setTopologyChangeAware(true);
            config.setReconnectOnConnectionLoss(true);

            client = ClientFactory.createClient(config);

            String[] hostnameArray = commaDelimitedHostnames.split(",");

            for (String s : hostnameArray) {
                LOGGER.info("Connect to " + s + "...");
                try {
                    client.createConnection(s);
                } catch (Exception e) {
                    LOGGER.error(e.getMessage());
                }
            }

            LOGGER.info("Connected to VoltDB");
        } catch (Exception e) {
            throw new RuntimeException("VoltDB connection failed", e);
        }

        return nextCdr -> {
            try {
                ComplainOnErrorCallback errorCallback = new ComplainOnErrorCallback();

                client.callProcedure(errorCallback, "HandleMediationCDR", nextCdr.getSessionId(),
                        nextCdr.getSessionStartUTC(), nextCdr.getSeqno(), nextCdr.getCallingNumber(),
                        nextCdr.getDestination(), nextCdr.getEventType(), nextCdr.getRecordStartUTC(),
                        nextCdr.getRecordUsage());
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
            }
        };
    }
}
