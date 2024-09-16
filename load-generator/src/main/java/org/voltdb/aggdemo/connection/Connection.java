package org.voltdb.aggdemo.connection;

import org.voltdb.aggdemo.MediationDataGenerator;
import org.voltdb.aggdemo.MediationMessage;

@FunctionalInterface
public interface Connection {

    void sendData(MediationMessage mediationMessage);
}
