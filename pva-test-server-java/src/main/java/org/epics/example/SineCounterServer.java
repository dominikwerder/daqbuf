package org.epics.example;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.epics.pvaccess.client.ChannelProvider;
import org.epics.pvaccess.server.impl.remote.ServerContextImpl;
import org.epics.pvdatabase.PVDatabase;
import org.epics.pvdatabase.PVDatabaseFactory;
import org.epics.pvdatabase.pva.ChannelProviderLocalFactory;

public class SineCounterServer {

    private static final long UPDATE_PERIOD_MS = 100;

    public static void main(String[] args) throws Exception {
        String recordName = args.length > 0 ? args[0] : "EXAMPLE:SINECOUNTER";
        double periodSeconds = args.length > 1 ? Double.parseDouble(args[1]) : 10.0;

        PVDatabase master = PVDatabaseFactory.getMaster();
        ChannelProvider channelProvider = ChannelProviderLocalFactory.getChannelProviderLocal();

        SineCounterRecord record = SineCounterRecord.create(recordName, periodSeconds);
        if (!master.addRecord(record)) {
            throw new IllegalStateException(recordName + " not added");
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "sine-counter-update");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleAtFixedRate(record::update, 0, UPDATE_PERIOD_MS, TimeUnit.MILLISECONDS);

        ServerContextImpl context = ServerContextImpl.startPVAServer(channelProvider.getProviderName(), 0, true, null);
        System.out.println("serving " + recordName + " with sine period " + periodSeconds
                + " s, update period " + UPDATE_PERIOD_MS + " ms");

        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduler.shutdownNow();
            try {
                context.destroy();
            } catch (Exception e) {
                e.printStackTrace();
            }
            master.destroy();
            channelProvider.destroy();
            shutdown.countDown();
        }));

        shutdown.await();
    }
}
