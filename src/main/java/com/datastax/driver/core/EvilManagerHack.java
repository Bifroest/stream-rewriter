package com.datastax.driver.core;

import io.bifroest.commons.statistics.WriteToStorageEvent;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.commons.statistics.storage.MetricStorage;

public class EvilManagerHack {
    public static void subscribe ( Cluster c ) {
        EventBusManager.createRegistrationPoint().subscribe( WriteToStorageEvent.class, e -> {
            MetricStorage storage = e.storageToWriteTo().getSubStorageCalled( "cassandra-driver" );

            storage.store( "known-hosts", c.manager.metadata.allHosts().size() );
        });

        EventBusManager.createRegistrationPoint().subscribe( WriteToStorageEvent.class, e -> {
            MetricStorage storage = e.storageToWriteTo().getSubStorageCalled( "cassandra-driver" );

            storage.store( "connected-to", c.manager
                                            .sessions
                                            .stream()
                                            .flatMap( session -> session.pools.keySet().stream() )
                                            .distinct()
                                            .count() );
        });

        EventBusManager.createRegistrationPoint().subscribe( WriteToStorageEvent.class, e -> {
            MetricStorage storage = e.storageToWriteTo().getSubStorageCalled( "cassandra-driver" );

            storage.store( "open-connections", c.manager
                                                .sessions
                                                .stream()
                                                .flatMap( session -> session.pools.values().stream() )
                                                .mapToInt( pool -> pool.opened() )
                                                .sum() );
        });

        EventBusManager.createRegistrationPoint().subscribe( WriteToStorageEvent.class, e -> {
            MetricStorage storage = e.storageToWriteTo().getSubStorageCalled( "cassandra-driver" );

            storage.store( "trashed-connections", c.manager
                                                   .sessions
                                                   .stream()
                                                   .flatMap( session -> session.pools.values().stream() )
                                                   .mapToInt( pool -> pool.trashed() )
                                                   .sum() );
        });

        EventBusManager.createRegistrationPoint().subscribe( WriteToStorageEvent.class, e -> {
            MetricStorage storage = e.storageToWriteTo().getSubStorageCalled( "cassandra-driver" );

            storage.store( "executor-queue-depth", c.manager.executorQueue.size() );
        });

        EventBusManager.createRegistrationPoint().subscribe( WriteToStorageEvent.class, e -> {
            MetricStorage storage = e.storageToWriteTo().getSubStorageCalled( "cassandra-driver" );

            storage.store( "blocking-executor-queue-depth", c.manager.blockingExecutorQueue.size() );
        });

        EventBusManager.createRegistrationPoint().subscribe( WriteToStorageEvent.class, e -> {
            MetricStorage storage = e.storageToWriteTo().getSubStorageCalled( "cassandra-driver" );

            storage.store( "reconnection-scheduler-task-count", c.manager.reconnectionExecutor.getQueue().size() );
        });

        EventBusManager.createRegistrationPoint().subscribe( WriteToStorageEvent.class, e -> {
            MetricStorage storage = e.storageToWriteTo().getSubStorageCalled( "cassandra-driver" );

            storage.store( "task-scheduler-task-count", c.manager.scheduledTasksExecutor.getQueue().size() );
        });
    }
}
