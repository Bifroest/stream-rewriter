package io.bifroest.stream_rewriter.persistent_drains.gatling_cassandra;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections4.map.LazyMap;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.EvilManagerHack;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import io.bifroest.commons.logging.LogService;
import io.bifroest.commons.model.Metric;
import io.bifroest.commons.statistics.WriteToStorageEvent;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.commons.statistics.storage.MetricStorage;
import io.bifroest.retentions.RetentionTable;
import io.bifroest.retentions.bootloader.EnvironmentWithRetentionStrategy;
import io.bifroest.stream_rewriter.persistent_drains.PersistentDrain;

public final class PersistentGatlingCassandraDrain<E extends EnvironmentWithRetentionStrategy> implements PersistentDrain {
    private static final Logger logger = LogService.getLogger(PersistentGatlingCassandraDrain.class);

    private static final AtomicLong metricCount = new AtomicLong();
    private static final AtomicBoolean eventRegistered = new AtomicBoolean();

    private static final String COL_NAME = "metric";
    private static final String COL_TIME = "timestamp";
    private static final String COL_VALUE = "value";

    private final E environment;
    private final String keyspace;
    private final Cluster cluster;
    private final Session session;

    public PersistentGatlingCassandraDrain(
            E environment,
            String username, String password,
            String[] seeds, String keyspace,
            int preparedStatementsToKeepAround,
            Duration initialBackoff, Duration maximumBackoff ) {
        Cluster.Builder builder = Cluster.builder().addContactPoints( seeds );
        builder.withReconnectionPolicy( new ExponentialReconnectionPolicy( initialBackoff.toMillis(), maximumBackoff.toMillis() ) );
        if ( username != null ) {
            if ( password != null ) {
                builder = builder.withCredentials( username, password );
            } else {
                logger.warn( "username was set, password was NOT set - IGNORING username!" );
            }
        }

        this.environment = environment;
        this.cluster = builder.build();
        this.keyspace = keyspace;
        this.session = cluster.connect( keyspace );
    }

    @Override
    public void shutdown() {
        session.close();
        cluster.close();
    }

    @Override
    public void output( Collection<Metric> metrics ) {
        initializeStatisticsLazily();

        Map<RetentionTable, List<Metric>> metricsByTable = splitMetricsByTable(metrics);
        createNecessaryTables( metricsByTable );
        doManyPreparedStatements( metricsByTable );

        metricCount.addAndGet( metrics.size() );
    }

    private void initializeStatisticsLazily() {
        // bootloader woes means we need to create this after the statistic system
        // has been booted, and the easiest way to do this is to initialize this
        // stuff once metrics are incoming
        
        if ( !eventRegistered.getAndSet( true ) ) {
            EventBusManager.createRegistrationPoint().subscribe( WriteToStorageEvent.class, w -> {
                MetricStorage storage = w.storageToWriteTo().getSubStorageCalled( "cassandra" );
                storage.store( "metrics-to-cassandra", metricCount.longValue() );
            });

            EvilManagerHack.subscribe( this.cluster );
        }
    }

    private Map<RetentionTable, List<Metric>> splitMetricsByTable( Collection<Metric> metrics ) {
        Map<RetentionTable, List<Metric>> metricsByTable = LazyMap.lazyMap( new HashMap<>(),
                () -> new ArrayList<>( metrics.size() / 4 ) );
        for ( Metric metric : metrics ) {
            Optional<RetentionTable> table = environment.retentions().findAccessTableForMetric( metric );
            if ( !table.isPresent() ) {
                logger.warn( "No retention defined - not outputting metric! {}", metric );
            } else {
                metricsByTable.get( table.get() ).add( metric );
            }
        }
        return metricsByTable;
    }

    private void createNecessaryTables( Map<RetentionTable, List<Metric>> metricsByTable ) {
        KeyspaceMetadata metadata = cluster.getMetadata().getKeyspace( keyspace );
        for (RetentionTable table : metricsByTable.keySet()) {
            createTableIfNecessary( table, metadata );
        }
    }

    private void doManyPreparedStatements( Map<RetentionTable, List<Metric>> metricsByTable ) {
        for ( Map.Entry<RetentionTable, List<Metric>> tableAndMetrics : metricsByTable.entrySet() ) {
            RetentionTable table = tableAndMetrics.getKey();
            List<Metric> metrics = tableAndMetrics.getValue();

            for ( Metric metric : metrics ) {
                session.execute(
                        String.format( "INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?);", table.tableName(), COL_NAME, COL_VALUE, COL_TIME ),
                        metric.name(),
                        metric.value(),
                        metric.timestamp()
                );
            }
        }
    }

    private void createTableIfNecessary( RetentionTable table, KeyspaceMetadata metadata ) {
        for ( TableMetadata meta : metadata.getTables()) {
            logger.debug( "Comparing " + meta.getName() + " with " + table.tableName() );
            if ( meta.getName().equalsIgnoreCase( table.tableName() )) {
                return;
            }
        }
         
        StringBuilder query = new StringBuilder();
        query.append( "CREATE TABLE " ).append( table.tableName() ).append( " (" );
        query.append( COL_NAME ).append( " text, " );
        query.append( COL_TIME ).append( " bigint, " );
        query.append( COL_VALUE ).append( " double, " );
        query.append( "PRIMARY KEY (" ).append( COL_NAME ).append( ", " ).append( COL_TIME ).append( ")");
        query.append( ");" );
        logger.debug( "Creating table with query: <" + query.toString() + ">");
        try {
            session.execute( query.toString() );
        } catch( AlreadyExistsException e ) {
            // Some other gatherer might have already created the same table.
        }
    }

    @Override
    public void dumpInfos() {
        logger.info( ReflectionToStringBuilder.toString( cluster.getMetadata() ) );
    }
}
