package com.goodgame.profiling.stream_rewriter.persistent_drains.cassandra;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections4.map.LazyMap;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.EvilManagerHack;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.goodgame.profiling.commons.logging.LogService;
import com.goodgame.profiling.commons.model.Metric;
import com.goodgame.profiling.commons.statistics.WriteToStorageEvent;
import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;
import com.goodgame.profiling.commons.statistics.storage.MetricStorage;
import com.goodgame.profiling.graphite_retentions.RetentionTable;
import com.goodgame.profiling.graphite_retentions.bootloader.EnvironmentWithRetentionStrategy;
import com.goodgame.profiling.stream_rewriter.persistent_drains.PersistentDrain;

public class PersistentCassandraDrain<E extends EnvironmentWithRetentionStrategy> implements PersistentDrain {
    private static final Logger logger = LogService.getLogger(PersistentCassandraDrain.class);

    private static AtomicLong metricCount = new AtomicLong();
    private static AtomicBoolean eventRegistered = new AtomicBoolean();

    private static final String COL_NAME = "metric";
    private static final String COL_TIME = "timestamp";
    private static final String COL_VALUE = "value";
    private static final String[] COLUMNS = { COL_NAME, COL_TIME, COL_VALUE };

    private final E environment;
    private final String keyspace;
    private final Cluster cluster;
    private final Session session;

    public PersistentCassandraDrain( E environment, String username, String password, String[] seeds, String keyspace ) {
        Cluster.Builder builder = Cluster.builder().addContactPoints( seeds );
        builder.withReconnectionPolicy( new ExponentialReconnectionPolicy( 500, 60_000 ) );
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
        if ( !eventRegistered.getAndSet( true ) ) {
            EventBusManager.createRegistrationPoint().subscribe( WriteToStorageEvent.class, w -> {
                MetricStorage storage = w.storageToWriteTo().getSubStorageCalled( "cassandra" );
                storage.store( "metrics-to-cassandra", metricCount.longValue() );
            });

            EvilManagerHack.subscribe( this.cluster );
        }

        if( metrics.size() == 0 ) {
            return;
        }

        Map<RetentionTable, BatchStatement> stms = LazyMap.<RetentionTable, BatchStatement>lazyMap( new HashMap<>(), () -> new BatchStatement() );
        for ( Metric metric : metrics ) {
            insertMetricIntoBatch( metric, stms );
        }
        KeyspaceMetadata metadata = cluster.getMetadata().getKeyspace( keyspace );
        for (RetentionTable table : stms.keySet()) {
            createTableIfNecessary( table, metadata );
        }
        for ( BatchStatement batch : stms.values() ) {
            session.execute( batch );
        }

        metricCount.addAndGet( metrics.size() );
    }

    private void insertMetricIntoBatch( Metric metric, Map<RetentionTable, BatchStatement> map ) {
        Object[] values = { metric.name(), metric.timestamp(), metric.value() };
        Optional<RetentionTable> table = environment.retentions().findAccessTableForMetric( metric );
        if ( !table.isPresent() ) {
            logger.warn( "No retention defined - not outputting metric! {}", metric );
        } else {
            map.get( table.get() ).add( QueryBuilder.insertInto( table.get().tableName() ).values( COLUMNS, values ) );
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
