package com.goodgame.profiling.stream_rewriter.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import com.goodgame.profiling.commons.boot.interfaces.Subsystem;
import com.goodgame.profiling.commons.model.Metric;
import com.goodgame.profiling.commons.statistics.WriteToStorageEvent;
import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;
import com.goodgame.profiling.commons.statistics.storage.MetricStorage;
import com.goodgame.profiling.commons.statistics.units.parse.DurationParser;
import com.goodgame.profiling.commons.systems.SystemIdentifiers;
import com.goodgame.profiling.dns.EndPoint;
import com.goodgame.profiling.drains.Drain;
import com.goodgame.profiling.drains.serial.failfirst.SerialFailFirstDrain;
import com.goodgame.profiling.stream_rewriter.StreamRewriterIdentifiers;
import com.goodgame.profiling.stream_rewriter.persistent_drains.EnvironmentWithPersistentDrainManager;
import com.goodgame.profiling.stream_rewriter.persistent_drains.PersistentDrainFrontend;
import com.goodgame.profiling.stream_rewriter.watermark.EnvironmentWithMutableCompositeWaterMark;

/**
 *
 * @author hkraemer@ggs-hh.net
 */
@MetaInfServices
public class DBnatorSystem<E extends EnvironmentWithMutableDBInput & EnvironmentWithPersistentDrainManager & EnvironmentWithMutableCompositeWaterMark> implements Subsystem<E> {
    private static final Logger log = LogManager.getLogger();
    
    private int queueSize;
    private int chunkSize;
    private int dbWorkerCount;
    private long sleepAfterFailMilliseconds;
    private String persistentDrainName;
    private String persistentBifroestDrainName;
    private List<EndPoint> carbonEndpoints;
    
    private double highWatermark;
    private double lowWatermark;
    
    private Drain drain;
    
    private ExecutorService executor;
    
    @Override
    public String getSystemIdentifier() {
        return StreamRewriterIdentifiers.DBNATOR;
    }

    @Override
    public Collection<String> getRequiredSystems() {
        return Arrays.asList( StreamRewriterIdentifiers.PERSISTENT_DRAINS, SystemIdentifiers.STATISTICS );
    }

    @Override
    public void configure( JSONObject appConfig ) {
        JSONObject systemConfig = appConfig.getJSONObject( "dbnator" );
        
        dbWorkerCount = systemConfig.getInt( "worker-count" );
        
        sleepAfterFailMilliseconds = new DurationParser().parse( systemConfig.optString( "sleep-after-fail", "1s" )).toMillis();
        queueSize = systemConfig.optInt( "queue-size", 10000 );
        chunkSize = systemConfig.optInt( "chunk-size", 5000 );
        persistentDrainName = systemConfig.optString( "persistent-drain", "cassandra" );
        persistentBifroestDrainName = systemConfig.optString( "persistent-bifroest-drain", "bifroest" );
        
        carbonEndpoints = new ArrayList<>();
        JSONArray carbonArray = systemConfig.getJSONArray( "carbon-hosts" );
        for ( int i = 0 ; i < carbonArray.length(); i++ ) {
            JSONObject carbonJSON = carbonArray.getJSONObject( i );
            carbonEndpoints.add( EndPoint.of( carbonJSON.getString( "host" ), carbonJSON.getInt( "port" ) ) );
        }
        
        JSONObject watermark = appConfig.optJSONObject( "queue-watermark" );
        if ( watermark != null ) {
            highWatermark = watermark.optDouble( "high", 0.75 );
            lowWatermark = watermark.optDouble( "low", 0.25 );
        } else {
            highWatermark = 0.75;
            lowWatermark = 0.25;
        }
        log.info( "Starting DBNatorSystem " );
        log.info( " - {} workers reading from a queue of size {} in chunks of {}", dbWorkerCount, queueSize, chunkSize );
        log.info( " - They will wait {} ms after an error to retry", sleepAfterFailMilliseconds );
        log.info( String.format( " - The queue is too full at %.2f %% and empty enough at %.2f %%", 100 * highWatermark, 100 * lowWatermark) );
    }

    @Override
    public void boot( E env ) throws Exception {
        LinkedBlockingQueue<Metric> incomingQueue = new LinkedBlockingQueue<>( queueSize );
        env.setDbInputQueue( incomingQueue );
        env.addWatermark( new QueueWatermark( lowWatermark, highWatermark, incomingQueue, queueSize ) );

        drain = new SerialFailFirstDrain( Arrays.asList(
                new PersistentDrainFrontend( env.persistentDrainManager().getPersistentDrain( persistentDrainName ) ),
                new PersistentDrainFrontend( env.persistentDrainManager().getPersistentDrain( persistentBifroestDrainName ) ) ) );
        executor = Executors.newFixedThreadPool( dbWorkerCount );
        executor.submit( new DBNator( executor,
                                      incomingQueue,
                                      chunkSize,
                                      drain,
                                      sleepAfterFailMilliseconds ) );

        EventBusManager.createRegistrationPoint().subscribe( WriteToStorageEvent.class, w -> {
            MetricStorage storage = w.storageToWriteTo().getSubStorageCalled( "dbnator" );
            storage.store( "queue-size", incomingQueue.size() );
            storage.store( "metrics-to-persistent-drains", DBNator.getMetricsread() );
        });
    }

    @Override
    public void shutdown( E env ) {
        executor.shutdownNow();
        try {
            executor.awaitTermination( 5, TimeUnit.MINUTES );
        } catch( InterruptedException e ) {
            // ignore
        }
        try {
            drain.close();
        } catch( IOException e ) {
            log.warn( "Exception while closing drain", e );
        }
    }
}
