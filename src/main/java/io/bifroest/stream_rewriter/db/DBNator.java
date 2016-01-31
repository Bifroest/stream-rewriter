package com.goodgame.profiling.stream_rewriter.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.goodgame.profiling.commons.model.Metric;
import com.goodgame.profiling.drains.Drain;

/**
 *
 * @author hkraemer@ggs-hh.net
 */
public class DBNator implements Callable<Void> {
    private static final Logger log = LogManager.getLogger();
    
    private static final AtomicLong metricsRead = new AtomicLong();
    
    private final ExecutorService executor;
    private final LinkedBlockingQueue<Metric> incomingQueue;
    private final int chunkSize;
    private final Drain drain;
    private final long sleepTime;
    
    public DBNator( ExecutorService executor, LinkedBlockingQueue<Metric> incomingQueue, int chunkSize, Drain drain, long sleepTime ) {
        this.executor = executor;
        this.incomingQueue = incomingQueue;
        this.chunkSize = chunkSize;
        this.drain = drain;
        this.sleepTime = sleepTime;
    }

    @Override
    public Void call() throws Exception {
        try {
            List<Metric> chunk = new ArrayList<>( chunkSize );
            while ( chunk.size() < chunkSize ) {
                Metric m = this.incomingQueue.poll( 1, TimeUnit.SECONDS );
                if ( m == null ) {
                    if ( !chunk.isEmpty() ) {
                        break;
                    }
                } else {
                    chunk.add( m );
                }
            }
            log.debug( "Metrics read: " + metricsRead.addAndGet( chunk.size() ) );
            this.executor.submit( new DBNator( executor, incomingQueue, chunkSize, drain, sleepTime ));
            outputToDb( chunk );
        } catch( Exception e ) {
            log.error( "Exception in DBNator", e );
        }
        return null;
    }
    
    private void outputToDb( List<Metric> chunk ) {
        while (true) {
            try {
                log.debug( "Outputting {} metrics to drain", chunk.size() );
                this.drain.output( chunk );
                log.debug( "Done." );
                return;
            } catch( WriteTimeoutException e ) {
                log.info( "Received Write Timeout exception, sleeping without retry" );
                try {
                    Thread.sleep( sleepTime );
                } catch ( InterruptedException e2 ) {
                }
                return;
            } catch ( IOException|DriverException e3 ) {
                log.info( "Received exception, sleeping and retrying", e3 );
                try {
                    Thread.sleep( sleepTime );
                } catch ( InterruptedException e2 ) {
                }
            }
        }
    }

    public static long getMetricsread() {
        return metricsRead.longValue();
    }
}
