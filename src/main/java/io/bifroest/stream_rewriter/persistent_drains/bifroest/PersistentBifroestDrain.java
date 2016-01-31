package io.bifroest.stream_rewriter.persistent_drains.bifroest;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.bifroest.commons.model.Metric;
import io.bifroest.commons.statistics.WriteToStorageEvent;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.commons.statistics.storage.MetricStorage;
import io.bifroest.stream_rewriter.persistent_drains.PersistentDrain;

/**
 * A special carbon plain text drain. It differs from the normal CarbonPlainTextDrain in the following ways:
 * - It's persistent. If the connection drops, it reconnects.
 * - It has one socket per thread.
 *
 * @author sglimm
 */
public class PersistentBifroestDrain implements PersistentDrain {
    private static final Logger log = LogManager.getLogger();

    private static AtomicLong metricCount = new AtomicLong();
    private static AtomicBoolean eventRegistered = new AtomicBoolean();

    /*
     * We store our OutputStreams and Sockets twice:
     * - in a ConcurrentHashMap, so we can close all of them in shutdown()
     * - in a ThreadLocal, so we can close it when the thread goes away.
     *
     * Accessing the ConcurrentHashMap as a normal map is safe, because
     * - output() only ever accesses the current thread's entry
     * - shutdown() is called single-threaded from the bootloader.
     */
    private final Map<Long, Socket> allSockets = new ConcurrentHashMap<>();
    private final ThreadLocal<Socket> threadLocalSocket = new ThreadLocal<Socket>();
    private final Map<Long, Writer> allWriters = new ConcurrentHashMap<>();
    private final ThreadLocal<Writer> threadLocalWriter = new ThreadLocal<Writer>() {
        @Override
        protected void finalize() throws Throwable {
            disconnect();
        }
    };

    private final String host;
    private final int port;

    public PersistentBifroestDrain( String host, int port ) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void output( Collection<Metric> metrics ) throws IOException {
        if ( !eventRegistered.getAndSet( true ) ) {
            EventBusManager.createRegistrationPoint().subscribe( WriteToStorageEvent.class, w -> {
                MetricStorage storage = w.storageToWriteTo().getSubStorageCalled( "bifroest" );
                storage.store( "metrics-to-bifroest", metricCount.longValue() );
            });
        }

        StringBuilder buffer = new StringBuilder();
        for ( Metric metric : metrics ) {
            String line = metric.name() + " " + metric.value() + " " + metric.timestamp() + "\n";
            buffer.append( line );
        }
        String metricString = buffer.toString();

        if ( !writerCreated() ) {
            connect();
        }

        try {
            doOutput( metricString );
            metricCount.addAndGet( metrics.size() );
        } catch( IOException e ) {
            log.warn( "Exception while outputting metrics - reconnecting!", e );

            try {
                reconnect();

                doOutput( metricString );
                metricCount.addAndGet( metrics.size() );
            } catch( IOException e2 ) {
                log.warn( "Another exception - giving up!", e );

                disconnect();
            }
        }
    }

    private void doOutput( String metrics ) throws IOException {
        Writer w = threadLocalWriter.get();
        w.write( metrics );
        w.flush();
    }

    private boolean writerCreated() {
        return threadLocalWriter.get() != null;
    }

    private void reconnect() throws UnknownHostException, IOException {
        disconnect();

        connect();
    }

    private void connect() throws UnknownHostException, IOException {
        log.info( "Connecting to {}:{} (Thread {})", host, port, Thread.currentThread().getId() );

        Socket socket = new Socket( host, port );
        socket.setKeepAlive( true );
        socket.shutdownInput();
        OutputStream os = socket.getOutputStream();
        Writer w = new OutputStreamWriter( os );

        threadLocalSocket.set( socket );
        allSockets.put( Thread.currentThread().getId(), socket );

        threadLocalWriter.set( w );
        allWriters.put( Thread.currentThread().getId(), w );
    }

    private void disconnect() throws IOException {
        log.info( "Disconnecting (Thread {})", Thread.currentThread().getId() );

        if ( writerCreated() ) {
            threadLocalWriter.get().close();
            threadLocalWriter.set( null );
            allWriters.remove( Thread.currentThread().getId() );

            threadLocalSocket.get().close();
            threadLocalSocket.set( null );
            allSockets.remove( Thread.currentThread().getId() );
        }
    }

    @Override
    public void shutdown() {
        for ( long threadId : allWriters.keySet() ) {
            try {
                log.info( "Disconnecting (Thread {})", threadId );

                allWriters.get( threadId ).close();
                allSockets.get( threadId ).close();
            } catch( IOException e ) {
                log.warn( "Exception during shutdown of PersistentBifroestDrain in thread " + threadId, e );
            }
        }
    }

    @Override
    public void dumpInfos() {
        log.info( "PersistentBifroestDrain: allWriters: " + allWriters.toString() );
    }

    public static void main(String...args) throws Exception {
        PersistentBifroestDrain d = new PersistentBifroestDrain( "10.1.106.42", 5432 );
        while( true ) {
            d.output( Arrays.asList( new Metric( "foo", 1, 2 ) ) );
            System.in.read();
        }
    }
}
