package io.bifroest.stream_rewriter.driver;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import io.bifroest.commons.boot.interfaces.Subsystem;
import io.bifroest.commons.cron.TaskRunner;
import io.bifroest.commons.cron.TaskRunner.TaskID;
import io.bifroest.commons.statistics.units.parse.DurationParser;
import io.bifroest.stream_rewriter.StreamRewriterIdentifiers;
import io.bifroest.stream_rewriter.netty.EnvironmentWithNettyPortController;
import io.bifroest.stream_rewriter.watermark.EnvironmentWithWatermark;

@MetaInfServices
public class DriverSystem<E extends EnvironmentWithNettyPortController & EnvironmentWithWatermark> implements Subsystem<E> {
    private static final Logger log = LogManager.getLogger();

    private Duration checkFrequency;

    private TaskID task;

    @Override
    public String getSystemIdentifier() {
        return StreamRewriterIdentifiers.DRIVER;
    }

    @Override
    public Collection<String> getRequiredSystems() {
        return Arrays.asList( StreamRewriterIdentifiers.DBNATOR, StreamRewriterIdentifiers.NETTY );
    }

    @Override
    public void configure( JSONObject configuration ) {
        checkFrequency = new DurationParser().parse( configuration.getJSONObject( "driver" ).getString( "check-frequency" ) );
    }

    @Override
    public void boot( final E environment ) throws Exception {
        task = TaskRunner.runRepeated( () -> {
            log.trace( "Driver checking stuff!" );
            if ( environment.nettyPortController().isPortOpen() ) {
                log.trace( "Am I over high watermark?" );
                if ( environment.watermark().overHigh() ) {
                    log.trace( "Yes I am!" );
                    try {
                        environment.nettyPortController().closeNettyPort();
                    } catch( Exception e ) {
                        log.warn( "Interrupted while waiting for netty port to close!", e );
                    }
                } else {
                    log.trace( "No I am not!" );
                }
            } else {
                log.trace( "Am I below low watermark?" );
                if ( environment.watermark().belowLow() ) {
                    log.trace( "Yes I am!" );
                    try {
                        environment.nettyPortController().openNettyPort();
                    } catch( Exception e ) {
                        log.warn( "Interrupted while waiting for netty port to open!", e );
                    }
                } else {
                    log.trace( "No I am not!" );
                }
            }
        }, "check-watermark", Duration.ZERO, checkFrequency, false );
    }

    @Override
    public void shutdown( E environment ) {
        TaskRunner.stopTask( task );
    }
}
