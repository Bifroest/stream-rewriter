/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.bifroest.stream_rewriter.forking;

import java.util.Arrays;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.json.JSONObject;

import io.bifroest.commons.SystemIdentifiers;
import io.bifroest.commons.boot.interfaces.Subsystem;
import io.bifroest.commons.configuration.EnvironmentWithJSONConfiguration;
import io.bifroest.stream_rewriter.StreamRewriterIdentifiers;

/**
 *
 * @author hkraemer@ggs-hh.net
 */
public class ForkingSystem<E extends EnvironmentWithJSONConfiguration> implements Subsystem<E> {
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger();

    private Thread serverThread;
    private ClientAcceptor<E> serverRunnable;
    
    private int port;
    private JSONObject drainConfig;
    
    @Override
    public String getSystemIdentifier() {
        return StreamRewriterIdentifiers.FORKING;
    }

    @Override
    public Collection<String> getRequiredSystems() {
        return Arrays.asList( SystemIdentifiers.STATISTICS, StreamRewriterIdentifiers.PERSISTENT_DRAINS );
    }

    @Override
    public void configure( JSONObject config ) {
        JSONObject systemConfig = config.getJSONObject( "forking" );
                
        port = systemConfig.optInt( "port", 9003 );
        drainConfig = systemConfig.getJSONObject( "drain-template" );
    }

    @Override
    public void boot( E env ) throws Exception {
        log.fatal( "REMEMBER TO HANDLE WRITETIMEOUTEXCEPTIONS!!!einseinself!!!!!!" );
        if ( env != null )
            throw new Error( "REMEMBER TO HANDLE WRITETIMEOUTEXCEPTIONS!!!einseinself!!!!!!" );

        serverRunnable = new ClientAcceptor<>( env, port, drainConfig );
        serverThread = new Thread( serverRunnable );
        serverThread.start();
    }

    @Override
    public void shutdown( E e ) {
        serverRunnable.stop();
        try {
            serverThread.join();
        } catch ( InterruptedException ex ) {
            log.warn( "Interrupted in join", e );
        }
    }
    
}
