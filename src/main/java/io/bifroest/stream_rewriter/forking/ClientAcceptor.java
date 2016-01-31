/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.goodgame.profiling.stream_rewriter.forking;

import com.goodgame.profiling.commons.systems.configuration.EnvironmentWithJSONConfiguration;
import com.goodgame.profiling.drains.DrainCreator;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.logging.log4j.LogManager;
import org.json.JSONObject;

/**
 *
 * @author hkraemer@ggs-hh.net
 */
public class ClientAcceptor<E extends EnvironmentWithJSONConfiguration> implements Runnable {
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger();

    private final int bindPort;
    private final JSONObject drainConfig;
    private final DrainCreator<E> creator;
    private final E env;
    
    private volatile boolean running;
    private ServerSocket socket;
    public ClientAcceptor( E env, int bindPort, JSONObject drainConfig ) {
        this.bindPort = bindPort;
        this.drainConfig = drainConfig;
        this.creator = new DrainCreator<>();
        this.env = env;
        this.running = true;
    }
    
    @Override
    public void run() {
        try {
            
            socket = new ServerSocket( bindPort );
            while ( running ) {
                Socket client = socket.accept();
                Thread clientHandler = new Thread( new ClientHandler( client,
                                                                      creator.loadFromDrainConfiguration(env, drainConfig)) );
                // Diamnd keeps connections open forever, so if we only shut down
                // when all client connections are closed, this system will never
                // shut down.
                clientHandler.setDaemon( true ); 
                clientHandler.start();
            }
        } catch ( IOException e ) {
            throw new IllegalStateException( "Cannot create server socket ", e);
        }
    }

    void stop() {
        if ( socket == null ) throw new IllegalStateException();
        
        running = false;
        try {
            socket.close();
        } catch ( IOException ex ) {
            log.warn( "Error closing server socket" );
        }
    }
}
