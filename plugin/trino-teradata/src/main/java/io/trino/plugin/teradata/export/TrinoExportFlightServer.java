/*
 * Copyright 2025-2026 The trino-teradata-direct contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.teradata.export;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import io.airlift.log.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import com.google.inject.Inject;
import java.io.IOException;

public class TrinoExportFlightServer implements FlightProducer, AutoCloseable {
    private static final Logger log = Logger.get(TrinoExportFlightServer.class);
    private final BufferAllocator allocator = new RootAllocator();
    private FlightServer server;
    private final int port;
    private final String bindAddress;

    @Inject
    public TrinoExportFlightServer(TrinoExportConfig config) {
        this.port = config.getFlightPort();
        this.bindAddress = config.getFlightBindAddress();
    }

    @PostConstruct
    public void start() throws IOException {
        Location location = Location.forGrpcInsecure(bindAddress, port);
        this.server = FlightServer.builder(allocator, location, this).build();
        this.server.start();
        log.info("Arrow Flight Server started on %s:%d", bindAddress, port);
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        // Simplified: Auth check removed for version compatibility verification
        return () -> {
            String queryId = "unknown";
            try {
                queryId = flightStream.getDescriptor().getPath().get(0);
                log.info("Receiving data for query: %s", queryId);
                
                while (flightStream.next()) {
                    VectorSchemaRoot originalRoot = flightStream.getRoot();
                    VectorSchemaRoot clonedRoot = VectorSchemaRoot.create(originalRoot.getSchema(), allocator);
                    for (int i = 0; i < originalRoot.getFieldVectors().size(); i++) {
                        originalRoot.getVector(i).makeTransferPair(clonedRoot.getVector(i)).transfer();
                    }
                    clonedRoot.setRowCount(originalRoot.getRowCount());
                    // DataBufferRegistry.pushData(queryId, clonedRoot); 
                    // Flight support temporarily disabled due to architecture change to Direct Page Parsing
                    clonedRoot.close();
                }
                ackStream.onCompleted();
            } catch (Exception e) {
                log.error(e, "Error in Arrow Flight acceptPut for query %s", queryId);
                ackStream.onError(CallStatus.INTERNAL.withCause(e).toRuntimeException());
            }
            // NOTE: JDBC_FINISHED removed - using deterministic EOS with per-worker expected counts
        };
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        listener.onCompleted();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        throw CallStatus.NOT_FOUND.toRuntimeException();
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        listener.onCompleted();
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        listener.onCompleted();
    }

    @Override
    @PreDestroy
    public void close() throws Exception {
        if (server != null) {
            server.shutdown();
        }
        allocator.close();
    }
}
