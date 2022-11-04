/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

import io.netty.handler.codec.CorruptedFrameException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.server.AbstractGremlinServerIntegrationTest;
import org.apache.tinkerpop.gremlin.server.TestClientFactory;
import org.apache.tinkerpop.gremlin.util.Log4jRecordingAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLHandshakeException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClientConnectionIntegrateTest extends AbstractGremlinServerIntegrationTest {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ClientConnectionIntegrateTest.class);
    private Log4jRecordingAppender recordingAppender = null;
    private Level previousLogLevel;

    @Before
    public void setupForEachTest() {
        recordingAppender = new Log4jRecordingAppender();
        final Logger rootLogger = Logger.getRootLogger();

        if (name.getMethodName().equals("shouldCloseConnectionDeadDueToUnRecoverableError")) {
            final org.apache.log4j.Logger connectionLogger = org.apache.log4j.Logger.getLogger(Connection.class);
            previousLogLevel = connectionLogger.getLevel();
            connectionLogger.setLevel(Level.DEBUG);
        }

        rootLogger.addAppender(recordingAppender);
    }

    @After
    public void teardownForEachTest() {
        final Logger rootLogger = Logger.getRootLogger();

        if (name.getMethodName().equals("shouldCloseConnectionDeadDueToUnRecoverableError")) {
            final org.apache.log4j.Logger connectionLogger = org.apache.log4j.Logger.getLogger(Connection.class);
            connectionLogger.setLevel(previousLogLevel);
        }

        rootLogger.removeAppender(recordingAppender);
    }

    /**
     * Reproducer for TINKERPOP-2169
     */
    @Test
    public void shouldCloseConnectionDeadDueToUnRecoverableError() throws Exception {
        // Set a low value of maxContentLength to intentionally trigger CorruptedFrameException
        final Cluster cluster = TestClientFactory.build()
                                                 .serializer(Serializers.GRAPHBINARY_V1D0)
                                                 .maxContentLength(64)
                                                 .minConnectionPoolSize(1)
                                                 .maxConnectionPoolSize(2)
                                                 .create();
        final Client.ClusteredClient client = cluster.connect();

        try {
            // Add the test data so that the g.V() response could exceed maxContentLength
            client.submit("g.inject(1).repeat(__.addV()).times(20).count()").all().get();
            try {
                client.submit("g.V().fold()").all().get();

                fail("Should throw an exception.");
            } catch (Exception re) {
                assertThat(re.getCause() instanceof CorruptedFrameException, is(true));
            }

            // without this wait this test is failing randomly on docker/travis with ConcurrentModificationException
            // see TINKERPOP-2504 - adjust the sleep to account for the max time to wait for sessions to close in
            // an orderly fashion
            Thread.sleep(Connection.MAX_WAIT_FOR_CLOSE + 1000);

            // Assert that the host has not been marked unavailable
            assertEquals(1, cluster.availableHosts().size());

            // Assert that there is no connection leak and all connections have been closed
            assertEquals(0, client.hostConnectionPools.values().stream()
                                                             .findFirst().get()
                                                             .numConnectionsWaitingToCleanup());
        } finally {
            cluster.close();
        }

        // Assert that the connection has been destroyed. Specifically check for the string with
        // isDead=true indicating the connection that was closed due to CorruptedFrameException.
        assertThat(recordingAppender.logContainsAny("^(?!.*(isDead=false)).*isDead=true.*destroyed successfully.$"), is(true));

    }

    @Test
    public void shouldEventuallySucceedAfterHostBecomesUnavailable() throws Exception {
        final int maxWaitMs = 4000;
        final Cluster cluster = TestClientFactory.build().minConnectionPoolSize(2).maxConnectionPoolSize(4).
                maxWaitForConnection(maxWaitMs).validationRequest("g.inject()").create();
        final Client.ClusteredClient client = cluster.connect();

        client.init();

        final JitteryConnectionFactory connectionFactory = new JitteryConnectionFactory();
        client.hostConnectionPools.forEach((h, pool) -> pool.connectionFactory = connectionFactory);

        // get an initial connection which marks the host as available
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());

//        // no more server - bye!
        //stopServer();
//
//        // send another request which will mark the host unavailable, where it timesout
//        try {
//            client.submit("1+1").all().join().get(0).getInt();
//            fail("Should not have gone through because the server is not running anymore");
//        } catch (Exception i) {
//            final Throwable t = ExceptionHelper.getRootCause(i);
//            assertThat(t, instanceOf(TimeoutException.class));
//        }
//
//        try {
//            client.submit("1+1").all().join().get(0).getInt();
//            fail("Should not have gone through because the server is not running anymore");
//        } catch (Exception i) {
//            final Throwable t = ExceptionHelper.getRootCause(i);
//            assertThat(t, instanceOf(TimeoutException.class));
//        }

        // network is gonna get fishy
        connectionFactory.jittery = true;

        // load up a hella ton of requests
        final AtomicInteger nhaFailures = new AtomicInteger(0);
        final int requests = 1000;
        final CountDownLatch latch = new CountDownLatch(requests);
        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (Exception ignored) {}

            IntStream.range(0, requests).forEach(i -> {
                try {
                    client.submitAsync("1 + " + i);
                } catch (Exception ex) {
                    if (ex instanceof NoHostAvailableException)
                        nhaFailures.incrementAndGet();
                    else {
                        logger.warn("Expecting {} typically but got a {} with message: {}",
                                NoHostAvailableException.class.getSimpleName(),
                                ex.getClass().getName(), ex.getMessage());
                        logger.warn("XXX", ex);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }).start();

        // startServerAsync();

        // wait long enough for the jitters to kick in at least a little
        Thread.sleep(10000);

        // default reconnect time is 1 second so wait some extra time to be sure it has time to try to bring it
        // back to life. usually this passes on the first attempt, but docker is sometimes slow and we get failures
        // waiting for Gremlin Server to pop back up
        boolean eventuallyWorked = false;

        // no more jitters
        connectionFactory.jittery = false;
        for (int ix = 3; ix < 33; ix++) {
            TimeUnit.SECONDS.sleep(ix);
            try {
                final int result = client.submit("1+1").all().join().get(0).getInt();
                assertEquals(2, result);
                eventuallyWorked = true;
                break;
            } catch (Exception ignored) {
                logger.warn("Attempt {} failed on shouldEventuallySucceedOnSameServerWithScript", ix);
            }
        }

        assertThat(eventuallyWorked, is(true));

        assertTrue(latch.await(90000, TimeUnit.MILLISECONDS));
        logger.warn("Ended with {}", nhaFailures.get());

        cluster.close();
    }

    /**
     * Introduces random failures when creating a {@link Connection} for the {@link ConnectionPool}.
     */
    public static class JitteryConnectionFactory implements ConnectionFactory {

        private volatile boolean jittery = false;

        @Override
        public Connection create(final ConnectionPool pool) {

            // fail creating a connection every 10 attempts or so when jittery
            if (jittery && TestHelper.RANDOM.nextInt(10) == 0)
                throw new ConnectionException(pool.host.getHostUri(),
                        new SSLHandshakeException("SSL on the funk - server is big mad"));

            return ConnectionFactory.super.create(pool);
        }
    }
}
