/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce;

import io.prometheus.client.vertx.MetricsHandler;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusMetricsServer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private final int port;

    public PrometheusMetricsServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        Vertx vertx = Vertx.vertx();
        Router router = Router.router(vertx);
        router.route("/metrics").handler(new MetricsHandler());
        vertx.createHttpServer().requestHandler(router::accept).listen(port);
        log.info("Started metric server on port {}", port);
    }
}
