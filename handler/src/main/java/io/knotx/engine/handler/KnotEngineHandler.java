/*
 * Copyright (C) 2019 Knot.x Project
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
 *
 * The code comes from https://github.com/tomaszmichalak/vertx-rx-map-reduce.
 */
package io.knotx.engine.handler;

import io.knotx.engine.core.FragmentEvent;
import io.knotx.engine.core.FragmentEventContext;
import io.knotx.engine.core.FragmentEventContextGraphAware;
import io.knotx.engine.core.FragmentsEngine;
import io.knotx.engine.handler.options.KnotEngineHandlerOptions;
import io.knotx.engine.handler.proxy.OperationProxyFactoryProvider;
import io.knotx.engine.handler.proxy.OperationProxyProvider;
import io.knotx.fragment.Fragment;
import io.knotx.server.api.context.ClientRequest;
import io.knotx.server.api.context.RequestContext;
import io.knotx.server.api.context.RequestEvent;
import io.knotx.server.api.handler.DefaultRequestContextEngine;
import io.knotx.server.api.handler.RequestContextEngine;
import io.knotx.server.api.handler.RequestEventHandlerResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.RoutingContext;
import java.util.List;
import java.util.stream.Collectors;

public class KnotEngineHandler implements Handler<RoutingContext> {

  private final FragmentsEngine engine;
  private final RequestContextEngine requestContextEngine;
  private final GraphBuilder graphBuilder;

  KnotEngineHandler(Vertx vertx, JsonObject config) {
    KnotEngineHandlerOptions options = new KnotEngineHandlerOptions(config);

    OperationProxyProvider proxyProvider = new OperationProxyProvider(options.getOperations(),
        new OperationProxyFactoryProvider(), vertx.getDelegate());
    graphBuilder = new GraphBuilder(options.getFlows(), proxyProvider);

    engine = new FragmentsEngine(vertx);
    requestContextEngine = new DefaultRequestContextEngine(getClass().getSimpleName());
  }

  @Override
  public void handle(RoutingContext routingContext) {
    RequestContext requestContext = routingContext.get(RequestContext.KEY);
    List<Fragment> fragments = requestContext.getRequestEvent().getFragments();
    ClientRequest clientRequest = requestContext.getRequestEvent().getClientRequest();

    engine.execute(toEvents(fragments, clientRequest))
        .map(events -> toHandlerResult(events, requestContext))
        .subscribe(
            result -> requestContextEngine
                .processAndSaveResult(result, routingContext, requestContext),
            error -> requestContextEngine.handleFatal(routingContext, requestContext, error)
        );
  }

  private RequestEventHandlerResult toHandlerResult(List<FragmentEvent> events,
      RequestContext requestContext) {
    RequestEvent requestEvent = updateRequestEvent(requestContext.getRequestEvent(), events);
    return RequestEventHandlerResult.success(requestEvent);
  }

  private RequestEvent updateRequestEvent(RequestEvent requestEvent, List<FragmentEvent> events) {
    // TODO implement error handling: now we process all fragments, even they are invalid
    List<Fragment> fragments = events.stream().map(FragmentEvent::getFragment)
        .collect(Collectors.toList());
    return new RequestEvent(requestEvent.getClientRequest(), fragments, requestEvent.getPayload());
  }

  private List<FragmentEventContextGraphAware> toEvents(List<Fragment> fragments,
      ClientRequest clientRequest) {
    return fragments.stream()
        .map(
            fragment -> {
              FragmentEventContext fragmentEventContext = new FragmentEventContext(
                  new FragmentEvent(fragment), clientRequest);
              return graphBuilder.build(fragment).map(
                  graphNode -> new FragmentEventContextGraphAware(fragmentEventContext, graphNode))
                  .orElseGet(() -> new FragmentEventContextGraphAware(fragmentEventContext));
            })
        .collect(
            Collectors.toList());
  }
}
