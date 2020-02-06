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
 */
package io.knotx.fragments.handler.action.http;

import io.knotx.fragments.handler.action.http.log.HttpActionLogger;
import io.knotx.fragments.handler.action.http.options.EndpointOptions;
import io.knotx.fragments.handler.action.http.options.HttpActionOptions;
import io.knotx.fragments.handler.api.Action;
import io.knotx.fragments.handler.api.actionlog.ActionLogLevel;
import io.knotx.fragments.handler.api.domain.FragmentContext;
import io.knotx.fragments.handler.api.domain.FragmentResult;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.Single;
import io.reactivex.exceptions.Exceptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.reactivex.ext.web.client.WebClient;
import java.util.concurrent.TimeoutException;

public class HttpAction implements Action {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpAction.class);
  private final EndpointInvoker endpointInvoker;

  private final EndpointOptions endpointOptions;
  private final String actionAlias;
  private final ActionLogLevel logLevel;
  private final ResponseProcessor responseProcessor;

  private EndpointRequestComposer requestComposer;

  HttpAction(WebClient webClient, HttpActionOptions httpActionOptions, String actionAlias) {
    this.endpointOptions = httpActionOptions.getEndpointOptions();
    this.actionAlias = actionAlias;
    this.logLevel = ActionLogLevel
        .fromConfig(httpActionOptions.getLogLevel(), ActionLogLevel.ERROR);
    this.endpointInvoker = new EndpointInvoker(webClient, httpActionOptions);
    this.requestComposer = new EndpointRequestComposer(endpointOptions);
    this.responseProcessor = new ResponseProcessor(httpActionOptions, actionAlias);
  }

  @Override
  public void apply(FragmentContext fragmentContext,
      Handler<AsyncResult<FragmentResult>> resultHandler) {
    HttpActionLogger httpActionLogger = HttpActionLogger
        .create(actionAlias, logLevel, endpointOptions);
    process(fragmentContext, httpActionLogger)
        .map(Future::succeededFuture)
        .map(future -> future.setHandler(resultHandler))
        .subscribe();
  }

  private Single<FragmentResult> process(FragmentContext fragmentContext,
      HttpActionLogger httpActionLogger) {
    return Single.just(fragmentContext)
        .map(requestComposer::createEndpointRequest)
        .doOnSuccess(httpActionLogger::onRequestCreation)
        .flatMap(
            request -> endpointInvoker.invokeEndpoint(request)
                .doOnSuccess(httpActionLogger::onRequestSucceeded)
                .doOnError(httpActionLogger::onRequestFailed)
                .map(EndpointResponse::fromHttpResponse)
                .onErrorReturn(HttpAction::handleTimeout)
                .map(response -> responseProcessor.createFragmentResult(fragmentContext, request, response,
                    httpActionLogger)))
        .doOnError(httpActionLogger::onDifferentError)
        .onErrorReturn(error -> errorTransition(fragmentContext, httpActionLogger));
  }

  private FragmentResult errorTransition(FragmentContext fragmentContext,
      HttpActionLogger actionLogger) {
    return new FragmentResult(fragmentContext.getFragment(), FragmentResult.ERROR_TRANSITION,
        actionLogger.getJsonLog());
  }

  private static EndpointResponse handleTimeout(Throwable throwable) {
    if (throwable instanceof TimeoutException) {
      LOGGER.error("Error timeout: ", throwable);
      return new EndpointResponse(HttpResponseStatus.REQUEST_TIMEOUT);
    }
    throw Exceptions.propagate(throwable);
  }
}
