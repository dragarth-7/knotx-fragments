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
package io.knotx.fragments.handler.action.http.log;

import static io.netty.handler.codec.http.HttpStatusClass.CLIENT_ERROR;
import static io.netty.handler.codec.http.HttpStatusClass.SERVER_ERROR;

import io.knotx.fragments.handler.action.http.options.EndpointOptions;
import io.knotx.fragments.handler.action.http.EndpointRequest;
import io.knotx.fragments.handler.api.actionlog.ActionLogLevel;
import io.knotx.fragments.handler.api.actionlog.ActionLogger;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.client.HttpResponse;
import org.apache.commons.lang3.StringUtils;

public class HttpActionLogger {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpActionLogger.class);

  private static final String REQUEST = "request";
  private static final String RESPONSE = "response";
  private static final String RESPONSE_BODY = "responseBody";

  private ActionLogger actionLogger;
  private EndpointOptions endpointOptions;

  private EndpointRequest endpointRequest;
  private HttpResponseData httpResponseData;
  private Buffer httpResponseBody;

  private HttpActionLogger(ActionLogger actionLogger, EndpointOptions endpointOptions) {
    this.actionLogger = actionLogger;
    this.endpointOptions = endpointOptions;
  }

  public static HttpActionLogger create(String actionAlias, ActionLogLevel logLevel, EndpointOptions endpointOptions) {
    return new HttpActionLogger(ActionLogger.create(actionAlias, logLevel), endpointOptions);
  }

  public JsonObject getJsonLog() {
    return actionLogger.toLog().toJson();
  }

  public void onRequestCreation(EndpointRequest endpointRequest) {
    this.endpointRequest = endpointRequest;
    logRequest(ActionLogLevel.INFO);
  }

  public void onRequestSucceeded(HttpResponse<Buffer> response) {
    this.httpResponseData = HttpResponseData.from(response);
    this.httpResponseBody = response.body();
    logResponse(ActionLogLevel.INFO);
    logResponseOnVertxLogger();
  }

  public void onRequestFailed(Throwable throwable) {
    logRequest(ActionLogLevel.ERROR);
    logError(throwable);
  }

  public void onResponseCodeUnsuccessful(Throwable throwable) {
    logRequest(ActionLogLevel.ERROR);
    if(httpResponseData != null) {
      logResponse(ActionLogLevel.ERROR);
    }
    logError(throwable);
  }

  public void onResponseProcessingFailed(Throwable throwable) {
    logRequest(ActionLogLevel.ERROR);
    logResponse(ActionLogLevel.ERROR);
    logError(throwable);
  }

  public void onResponseCodeSuccessful() {
    logResponseBody();
  }

  public void onDifferentError(Throwable throwable) {
    if(endpointRequest != null) {
      logRequest(ActionLogLevel.ERROR);
    }
    if(httpResponseData != null) {
      logResponse(ActionLogLevel.ERROR);
    }
    logError(throwable);
  }

  private void logRequest(ActionLogLevel level) {
    log(level, REQUEST, getRequestData());
  }

  private void logResponse(ActionLogLevel level) {
    log(level, RESPONSE, getResponseData());
  }

  private void logResponseBody() {
    String responseBody = httpResponseBody != null ? httpResponseBody.toString() : StringUtils.EMPTY;
    actionLogger.info(RESPONSE_BODY, responseBody);
  }

  private void logError(Throwable throwable) {
    actionLogger.error(throwable);
  }

  private void log(ActionLogLevel logLevel, String key, JsonObject value) {
    if(ActionLogLevel.INFO.equals(logLevel)) {
      actionLogger.info(key, value);
    } else {
      actionLogger.error(key, value);
    }
  }

  private JsonObject getRequestData() {
    return new JsonObject().put("path", endpointRequest.getPath())
        .put("requestHeaders", getHeadersFromRequest());
  }

  private JsonObject getHeadersFromRequest() {
    JsonObject headers = new JsonObject();
    endpointRequest.getHeaders().entries().forEach(e -> headers.put(e.getKey(), e.getValue()));
    // TODO: Multimap entries get overwritten?
    return headers;
  }

  private JsonObject getResponseData() {
    return httpResponseData.toJson().put("httpMethod", HttpMethod.GET)
        .put("requestPath", getRequestPath());
  }

  private String getRequestPath() {
    return endpointOptions.getDomain() + ":" + endpointOptions.getPort() + endpointRequest.getPath();
  }

  private void logResponseOnVertxLogger() {
    if (isHttpErrorResponse()) {
      LOGGER.error("GET {} -> Error response {}, headers[{}]",
          getVertxLogResponseParameters());
    } else if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("GET {} -> Got response {}, headers[{}]",
          getVertxLogResponseParameters());
    }
  }

  private boolean isHttpErrorResponse() {
    return CLIENT_ERROR.contains(Integer.parseInt(httpResponseData.getStatusCode())) || SERVER_ERROR
        .contains(Integer.parseInt(httpResponseData.getStatusCode()));
  }

  private Object[] getVertxLogResponseParameters() {
    JsonObject headers = new JsonObject();
    httpResponseData.getHeaders().entries().forEach(e -> headers.put(e.getKey(), e.getValue()));
    return new Object[]{
        getRequestPath(),
        httpResponseData.getStatusCode(),
        headers
    };
  }

}
