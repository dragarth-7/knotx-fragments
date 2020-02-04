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

import static io.netty.handler.codec.http.HttpStatusClass.CLIENT_ERROR;
import static io.netty.handler.codec.http.HttpStatusClass.SERVER_ERROR;

import io.knotx.fragments.handler.api.actionlog.ActionLogLevel;
import io.knotx.fragments.handler.api.actionlog.ActionLogger;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.client.HttpResponse;

class HttpActionLogger {

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

  static HttpActionLogger create(String actionAlias, ActionLogLevel logLevel, EndpointOptions endpointOptions) {
    return new HttpActionLogger(ActionLogger.create(actionAlias, logLevel), endpointOptions);
  }

  void error(Throwable error) {
    actionLogger.error(error);
  }

  JsonObject getJsonLog() {
    return actionLogger.toLog().toJson();
  }

  void onRequestSucceeded(HttpResponse<Buffer> response) {
    this.httpResponseData = HttpResponseData.from(response);
    this.httpResponseBody = response.body();
    JsonObject responseData = getResponseData(httpResponseData);
    if (isHttpErrorResponse(httpResponseData)) {
      LOGGER.error("GET {} -> Error response {}, headers[{}]",
          logResponseData());
    } else if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("GET {} -> Got response {}, headers[{}]",
          logResponseData());
    }
    actionLogger.info(RESPONSE, responseData);
  }

  private Object[] logResponseData() {
    JsonObject headers = new JsonObject();
    httpResponseData.getHeaders().entries().forEach(e -> headers.put(e.getKey(), e.getValue()));
    return new Object[]{
        getRequestPath(),
        httpResponseData.getStatusCode(),
        headers
    };
  }

  void onResponseProcessingFailed(Throwable throwable) {
    JsonObject responseData = getResponseData(httpResponseData);
    actionLogger.error(RESPONSE, responseData);
    logRequest(ActionLogLevel.ERROR);
    logError(throwable);
  }

  void onRequestFailed(Throwable throwable) {
    logRequest(ActionLogLevel.ERROR);
    logError(throwable);
  }

  void onRequestCreation(EndpointRequest endpointRequest) {
    this.endpointRequest = endpointRequest;
    logRequest(ActionLogLevel.INFO);
  }

  private void logRequest(ActionLogLevel level) {
    JsonObject headers = getHeadersFromRequest(endpointRequest);
    JsonObject requestLog = new JsonObject().put("path", endpointRequest.getPath())
        .put("requestHeaders", headers);
    if(ActionLogLevel.INFO.equals(level)) {
      actionLogger.info(REQUEST, requestLog);
    } else {
      actionLogger.error(REQUEST, requestLog);
    }
  }

  private JsonObject getHeadersFromRequest(EndpointRequest request) {
    JsonObject headers = new JsonObject();
    request.getHeaders().entries().forEach(e -> headers.put(e.getKey(), e.getValue()));
    return headers;
  }

  private void logError(Throwable throwable) {
    actionLogger.error(throwable);
  }

  private boolean isHttpErrorResponse(HttpResponseData resp) {
    return CLIENT_ERROR.contains(Integer.parseInt(resp.getStatusCode())) || SERVER_ERROR
        .contains(Integer.parseInt(resp.getStatusCode()));
  }

  private JsonObject getResponseData(HttpResponseData responseData) {
    JsonObject json = responseData.toJson();
    return json.put("httpMethod", HttpMethod.GET)
        .put("requestPath", getRequestPath());
  }

  private String getRequestPath() {
    return endpointOptions.getDomain() + ":" + endpointOptions.getPort() + endpointRequest.getPath();
  }

  void onResponseCodeVerified() {
    actionLogger.info(RESPONSE_BODY, httpResponseBody != null ? httpResponseBody.toString() : Buffer.buffer());
  }
}
