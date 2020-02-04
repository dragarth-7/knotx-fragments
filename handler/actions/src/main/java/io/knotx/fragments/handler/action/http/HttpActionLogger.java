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

  void logResponse(EndpointRequest endpointRequest, HttpResponse<Buffer> response) {
    HttpResponseData httpResponseData = HttpResponseData.from(response);
    JsonObject responseData = getResponseData(endpointRequest, httpResponseData);
    if (isHttpErrorResponse(httpResponseData)) {
      LOGGER.error("GET {} -> Error response {}, headers[{}]",
          logResponseData(endpointRequest, httpResponseData));
    } else if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("GET {} -> Got response {}, headers[{}]",
          logResponseData(endpointRequest, httpResponseData));
    }
    actionLogger.info(RESPONSE, responseData);
  }

  private Object[] logResponseData(EndpointRequest request, HttpResponseData responseData) {
    JsonObject headers = new JsonObject();
    responseData.getHeaders().entries().forEach(e -> headers.put(e.getKey(), e.getValue()));
    return new Object[]{
        toUrl(request),
        responseData.getStatusCode(),
        headers
    };
  }

  void logResponseOnError(EndpointRequest request, HttpResponseData httpResponseData) {
    JsonObject responseData = getResponseData(request, httpResponseData);
    actionLogger.error(RESPONSE, responseData);
  }

  void logErrorAndRequest(Throwable throwable, EndpointRequest request) {
    JsonObject headers = getHeadersFromRequest(request);
    actionLogger.error(REQUEST, new JsonObject().put("path", request.getPath())
        .put("requestHeaders", headers));
    actionLogger.error(throwable);
  }

  void logRequest(EndpointRequest request) {
    JsonObject headers = getHeadersFromRequest(request);
    actionLogger.info(REQUEST, new JsonObject().put("path", request.getPath())
        .put("requestHeaders", headers));
  }

  private JsonObject getHeadersFromRequest(EndpointRequest request) {
    JsonObject headers = new JsonObject();
    request.getHeaders().entries().forEach(e -> headers.put(e.getKey(), e.getValue()));
    return headers;
  }

  private boolean isHttpErrorResponse(HttpResponseData resp) {
    return CLIENT_ERROR.contains(Integer.parseInt(resp.getStatusCode())) || SERVER_ERROR
        .contains(Integer.parseInt(resp.getStatusCode()));
  }

  private JsonObject getResponseData(EndpointRequest request, HttpResponseData responseData) {
    JsonObject json = responseData.toJson();
    return json.put("httpMethod", HttpMethod.GET)
        .put("requestPath", toUrl(request));
  }

  private String toUrl(EndpointRequest request) {
    return endpointOptions.getDomain() + ":" + endpointOptions.getPort() + request.getPath();
  }

  void logInfoResponseBody(EndpointResponse endpointResponse) {
    actionLogger.info(RESPONSE_BODY, endpointResponse.getBody().toString());
  }
}
