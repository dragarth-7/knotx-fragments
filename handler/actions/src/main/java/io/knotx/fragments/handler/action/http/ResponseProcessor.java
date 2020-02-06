package io.knotx.fragments.handler.action.http;

import static io.knotx.fragments.handler.api.domain.FragmentResult.ERROR_TRANSITION;
import static io.netty.handler.codec.http.HttpStatusClass.SUCCESS;

import io.knotx.fragments.api.Fragment;
import io.knotx.fragments.handler.action.http.log.HttpActionLogger;
import io.knotx.fragments.handler.action.http.options.HttpActionOptions;
import io.knotx.fragments.handler.api.domain.FragmentContext;
import io.knotx.fragments.handler.api.domain.FragmentResult;
import io.knotx.fragments.handler.api.domain.payload.ActionPayload;
import io.knotx.fragments.handler.api.domain.payload.ActionRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;

public class ResponseProcessor {

  public static final String TIMEOUT_TRANSITION = "_timeout";
  private static final String HTTP_ACTION_TYPE = "HTTP";
  private static final String METADATA_HEADERS_KEY = "headers";
  private static final String METADATA_STATUS_CODE_KEY = "statusCode";
  private static final String APPLICATION_JSON = "application/json";
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String JSON = "JSON";
  private final boolean isJsonPredicate;
  private final boolean isForceJson;
  private final String actionAlias;

  ResponseProcessor(HttpActionOptions httpActionOptions, String actionAlias) {
    this.isJsonPredicate = httpActionOptions.getResponseOptions().getPredicates()
        .contains(JSON);
    this.isForceJson = httpActionOptions.getResponseOptions().isForceJson();
    this.actionAlias = actionAlias;
  }

  FragmentResult createFragmentResult(FragmentContext fragmentContext,
      EndpointRequest endpointRequest, EndpointResponse endpointResponse,
      HttpActionLogger actionLogger) {
    ActionRequest request = createActionRequest(endpointRequest);
    final ActionPayload payload;
    final String transition;
    if (SUCCESS.contains(endpointResponse.getStatusCode().code())) {
      actionLogger.onResponseCodeSuccessful();
      payload = getActionPayload(endpointResponse, actionLogger, request);
      transition = FragmentResult.SUCCESS_TRANSITION;
    } else {
      actionLogger.onResponseCodeUnsuccessful(new IOException(
          "The service responded with unsuccessful status code: " + endpointResponse.getStatusCode()
              .code()));
      payload = handleErrorResponse(request, endpointResponse.getStatusCode().toString(),
          endpointResponse.getStatusMessage());
      transition = getErrorTransition(endpointResponse);
    }
    updateResponseMetadata(endpointResponse, payload);
    Fragment fragment = fragmentContext.getFragment();
    fragment.appendPayload(actionAlias, payload.toJson());
    return new FragmentResult(fragment, transition, actionLogger.getJsonLog());
  }

  private String getErrorTransition(EndpointResponse endpointResponse) {
    if (isTimeout(endpointResponse)) {
      return TIMEOUT_TRANSITION;
    } else {
      return ERROR_TRANSITION;
    }
  }

  private ActionPayload getActionPayload(EndpointResponse endpointResponse,
      HttpActionLogger actionLogger, ActionRequest request) {
    try {
      return handleSuccessResponse(endpointResponse, request);
    } catch (Exception e) {
      actionLogger.onResponseProcessingFailed(e);
      throw e;
    }
  }

  private ActionRequest createActionRequest(EndpointRequest endpointRequest) {
    ActionRequest request = new ActionRequest(HTTP_ACTION_TYPE, endpointRequest.getPath());
    request.appendMetadata(METADATA_HEADERS_KEY, headersToJsonObject(endpointRequest.getHeaders()));
    return request;
  }

  private void updateResponseMetadata(EndpointResponse response, ActionPayload payload) {
    payload.getResponse()
        .appendMetadata(METADATA_STATUS_CODE_KEY, String.valueOf(response.getStatusCode().code()))
        .appendMetadata(METADATA_HEADERS_KEY, headersToJsonObject(response.getHeaders()));
  }

  private ActionPayload handleErrorResponse(ActionRequest request, String statusCode,
      String statusMessage) {
    return ActionPayload.error(request, statusCode, statusMessage);
  }

  private ActionPayload handleSuccessResponse(EndpointResponse response, ActionRequest request) {
    if (isForceJson || isJsonPredicate || isContentTypeHeaderJson(response)) {
      return ActionPayload.success(request, bodyToJson(response.getBody().toString()));
    } else {
      return ActionPayload.success(request, response.getBody().toString());
    }
  }

  private boolean isContentTypeHeaderJson(EndpointResponse endpointResponse) {
    String contentType = endpointResponse.getHeaders().get(CONTENT_TYPE);
    return contentType != null && contentType.contains(APPLICATION_JSON);
  }

  private Object bodyToJson(String responseBody) {
    if (StringUtils.isBlank(responseBody)) {
      return new JsonObject();
    } else if (responseBody.startsWith("[")) {
      return new JsonArray(responseBody);
    } else {
      return new JsonObject(responseBody);
    }
  }

  private boolean isTimeout(EndpointResponse response) {
    return HttpResponseStatus.REQUEST_TIMEOUT == response.getStatusCode();
  }

  private JsonObject headersToJsonObject(MultiMap headers) {
    JsonObject responseHeaders = new JsonObject();
    headers.entries().forEach(entry -> {
      final JsonArray values;
      if (responseHeaders.containsKey(entry.getKey())) {
        values = responseHeaders.getJsonArray(entry.getKey());
      } else {
        values = new JsonArray();
      }
      responseHeaders.put(entry.getKey(), values.add(entry.getValue())
      );
    });
    return responseHeaders;
  }

}
