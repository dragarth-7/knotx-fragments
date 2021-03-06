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
package io.knotx.fragments.handler.consumer;

import static io.knotx.fragments.handler.consumer.FragmentHtmlBodyWriterFactory.CONDITION_OPTION;
import static io.knotx.fragments.handler.consumer.FragmentHtmlBodyWriterFactory.FRAGMENT_TYPES_OPTIONS;
import static io.knotx.fragments.handler.consumer.FragmentHtmlBodyWriterFactory.HEADER_OPTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import io.knotx.fragments.api.Fragment;
import io.knotx.fragments.engine.FragmentEvent;
import io.knotx.server.api.context.ClientRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FragmentHtmlBodyWriterFactoryTest {

  public static final String EXPECTED_FRAGMENT_TYPE = "snippet";
  public static final String EXPECTED_HEADER = "x-knotx-debug";
  public static final String EXPECTED_PARAM = "debug";
  private static final String PARAM_OPTION = "param";

  @Test
  @DisplayName("Expect fragment is not modified when condition not configured")
  void expectFragmentNotModifiedWhenConditionNotConfigured() {
    // given
    FragmentEvent original = new FragmentEvent(
        new Fragment(EXPECTED_FRAGMENT_TYPE, new JsonObject(),
            "{ \"body\": \"<div>body</div>\" }"));
    FragmentEvent copy = new FragmentEvent(original.toJson());

    // when
    FragmentEventsConsumer tested = new FragmentHtmlBodyWriterFactory()
        .create(new JsonObject().put(FRAGMENT_TYPES_OPTIONS, new JsonArray().add(
            EXPECTED_FRAGMENT_TYPE)));
    tested.accept(new ClientRequest(), ImmutableList.of(original));

    // then
    assertEquals(copy, original);
  }

  @Test
  @DisplayName("Expect fragment is not modified when supported fragments types not configured.")
  void expectFragmentNotModifiedWhenSupportedTypesNotConfigured() {
    // given
    FragmentEvent original = new FragmentEvent(
        new Fragment(EXPECTED_FRAGMENT_TYPE, new JsonObject(),
            "{ \"body\": \"<div>body</div>\" }"));
    FragmentEvent copy = new FragmentEvent(original.toJson());

    // when
    FragmentEventsConsumer tested = new FragmentHtmlBodyWriterFactory()
        .create(new JsonObject()
            .put(CONDITION_OPTION, new JsonObject().put(HEADER_OPTION, EXPECTED_HEADER)));
    tested.accept(new ClientRequest()
            .setHeaders(MultiMap.caseInsensitiveMultiMap().add(EXPECTED_HEADER, "true")),
        ImmutableList.of(original));

    // then
    assertEquals(copy, original);
  }

  @Test
  @DisplayName("Expect fragment is not modified when supported fragments does not contain fragment type.")
  void expectFragmentNotModifiedWhenOtherSupportedTypeConfigured() {
    // given
    FragmentEvent original = new FragmentEvent(new Fragment("json", new JsonObject(),
        "{ \"body\": \"<div>body</div>\" }"));
    FragmentEvent copy = new FragmentEvent(original.toJson());

    // when
    FragmentEventsConsumer tested = new FragmentHtmlBodyWriterFactory().create(new JsonObject()
        .put(FRAGMENT_TYPES_OPTIONS, new JsonArray().add(EXPECTED_FRAGMENT_TYPE))
        .put(CONDITION_OPTION, new JsonObject().put(HEADER_OPTION, EXPECTED_HEADER)));
    tested.accept(new ClientRequest()
            .setHeaders(MultiMap.caseInsensitiveMultiMap().add(EXPECTED_HEADER, "true")),
        ImmutableList.of(original));

    // then
    assertEquals(copy, original);
  }

  @Test
  @DisplayName("Expect fragment is modified when header condition and supported type configured.")
  void expectFragmentBodyModifiedWhenHeaderConditionConfigured() {
    // given
    FragmentEvent original = new FragmentEvent(
        new Fragment(EXPECTED_FRAGMENT_TYPE, new JsonObject(),
            "{ \"body\": \"<div>body</div>\" }"));
    FragmentEvent copy = new FragmentEvent(original.toJson());

    // when
    FragmentEventsConsumer tested = new FragmentHtmlBodyWriterFactory().create(new JsonObject()
        .put(FRAGMENT_TYPES_OPTIONS, new JsonArray().add(EXPECTED_FRAGMENT_TYPE))
        .put(CONDITION_OPTION, new JsonObject().put(HEADER_OPTION, EXPECTED_HEADER)));
    tested.accept(new ClientRequest()
            .setHeaders(MultiMap.caseInsensitiveMultiMap().add(EXPECTED_HEADER, "true")),
        ImmutableList.of(original));

    // then
    // then
    assertNotEquals(copy, original);
  }

  @Test
  @DisplayName("Expect fragment is modified when param condition and supported type configured.")
  void expectFragmentBodyModifiedWhenParamConditionConfigured() {
    // given
    FragmentEvent original = new FragmentEvent(
        new Fragment(EXPECTED_FRAGMENT_TYPE, new JsonObject(),
            "{ \"body\": \"<div>body</div>\" }"));
    FragmentEvent copy = new FragmentEvent(original.toJson());

    // when
    FragmentEventsConsumer tested = new FragmentHtmlBodyWriterFactory().create(new JsonObject()
        .put(FRAGMENT_TYPES_OPTIONS, new JsonArray().add(EXPECTED_FRAGMENT_TYPE))
        .put(CONDITION_OPTION, new JsonObject().put(PARAM_OPTION, EXPECTED_PARAM)));
    tested.accept(new ClientRequest()
            .setParams(MultiMap.caseInsensitiveMultiMap().add(EXPECTED_PARAM, "true")),
        ImmutableList.of(original));

    // then
    // then
    assertNotEquals(copy, original);
  }

  @Test
  @DisplayName("Expect fragment body is wrapped by fragmentId.")
  void expectFragmentBodyWrappedByFragmentId() {
    // given
    String body = "<div>body</div>";
    FragmentEvent event = new FragmentEvent(new Fragment("snippet", new JsonObject(), body));

    // when
    FragmentEventsConsumer tested = new FragmentHtmlBodyWriterFactory().create(new JsonObject()
        .put(FRAGMENT_TYPES_OPTIONS, new JsonArray().add(EXPECTED_FRAGMENT_TYPE))
        .put(CONDITION_OPTION, new JsonObject().put(PARAM_OPTION, EXPECTED_PARAM)));
    tested.accept(new ClientRequest()
            .setParams(MultiMap.caseInsensitiveMultiMap().add(EXPECTED_PARAM, "true")),
        ImmutableList.of(event));

    // then
    assertTrue(event.getFragment().getBody()
        .startsWith("<!-- data-knotx-id=\"" + event.getFragment().getId() + "\" -->"));
    assertTrue(event.getFragment().getBody()
        .endsWith("<!-- data-knotx-id=\"" + event.getFragment().getId() + "\" -->"));
  }

  @Test
  @DisplayName("Expect fragment body contains debug script when fragment type configured.")
  void expectFragmentBodyContainsDebugScript() {
    //given
    String body = "<div>body</div>";
    FragmentEvent event = new FragmentEvent(new Fragment("snippet", new JsonObject(), body));
    JsonObject eventData = event.toJson();

    String scriptRegexp = "<script data-knotx-id=\"" + event.getFragment().getId()
        + "\" type=\"application/json\">(?<fragmentEventJson>.*?)</script>";
    Pattern scriptPattern = Pattern.compile(scriptRegexp, Pattern.DOTALL);

    // when
    FragmentEventsConsumer tested = new FragmentHtmlBodyWriterFactory().create(new JsonObject()
        .put(FRAGMENT_TYPES_OPTIONS, new JsonArray().add(EXPECTED_FRAGMENT_TYPE))
        .put(CONDITION_OPTION, new JsonObject().put(PARAM_OPTION, EXPECTED_PARAM)));
    tested.accept(new ClientRequest()
            .setParams(MultiMap.caseInsensitiveMultiMap().add(EXPECTED_PARAM, "true")),
        ImmutableList.of(event));

    // then
    Matcher matcher = scriptPattern.matcher(event.getFragment().getBody());
    assertTrue(matcher.find());
    assertEquals(eventData, new JsonObject(matcher.group("fragmentEventJson")));
  }

  @Test
  @DisplayName("Expect debug script is first HTML tag.")
  void expectDebugScriptAfterComment() {
    //given
    String body = "<div>body</div>";
    FragmentEvent event = new FragmentEvent(new Fragment("snippet", new JsonObject(), body));

    // when
    FragmentEventsConsumer tested = new FragmentHtmlBodyWriterFactory().create(new JsonObject()
        .put(FRAGMENT_TYPES_OPTIONS, new JsonArray().add(EXPECTED_FRAGMENT_TYPE))
        .put(CONDITION_OPTION, new JsonObject().put(PARAM_OPTION, EXPECTED_PARAM)));
    tested.accept(new ClientRequest()
            .setParams(MultiMap.caseInsensitiveMultiMap().add(EXPECTED_PARAM, "true")),
        ImmutableList.of(event));

    // then
    String bodyWithoutComments = event.getFragment().getBody()
        .replaceAll("<!-- data-knotx-id=\"" + event.getFragment().getId() + "\" -->", "");
    assertTrue(
        bodyWithoutComments.startsWith("<script data-knotx-id=\"" + event.getFragment().getId()
            + "\" type=\"application/json\">"));
  }

}