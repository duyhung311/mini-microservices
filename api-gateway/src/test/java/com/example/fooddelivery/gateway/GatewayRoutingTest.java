package com.example.fooddelivery.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GatewayRoutingTest {

  private static final HttpServer ORDER = stub("order-service", "order-1");
  private static final HttpServer RESTAURANT_A = stub("restaurant-service", "rest-A");
  private static final HttpServer RESTAURANT_B = stub("restaurant-service", "rest-B");
  private static final HttpServer DELIVERY = stub("delivery-service", "delivery-1");

  private static HttpServer stub(String name, String instanceId) {
    try {
      HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      s.createContext(
          "/",
          ex -> {
            byte[] in;
            try (InputStream is = ex.getRequestBody()) {
              in = is.readAllBytes();
            }
            String body =
                "service=" + name
                    + ";method=" + ex.getRequestMethod()
                    + ";path=" + ex.getRequestURI().getRawPath()
                    + ";body=" + new String(in, StandardCharsets.UTF_8);
            byte[] out = body.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "text/plain");
            ex.getResponseHeaders().add("X-Instance-Id", instanceId);
            ex.sendResponseHeaders(200, out.length);
            ex.getResponseBody().write(out);
            ex.close();
          });
      s.start();
      return s;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String uri(HttpServer s) {
    return "http://127.0.0.1:" + s.getAddress().getPort();
  }

  @DynamicPropertySource
  static void props(DynamicPropertyRegistry r) {
    String p = "spring.cloud.discovery.client.simple.instances.";
    r.add(p + "order-service[0].uri", () -> uri(ORDER));
    r.add(p + "restaurant-service[0].uri", () -> uri(RESTAURANT_A));
    r.add(p + "restaurant-service[1].uri", () -> uri(RESTAURANT_B));
    r.add(p + "delivery-service[0].uri", () -> uri(DELIVERY));
  }

  @AfterAll
  static void stop() {
    ORDER.stop(0);
    RESTAURANT_A.stop(0);
    RESTAURANT_B.stop(0);
    DELIVERY.stop(0);
  }

  @Autowired WebTestClient client;

  private void assertRouted(String path, String service) {
    EntityExchangeResult<String> res =
        client.get().uri(path).exchange().expectStatus().isOk()
            .expectBody(String.class).returnResult();
    assertThat(res.getResponseBody())
        .contains("service=" + service)
        .contains("method=GET")
        .contains("path=" + path + ";");
  }

  @Test
  void ordersRoutesToOrderService() {
    assertRouted("/orders/x", "order-service");
  }

  @Test
  void restaurantsRoutesToRestaurantService() {
    assertRouted("/restaurants", "restaurant-service");
  }

  @Test
  void ticketsRoutesToRestaurantService() {
    assertRouted("/tickets/x", "restaurant-service");
  }

  @Test
  void driversRoutesToDeliveryService() {
    assertRouted("/drivers", "delivery-service");
  }

  @Test
  void deliveriesRoutesToDeliveryService() {
    assertRouted("/deliveries/x", "delivery-service");
  }

  @Test
  void postMethodAndBodyPassThrough() {
    String body =
        client.post().uri("/orders").bodyValue("{\"item\":\"pizza\"}")
            .exchange().expectStatus().isOk()
            .expectBody(String.class).returnResult().getResponseBody();
    assertThat(body)
        .contains("service=order-service")
        .contains("method=POST")
        .contains("path=/orders;")
        .contains("body={\"item\":\"pizza\"}");
  }

  @Test
  void backendInstanceIdHeaderReachesClient() {
    client.get().uri("/orders/x").exchange().expectStatus().isOk()
        .expectHeader().valueEquals("X-Instance-Id", "order-1");
    client.get().uri("/deliveries/x").exchange().expectStatus().isOk()
        .expectHeader().valueEquals("X-Instance-Id", "delivery-1");
  }

  @Test
  void unknownPathReturns404FromGateway() {
    EntityExchangeResult<byte[]> res =
        client.get().uri("/nope").exchange().expectStatus().isNotFound()
            .expectBody().returnResult();
    assertThat(res.getResponseHeaders().containsKey("X-Instance-Id")).isFalse();
  }

  @Test
  void restaurantCallsAreLoadBalancedAcrossInstances() {
    Set<String> ids = new HashSet<>();
    for (int i = 0; i < 12; i++) {
      EntityExchangeResult<byte[]> res =
          client.get().uri("/restaurants").exchange().expectStatus().isOk()
              .expectBody().returnResult();
      ids.add(res.getResponseHeaders().getFirst("X-Instance-Id"));
    }
    assertThat(ids).containsExactlyInAnyOrder("rest-A", "rest-B");
  }
}
