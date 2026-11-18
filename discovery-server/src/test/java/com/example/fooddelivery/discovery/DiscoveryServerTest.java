package com.example.fooddelivery.discovery;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DiscoveryServerTest {

  @Value("${local.server.port}")
  int port;

  private final HttpClient http = HttpClient.newHttpClient();

  private HttpResponse<String> send(String method, String path, String body) throws Exception {
    HttpRequest.Builder b =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
            .header("Accept", "application/json");
    if (body != null) {
      b.header("Content-Type", "application/json");
      b.method(method, HttpRequest.BodyPublishers.ofString(body));
    } else {
      b.method(method, HttpRequest.BodyPublishers.noBody());
    }
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  @Test
  void contextLoadsOnRandomPort() {
    assertThat(port).isPositive();
  }

  @Test
  void eurekaAppsReturnsJsonWithApplicationsObject() throws Exception {
    HttpResponse<String> r = send("GET", "/eureka/apps", null);
    assertThat(r.statusCode()).isEqualTo(200);
    assertThat(r.headers().firstValue("Content-Type").orElse("")).contains("json");
    assertThat(r.body()).contains("\"applications\"");
  }

  @Test
  void instanceCanBeRegisteredListedAndDeregistered() throws Exception {
    String instance =
        "{\"instance\":{\"instanceId\":\"fake-1\",\"hostName\":\"localhost\","
            + "\"app\":\"TEST-APP\",\"ipAddr\":\"127.0.0.1\",\"status\":\"UP\","
            + "\"port\":{\"$\":9999,\"@enabled\":\"true\"},"
            + "\"dataCenterInfo\":{\"@class\":\"com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo\","
            + "\"name\":\"MyOwn\"}}}";

    assertThat(send("POST", "/eureka/apps/TEST-APP", instance).statusCode()).isEqualTo(204);

    HttpResponse<String> listed = send("GET", "/eureka/apps", null);
    assertThat(listed.statusCode()).isEqualTo(200);
    assertThat(listed.body()).contains("TEST-APP").contains("fake-1");

    assertThat(send("DELETE", "/eureka/apps/TEST-APP/fake-1", null).statusCode()).isEqualTo(200);

    HttpResponse<String> after = send("GET", "/eureka/apps", null);
    assertThat(after.body()).doesNotContain("fake-1");
  }
}
