package io.confluent.developer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerRefreshingLogin;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslClient;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslClientCallbackHandler;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

public class OAuthTokenProvider {
    private final String clientId;
    private final String clientSecret;
    private final String tokenEndpoint;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    
    private String accessToken;
    private Instant tokenExpiry;

    public OAuthTokenProvider(String clientId, String clientSecret, String tokenEndpoint) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.tokenEndpoint = tokenEndpoint;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.objectMapper = new ObjectMapper();
    }

    public String getAccessToken() throws IOException, InterruptedException {
        // Check if we have a valid token
        if (accessToken != null && tokenExpiry != null && Instant.now().isBefore(tokenExpiry)) {
            return accessToken;
        }

        // Request new token
        String requestBody = String.format(
                "grant_type=client_credentials&client_id=%s&client_secret=%s",
                clientId, clientSecret
        );

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenEndpoint))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("Failed to get OAuth token. Status: " + response.statusCode() + ", Body: " + response.body());
        }

        JsonNode jsonResponse = objectMapper.readTree(response.body());
        this.accessToken = jsonResponse.get("access_token").asText();
        
        // Set expiry time (subtract 60 seconds as buffer)
        int expiresIn = jsonResponse.get("expires_in").asInt();
        this.tokenExpiry = Instant.now().plusSeconds(expiresIn - 60);

        return accessToken;
    }

    // This class is no longer needed as we're using CustomOAuthLoginModule instead
} 