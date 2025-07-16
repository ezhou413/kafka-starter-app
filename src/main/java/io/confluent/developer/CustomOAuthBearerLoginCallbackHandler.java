package io.confluent.developer;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class CustomOAuthBearerLoginCallbackHandler implements CallbackHandler {
    private OAuthTokenProvider tokenProvider;

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                if (tokenProvider == null) {
                    // In a real implementation, you would get these from JAAS config or env
                    String clientId = System.getProperty("oauth.client.id", "your_oauth_client_id_here");
                    String clientSecret = System.getProperty("oauth.client.secret", "your_oauth_client_secret_here");
                    String tokenEndpoint = System.getProperty("oauth.token.endpoint", "https://login.confluent.io/oauth/token");
                    tokenProvider = new OAuthTokenProvider(clientId, clientSecret, tokenEndpoint);
                }
                try {
                    String accessToken = tokenProvider.getAccessToken();
                    ((OAuthBearerTokenCallback) callback).token(new SimpleBearerToken(accessToken));
                } catch (Exception e) {
                    throw new IOException("Failed to get OAuth token", e);
                }
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    // Minimal implementation of OAuthBearerToken
    static class SimpleBearerToken implements OAuthBearerToken {
        private final String token;
        private final long lifetimeMs;
        private final Long startTimeMs;
        private final String principalName;
        private final Set<String> scope;

        public SimpleBearerToken(String token) {
            this.token = token;
            this.lifetimeMs = System.currentTimeMillis() + 3600_000; // 1 hour
            this.startTimeMs = System.currentTimeMillis();
            this.principalName = "oauth-user";
            this.scope = new HashSet<>();
        }

        @Override
        public String value() {
            return token;
        }

        @Override
        public Set<String> scope() {
            return Collections.unmodifiableSet(scope);
        }

        @Override
        public long lifetimeMs() {
            return lifetimeMs;
        }

        @Override
        public String principalName() {
            return principalName;
        }

        @Override
        public Long startTimeMs() {
            return startTimeMs;
        }
    }
} 