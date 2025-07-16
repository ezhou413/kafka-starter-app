# OAuth Authentication for Kafka Java Client

This project now supports OAuth authentication for connecting to Confluent Cloud. The implementation includes a custom OAuth token provider and examples for both producers and consumers.

## Files Added/Modified

### New Files:
- `src/main/java/io/confluent/developer/OAuthTokenProvider.java` - OAuth token management
- `src/main/java/io/confluent/developer/CustomOAuthLoginModule.java` - Custom JAAS login module
- `src/main/java/io/confluent/developer/OAuthProducerExample.java` - OAuth-enabled producer
- `src/main/java/io/confluent/developer/OAuthConsumerExample.java` - OAuth-enabled consumer
- `oauth-config.properties` - OAuth configuration file
- `run-oauth-examples.sh` - Helper script to run examples
- `OAUTH_README.md` - This documentation

## Setup Instructions

### 1. Get OAuth Credentials from Confluent Cloud

1. Log into your Confluent Cloud console
2. Navigate to **API Keys** in the left sidebar
3. Click **Create API Key**
4. Select **OAuth** as the authentication method
5. Note down your:
   - Client ID
   - Client Secret
   - Token Endpoint (usually `https://login.confluent.io/oauth/token`)

### 2. Update Configuration

Edit the `oauth-config.properties` file and replace the placeholder values:

```properties
# Replace these with your actual OAuth credentials
oauth.client.id=your_actual_client_id_here
oauth.client.secret=your_actual_client_secret_here
oauth.token.endpoint=https://login.confluent.io/oauth/token
```

### 3. Build the Project

```bash
./gradlew clean build
```

### 4. Run the Examples

#### OAuth Producer Example:
```bash
java -cp build/libs/kafka-starter-app-0.0.1.jar io.confluent.developer.OAuthProducerExample oauth-config.properties
```

#### OAuth Consumer Example:
```bash
java -cp build/libs/kafka-starter-app-0.0.1.jar io.confluent.developer.OAuthConsumerExample oauth-config.properties
```

Or use the helper script:
```bash
./run-oauth-examples.sh
```

## How It Works

### OAuth Token Provider (`OAuthTokenProvider.java`)
- Manages OAuth token acquisition and refresh
- Implements client credentials flow
- Handles token caching and expiration
- Uses HTTP client to request tokens from Confluent's OAuth endpoint

### Custom Login Module (`CustomOAuthLoginModule.java`)
- Extends Kafka's OAuthBearerRefreshingLogin
- Integrates with our token provider
- Handles JAAS configuration parsing
- Provides tokens to Kafka client

### Configuration Flow
1. Application reads OAuth credentials from `oauth-config.properties`
2. Creates `OAuthTokenProvider` instance
3. Sets up JAAS configuration with custom login module
4. Kafka client uses OAuth bearer tokens for authentication

## Security Features

- **Token Caching**: Tokens are cached and reused until near expiration
- **Automatic Refresh**: Tokens are automatically refreshed before expiration
- **Secure Storage**: Credentials are stored in configuration files (consider using environment variables for production)
- **SSL/TLS**: All connections use SASL_SSL for encryption

## Troubleshooting

### Common Issues:

1. **"OAuth credentials not found"**
   - Check that `oauth-config.properties` exists and contains the required fields
   - Verify that client ID, secret, and token endpoint are properly set

2. **"Failed to get OAuth token"**
   - Verify your OAuth credentials are correct
   - Check that your Confluent Cloud cluster supports OAuth
   - Ensure network connectivity to the token endpoint

3. **"Authentication failed"**
   - Verify your cluster endpoint is correct
   - Check that your OAuth credentials have the necessary permissions
   - Ensure the topic exists and is accessible

### Debug Mode:
To enable debug logging, add this to your JVM arguments:
```bash
-Djava.security.debug=logincontext
```

## Production Considerations

1. **Credential Management**: Use environment variables or a secure credential store instead of plain text files
2. **Token Refresh**: The implementation handles automatic token refresh, but monitor for any issues
3. **Error Handling**: Add proper error handling and retry logic for production use
4. **Monitoring**: Implement logging and monitoring for OAuth token acquisition and usage

## Dependencies

The project already includes the necessary dependencies:
- `jackson-databind` for JSON parsing
- `kafka-clients` for Kafka client functionality
- Java 11+ for HTTP client support

## Example Output

When running the producer:
```
Starting to produce messages with OAuth authentication...
Produced event to topic evan_oauth_topic: key = book       value = evan
Produced event to topic evan_oauth_topic: key = batteries  value = jsmith
...
```

When running the consumer:
```
Starting to consume messages with OAuth authentication from topic: evan_oauth_topic
Consumed event from topic evan_oauth_topic: key = book       value = evan
Consumed event from topic evan_oauth_topic: key = batteries  value = jsmith
...
``` 