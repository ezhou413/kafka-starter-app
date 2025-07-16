#!/bin/bash

# Build the project
echo "Building the project..."
./gradlew clean build

# Check if oauth-config.properties exists
if [ ! -f "oauth-config.properties" ]; then
    echo "Error: oauth-config.properties file not found!"
    echo "Please create the oauth-config.properties file with your OAuth credentials."
    exit 1
fi

echo ""
echo "OAuth Kafka Examples"
echo "===================="
echo ""
echo "To run the OAuth Producer Example:"
echo "  java -cp build/libs/kafka-starter-app-0.0.1.jar io.confluent.developer.OAuthProducerExample oauth-config.properties"
echo ""
echo "To run the OAuth Consumer Example:"
echo "  java -cp build/libs/kafka-starter-app-0.0.1.jar io.confluent.developer.OAuthConsumerExample oauth-config.properties"
echo ""
echo "Note: Make sure to update oauth-config.properties with your actual OAuth credentials before running."
echo ""
echo "To get OAuth credentials for Confluent Cloud:"
echo "1. Go to your Confluent Cloud console"
echo "2. Navigate to API Keys section"
echo "3. Create a new API key with OAuth authentication"
echo "4. Update the oauth-config.properties file with your credentials" 