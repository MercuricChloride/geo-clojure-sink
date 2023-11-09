#!/bin/bash

# Load environment variables from .env file if it exists (Local Development)
if [[ -f .env ]]; then
    export $(cat .env | sed 's/#.*//g' | xargs)
fi

# Check for necessary API key and set up the SUBSTREAMS_API_TOKEN
if [[ -z "$PINAX_API_KEY"  ]]; then
    echo "Error: PINAX_API_KEY must be provided"
    exit 1
fi

# Assuming this is the command to get the token, uncomment and use it if necessary
export SUBSTREAMS_API_TOKEN=$(curl https://auth.pinax.network/v1/auth/issue -s --data-binary '{"api_key":"'$PINAX_API_KEY'"}' | jq -r .token) 
if [[ -z "$SUBSTREAMS_API_TOKEN"  ]]; then
    echo "Error: SUBSTREAMS_API_TOKEN failed to be set"
    exit 1
fi
echo $SUBSTREAMS_API_TOKEN set on SUBSTREAMS_API_TOKEN

# Run your main application
# The "$@" will pass all the command line arguments received by the script to the Java application
java -jar app-standalone.jar "$@"
