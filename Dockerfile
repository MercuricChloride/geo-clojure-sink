FROM clojure

# Create app directory in container
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

RUN apt-get update && \
    apt-get install -y curl jq && \
    # Clean up the cache to reduce layer size
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the project file and fetch dependencies
COPY project.clj /usr/src/app/
RUN lein deps

# Copy the entire project
COPY . /usr/src/app

# Build the uberjar
RUN lein uberjar

# Move the standalone jar to a known location
RUN mv "$(lein uberjar | sed -n 's/^Created \(.*standalone\.jar\)/\1/p')" app-standalone.jar

# Make the entrypoint script executable
RUN chmod +x /usr/src/app/entrypoint.sh

# Set the entrypoint script
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]

# Default command
CMD ["java", "-jar", "app-standalone.jar", "--from-cache"]
