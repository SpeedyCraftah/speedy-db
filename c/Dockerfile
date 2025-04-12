FROM alpine:latest

# Install OpenSSL and glibc compatibility.
RUN apk add --no-cache openssl
RUN apk add --no-cache gcompat
RUN apk add --no-cache libstdc++

# Create a rootless user.
RUN adduser -u 1000 -H -D dbuser

# Define the work directory.
WORKDIR /app

# Copy over the DB executable.
COPY ./bin/production ./production
RUN chmod u=rx,g=rx,o=--- ./production

# Change permissions for the work directory to allow the non-root user.
RUN chown -R dbuser:dbuser /app

# Drop down to the non-root user.
USER dbuser

# Run the app.
EXPOSE 4546
ENTRYPOINT ["/app/production"]
CMD ["data-directory=/data"]

# For debugging :).
# ENTRYPOINT ["sh", "-c", "while true; do sleep 1000d; done"]