#!/bin/bash

ADMIN_USER="admin"
ADMIN_PWD="admin"
ROOT_USER="root"
ROOT_PWD="root"
TARGET_DB="rate-db"

# Function to check MongoDB readiness with timeout
wait_for_mongo() {
    local timeout=60
    while ! mongo --eval "db.runCommand({ ping: 1 })" > /dev/null 2>&1 && [ $timeout -gt 0 ]; do
        sleep 1
        timeout=$((timeout - 1))
    done
    if [ $timeout -eq 0 ]; then
        echo "Error: MongoDB did not become ready in time." >&2
        exit 1
    fi
}

# Wait for MongoDB to start
echo "Waiting for MongoDB to start..."
wait_for_mongo

# Create admin user
echo "Creating admin user..."
mongo admin --eval "
    if (!db.getUser('$ADMIN_USER')) {
        db.createUser({
            user: '$ADMIN_USER',
            pwd: '$ADMIN_PWD',
            roles: [
                { role: 'userAdminAnyDatabase', db: 'admin' },
                { role: 'readWrite', db: '$TARGET_DB' }
            ]
        });
        print('Admin user created successfully.');
    } else {
        print('Admin user already exists.');
    }
"

# Create root user
echo "Creating root user..."
mongo admin --eval "
    if (!db.getUser('$ROOT_USER')) {
        db.createUser({
            user: '$ROOT_USER',
            pwd: '$ROOT_PWD',
            roles: [
                { role: 'userAdminAnyDatabase', db: 'admin' }
            ]
        });
        print('Root user created successfully.');
    } else {
        print('Root user already exists.');
    }
"

# Ensure rate-db exists
echo "Ensuring database $TARGET_DB exists..."
mongo $TARGET_DB --eval "db.createCollection('init'); db.dropCollection('init');"

echo "Initialization script completed successfully."
