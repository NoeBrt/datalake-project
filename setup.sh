#!/bin/bash

# Run Airflow initialization
echo -e "\033[1;34m[INFO]\033[0m Running Airflow initialization..."
docker compose run airflow-init
if [ $? -eq 0 ]; then
    echo -e "\033[1;32m[SUCCESS]\033[0m Airflow initialization completed successfully."
else
    echo -e "\033[1;31m[ERROR]\033[0m Airflow initialization failed."
    exit 1
fi

# Define the script paths (relative)
SCRIPT_DIR="$(dirname "$0")/setup"
MYSQL_SCRIPT="$SCRIPT_DIR/setup_mysql.py"
AWS_SCRIPT="$SCRIPT_DIR/setup_aws.sh"

# Function to log messages with colors
echo_info() {
    echo -e "\033[1;34m[INFO]\033[0m $1"
}

echo_success() {
    echo -e "\033[1;32m[SUCCESS]\033[0m $1"
}

echo_error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1"
}

# Run MySQL setup
if [ -f "$MYSQL_SCRIPT" ]; then
    echo_info "Running MySQL setup script..."
    python3 "$MYSQL_SCRIPT"
    if [ $? -eq 0 ]; then
        echo_success "MySQL setup completed successfully."
    else
        echo_error "MySQL setup failed."
        exit 1
    fi
else
    echo_error "MySQL setup script not found: $MYSQL_SCRIPT"
    exit 1
fi

# Run AWS setup
if [ -f "$AWS_SCRIPT" ]; then
    echo_info "Running AWS setup script..."
    bash "$AWS_SCRIPT"
    if [ $? -eq 0 ]; then
        echo_success "AWS setup completed successfully."
    else
        echo_error "AWS setup failed."
        exit 1
    fi
else
    echo_error "AWS setup script not found: $AWS_SCRIPT"
    exit 1
fi

echo_success "All setup scripts executed successfully."

# Start Docker Compose services
echo -e "\033[1;34m[INFO]\033[0m Starting Docker Compose services..."
docker compose up -d
if [ $? -eq 0 ]; then
    echo -e "\033[1;32m[SUCCESS]\033[0m Docker Compose services started successfully."
else
    echo -e "\033[1;31m[ERROR]\033[0m Failed to start Docker Compose services."
    exit 1
fi