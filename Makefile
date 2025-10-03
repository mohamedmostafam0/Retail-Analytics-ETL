# Makefile for managing the Dockerized Airflow environment

# Build and start all services in the background
up:
	@echo "Starting all services..."
	docker compose up -d

build:
	@echo "Starting all services..."
	docker compose up -d --build

# Stop and remove all services
down:
	@echo "Stopping all services..."
	docker compose down

# Restart all services
restart: down up

# Get a shell into the webserver container
sh:
	@echo "Opening a shell into the webserver container..."
	docker exec -it webserver bash

# Run pytest for the Airflow tests
pytest:
	@echo "Running pytest..."
	docker exec webserver pytest -p no:warnings -v /opt/airflow/tests

# Format code with black
format:
	@echo "Formatting code with black..."
	docker exec webserver python -m black -S --line-length 79 .

# Sort imports with isort
isort:
	@echo "Sorting imports with isort..."
	docker exec webserver isort .

# Run mypy for type checking
type:
	@echo "Running mypy for type checking..."
	docker exec webserver mypy --ignore-missing-imports /opt/airflow

# Run flake8 for linting
lint:
	@echo "Running flake8 for linting..."
	docker exec webserver flake8 /opt/airflow/dags

# Run all CI checks
ci: isort format type lint pytest

.PHONY: up down restart sh pytest format isort type lint ci