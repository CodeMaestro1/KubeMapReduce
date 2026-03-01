# KubeMapReduce

## Prerequisites
- Go 1.25.0+ installed
- Docker & Docker Compose installed

## Setup & Running

1. **Start Keycloak (Authentication Server):**
   The Keycloak configuration is located in `docker/.env`.
   ```bash
   cd docker
   docker-compose up -d
   ```
   *Note: Keycloak will run on `http://localhost:8080`. You will need to log in to the admin console (credentials are in `docker/.env`, default: admin/admin), create a realm named `mapreduce`, and configure a client named `mapreduce-api`.*

2. **Run the API:**
   From the root of the project:
   ```bash
   go run ./cmd/api
   ```
   The API will start on port `8081`.