#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Book Search Application Startup${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""


# Load environment variables. Docker Compose and the importer both need them.
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo -e "${RED}Error: .env not found. Create it with: cp .env.example .env${NC}"
  exit 1
fi

# Step 1: Check if Docker is running
echo -e "${YELLOW}[1/5] Checking if Docker is running...${NC}"
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running. Please start Docker Desktop and try again.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker is running${NC}"
echo ""

# Step 2: Check if port 8080 is in use
echo -e "${YELLOW}[2/5] Checking if port 8080 is available...${NC}"
if lsof -Pi :8080 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo -e "${YELLOW}Warning: Port 8080 is already in use.${NC}"
    read -p "Do you want to kill the process using port 8080? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        lsof -ti :8080 | xargs kill -9 2>/dev/null
        echo -e "${GREEN}✓ Port 8080 freed${NC}"
    else
        echo -e "${RED}Cannot start application. Port 8080 is in use.${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}✓ Port 8080 is available${NC}"
fi
echo ""

# Step 3: Start PostgreSQL database
echo -e "${YELLOW}[3/5] Starting PostgreSQL database...${NC}"
docker compose up -d
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Database container started${NC}"
else
    echo -e "${RED}Error: Failed to start database${NC}"
    exit 1
fi

# Wait until PostgreSQL accepts connections. On the first start it also runs create_schema.sql,
# which can take well over a few seconds. Checking over TCP (-h 127.0.0.1) matters: while the init
# scripts run, the container's temporary server only listens on a Unix socket, so a socket check
# would report "ready" too early.
echo -e "${YELLOW}Waiting for the database to accept connections...${NC}"
for i in $(seq 1 60); do
    if docker compose exec -T db pg_isready -q -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"; then
        echo -e "${GREEN}✓ Database is ready${NC}"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo -e "${RED}Error: Database was not ready after 60 seconds. Check: docker compose logs db${NC}"
        exit 1
    fi
    sleep 1
done

echo ""

# Step 4: Import Data (if needed)
echo -e "${YELLOW}[4/5] Importing/Verifying Data...${NC}"
mvn clean compile exec:java -Dexec.mainClass="com.h2.DBImporter"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Data check/import completed${NC}"
else
    echo -e "${RED}Warning: Data import encountered issues.${NC}"
fi
echo ""

# Step 5: Run Spring Boot application
echo -e "${YELLOW}[5/5] Starting Spring Boot application...${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
mvn spring-boot:run
