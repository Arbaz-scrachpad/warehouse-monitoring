# Warehouse Monitoring System

## Prerequisites

Make sure the following tools are installed:

- Java (the version required by the project)
- Maven
- Docker Desktop with Docker Compose
- PowerShell (for the UDP test commands below)

## 1. Start the Infrastructure

Start the Docker services:

```bash
docker compose up -d
```

Check the running containers:

```bash
docker compose ps
```

Check Kafka logs:

```bash
docker logs warehouse-kafka --tail 50
```

## 2. Create the Kafka Topic

Create the `sensor-measurements` topic:

```bash
docker exec warehouse-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic sensor-measurements \
  --partitions 3 \
  --replication-factor 1
```

Verify that the topic exists:

```bash
docker exec warehouse-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

If the topic already exists, Kafka may report that it already exists; in that case, verify it using the list command.

## 3. Build the Project

From the project root directory, run:

```bash
mvn clean verify
```

Ensure the build and tests complete successfully before proceeding.

## 4. Start the Central Monitoring Service

Open a terminal in the project root and run:

```bash
mvn -pl central-monitoring spring-boot:run
```

Keep this terminal open while testing the application.

## 5. Start the Warehouse Agent

Open another terminal in the project root and build the agent module:

```bash
mvn -pl warehouse-agent -am clean package
```

Start the agent:

```bash
mvn -pl warehouse-agent exec:java "-Dexec.mainClass=com.example.warehouse.agent.WarehouseAgentMain"
```

Keep this terminal open while sending sensor measurements.

## 6. Send Test Sensor Measurements

Open a PowerShell terminal and run the following commands individually to send sample UDP measurements.

**Temperature sensor**

```powershell
$udp = New-Object System.Net.Sockets.UdpClient
$bytes = [System.Text.Encoding]::UTF8.GetBytes("sensor_id=t1; value=36")
$udp.Send($bytes, $bytes.Length, "127.0.0.1", 3344)
$udp.Close()
```

**Humidity sensor 1**

```powershell
$udp = New-Object System.Net.Sockets.UdpClient
$bytes = [System.Text.Encoding]::UTF8.GetBytes("sensor_id=h1; value=50")
$udp.Send($bytes, $bytes.Length, "127.0.0.1", 3355)
$udp.Close()
```

**Humidity sensor 2**

```powershell
$udp = New-Object System.Net.Sockets.UdpClient
$bytes = [System.Text.Encoding]::UTF8.GetBytes("sensor_id=h2; value=52")
$udp.Send($bytes, $bytes.Length, "127.0.0.1", 3355)
$udp.Close()
```

Confirm that the ports match the configuration in the warehouse agent.

## 7. Verify Kafka Messages

Run the following command to consume up to five messages from the Kafka topic:

```bash
docker exec warehouse-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic sensor-measurements --from-beginning --max-messages 5
```

Confirm that the sensor measurements appear in the output.

## 8. Troubleshooting

- **Kafka is unavailable:** Check `docker compose ps` and inspect the Kafka container logs.
- **Topic creation fails:** Verify that Kafka is running and the bootstrap server is reachable.
- **The agent does not start:** Check the Java version, Maven build output, and application configuration.
- **No sensor measurements appear:** Verify the UDP ports, sensor message format, and that the agent and monitoring service are running.

## Notes

- Run all Maven commands from the project root directory.
- Keep Docker, the central monitoring service, and the warehouse agent running during the test.
- The commands above assume the Kafka container is named `warehouse-kafka` and that the services use the stated ports.
