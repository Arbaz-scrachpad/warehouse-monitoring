step to run docker:
1. docker compose up -d
2. docker compose ps
   docker logs warehouse-kafka --tail 50
3. create topics
   - docker exec warehouse-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic sensor-measurements --partitions 3 --replication-factor 1
4. verify
   - docker exec warehouse-kafka kafka-topics --bootstrap-server localhost:9092 --list
5. start central monitoring
   - mvn clean verify
   - mvn -pl central-monitoring spring-boot:run
7. start warehouse agent
   - mvn -pl warehouse-agent -am clean package
   - mvn -pl warehouse-agent exec:java "-Dexec.mainClass=com.example.warehouse.agent.WarehouseAgentMain"
8. test
$udp = New-Object System.Net.Sockets.UdpClient
$bytes = [System.Text.Encoding]::UTF8.GetBytes("sensor_id=t1; value=36")
$udp.Send($bytes, $bytes.Length, "127.0.0.1", 3344)
$udp.Close()

$udp = New-Object System.Net.Sockets.UdpClient
$bytes = [System.Text.Encoding]::UTF8.GetBytes("sensor_id=h1; value=51")
$udp.Send($bytes, $bytes.Length, "127.0.0.1", 3355)
$udp.Close()

$udp = New-Object System.Net.Sockets.UdpClient
$bytes = [System.Text.Encoding]::UTF8.GetBytes("sensor_id=h2; value=52")
$udp.Send($bytes, $bytes.Length, "127.0.0.1", 3355)
$udp.Close()

docker exec warehouse-kafka kafka-console-consumer `
  --bootstrap-server localhost:9092 `
--topic sensor-measurements `
  --from-beginning `
--max-messages 5