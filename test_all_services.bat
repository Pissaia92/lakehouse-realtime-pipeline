@echo off
title Test All Services

echo [1/9] Testing MinIO...
curl -s http://localhost:9000 > nul 2>&1 && echo ✅ MinIO is UP || echo ❌ MinIO is DOWN

echo [2/9] Testing Kafka...
curl -s http://localhost:9092 > nul 2>&1 && echo ✅ Kafka is UP || echo ❌ Kafka is DOWN

echo [3/9] Testing Flink...
curl -s http://localhost:8081 > nul 2>&1 && echo ✅ Flink is UP || echo ❌ Flink is DOWN

echo [4/9] Testing Prometheus...
curl -s http://localhost:9090 > nul 2>&1 && echo ✅ Prometheus is UP || echo ❌ Prometheus is DOWN

echo [5/9] Testing Grafana...
curl -s http://localhost:3000 > nul 2>&1 && echo ✅ Grafana is UP || echo ❌ Grafana is DOWN

echo [6/9] Testing Loki...
curl -s http://localhost:3100 > nul 2>&1 && echo ✅ Loki is UP || echo ❌ Loki is DOWN

echo [7/9] Testing Jaeger...
curl -s http://localhost:16686 > nul 2>&1 && echo ✅ Jaeger is UP || echo ❌ Jaeger is DOWN

echo [8/9] Testing Redis...
curl -s http://localhost:6379 > nul 2>&1 && echo ✅ Redis is UP || echo ❌ Redis is DOWN

echo [9/9] Testing Next.js Dashboard...
curl -s http://localhost:3000 > nul 2>&1 && echo ✅ Next.js is UP || echo ❌ Next.js is DOWN

echo.
echo ✅ Test completed!
pause