echo "Creating keyspace 'myks' and column family 'mycf'"
curl -vX POST http://localhost:8080/intravert/intrareq-json -d "{\"e\":[ {\"type\":\"CREATEKEYSPACE\",\"op\":{\"name\":\"myks\",\"replication\":1}}, {\"type\":\"SETKEYSPACE\",\"op\":{\"keyspace\":\"myks\"}}, {\"type\":\"CREATECOLUMNFAMILY\",\"op\":{\"name\":\"mycf\"}} ]}"
echo
