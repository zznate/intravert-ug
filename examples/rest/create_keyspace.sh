echo "Creating a new keyspace [myks]:"
curl -X POST http://localhost:8080/intravert/intrareq-rest/myks/
echo
echo "Listing keyspaces to see if it is there:"
curl -X GET http://localhost:8080/intravert/intrareq-rest/
echo
