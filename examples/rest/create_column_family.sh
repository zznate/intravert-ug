echo "Creating a new column family [myks/mycf]:"
curl -X POST http://localhost:8080/intravert/intrareq-rest/myks/mycf
echo
echo "Listing keyspaces to see if it is there:"
curl -X GET http://localhost:8080/intravert/intrareq-rest/myks/
echo
