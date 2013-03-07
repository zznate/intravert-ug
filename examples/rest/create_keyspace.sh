echo "Creating a new keyspace [mynewks]:"
curl -X POST http://localhost:8080/myapp/intrareq-rest/mynewks/
echo
echo "Listing keyspaces to see if it is there:"
curl -X GET http://localhost:8080/myapp/intrareq-rest/
echo
