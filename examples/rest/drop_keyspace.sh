echo "Dropping a keyspace [mynewks]:"
curl -X DELETE http://localhost:8080/intravert/intrareq-rest/mynewks2/
echo
echo "Listing keyspaces to see if it is there:"
curl -X GET http://localhost:8080/intravert/intrareq-rest/
echo
