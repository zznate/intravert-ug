echo "Setting [myks/mycf/mycol] -> [myval]"
curl -X POST http://localhost:8080/intravert/intrareq-rest/myks/mycf/myrow/mycol -d "myval"
echo "Getting [myks/mycf/mycol]"
curl -X GET http://localhost:8080/intravert/intrareq-rest/myks/mycf/myrow/mycol 
