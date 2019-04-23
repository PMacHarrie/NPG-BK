#ps -ef | grep ingest | grep -v grep | awk '{print "kill " $2}' | bash

touch ingestNdeRest.pySTOP
touch ingestGoesRest.pySTOP

while [[ `ps -ef | grep ingest.*Rest.py | grep -v  grep | wc -l` -gt 0 ]]; do
  echo "waiting for scripts to stop..."
  sleep 1
done

rm ingestNdeRest.pySTOP
rm ingestGoesRest.pySTOP
