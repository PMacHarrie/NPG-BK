import elasticsearch as es
import json
import time

es_conn = {
        "conn" : es.Elasticsearch(
            ["https://vpc-nde-test-sncp4dqd63lf4rs2vdihlk763e.us-east-1.es.amazonaws.com:443"],
            verify_certs=False
            ),
        "arn" : "arn:aws:es:us-east-1:784330347242:domain/nde-test"
}

deleted = -1
while deleted <> 0:
	print "deleted=", deleted
	res = es_conn['conn'].delete_by_query(index="file", 
#	res = es_conn['conn'].delete_by_query(index="file", 
		body={
			"query" : { 
				"range": { "edmCore.fileInsertTime" : { "lte" : "now-6d"} }
			 }
		},
		size=5000
	)
	print json.dumps(res, indent=2)
	deleted = res['deleted']
	print "deleted=", deleted
	time.sleep(7)
