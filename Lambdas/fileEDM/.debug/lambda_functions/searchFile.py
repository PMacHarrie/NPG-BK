"""
    Author: Hieu Phung; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

"""
SAMPLE INPUT FORMAT:
{
	"query": {
        "fullText": "query string",
		"edmCore": {
			"productShortNames": ["VIIRS-M12-SDR", "VIIRS-M11-SDR"],
			"fileIds": ["fileId1", "fileId2"],
			"fileNames": ["fileName1", "fileName2"],
			"timeRange": {
				"startTime": "20180926T223727.900Z",
				"endTime": "20180926T223757.700Z"
			},
			"spatialArea": {
				"topLeft": [-87.3809661865234, 48.8152542114258],
				"bottomRight": [-72.6752777099609, 34.4298515319824]
			},
			"orbitRange": {
				"fileBeginOrbitNum": 4888,
				"fileEndOrbitNum": 4891
			},
			"fileDayNightFlag": "Day|Night|Both",
			"fileAscDescIndicator": 1
		},
		"objectGroupAttrs": {
			"groupAttrName1": "groupAttrValue1",
			"groupAttrName2": "groupAttrValue2",
		}
	},
	"result": {
		"format": "full | edmCore | attrList",
		"attrList*": ["attr1 ", "attr2", "attr3"],
		"temporalSum": true | false,
		"sort": [
            {"attr1": "asc"}, 
            {"attr2": "desc"}
		],
		"limit": {
            "from": 0,
            "size": 100
        }
	}
}
"""

import os
import json
import certifi
import elasticsearch
from datetime import datetime
import re
import traceback
import psycopg2

from ndeutil import *


print('Loading searchFile')

conn = psycopg2.connect(
    host = os.environ['RD_HOST'],
    dbname = os.environ['RD_DBNM'],
    user = os.environ['RD_USER'],
    password = os.environ['RD_PSWD']
    )

def lambda_handler(event, context):
    print(event)
    
    if 'resource' in event:
        if event.get('body') is None:
            return createErrorResponse(400, "Validation error", 
                "body (search json) required")
        if isinstance(event.get('body'), dict):
            inputBody = event.get('body')
        else:
            inputBody = json.loads(event.get('body'))
    else:
        inputBody = event

    # validate the "query" portion of the request
    inQuery = inputBody.get('query')
    if inQuery is None:
        return createErrorResponse(400, "Validation error", "Must specify query")
    
    print("inQuery", inQuery)
    if 'fullText' not in inQuery and 'edmCore' not in inQuery and 'objectGroupAttrs' not in inQuery:
        return createErrorResponse(400, "Validation error", "fullText, edmCore, or objectGroupAttrs is required for query")

    sqlWheres = []
    mustSnippet = []
    # shouldSnippet = []
    filterSnippet = []
    
    searchBody = {"query": {"bool": {} } }
    
    if inQuery.get('fullText') is not None:
        if not isinstance(inQuery.get('fullText'), str):
            return createErrorResponse(400, "Validation error", "fullText should be a string")
        mustSnippet.append( {
            "multi_match": {
                "query": inQuery.get('fullText'), 
                "fields": ["edmCore.*", "objectGroupAttrs.*"],
                "lenient": True 
            }
        } )
    print("mustSnippet", mustSnippet)
    
    if inQuery.get('edmCore') is not None:
        edmCoreReq = inQuery.get('edmCore')
        
        # validate and parse spatialArea
        if edmCoreReq.get('spatialArea') is not None:
            spatialArea = edmCoreReq.get('spatialArea')
            if not isinstance(spatialArea, dict):
                return createErrorResponse(400, "Validation error",  "spatialArea must contain topLeft and bottomRight keys")
            if spatialArea.get('topLeft') is None or spatialArea.get('bottomRight') is None:
                return createErrorResponse(400, "Validation error", "topLeft and bottomRight required")
            if not isinstance(spatialArea.get('topLeft'), list) or len(spatialArea.get('topLeft')) != 2:
                return createErrorResponse(400, "Validation error", "topLeft should be array with one lon/lat pair")
            if not isinstance(spatialArea.get('bottomRight'), list) or len(spatialArea.get('bottomRight')) != 2:
                return createErrorResponse(400, "Validation error", "bottomRight should be array with one lon/lat pair")
            # filterSnippet.append( {"geo_shape": {
            #                             "edmCore.fileSpatialArea": {
            #                                 "shape": {
            #                                     "type": "envelope", 
            #                                     "coordinates": [spatialArea.get('topLeft'), spatialArea.get('bottomRight')]
            #                                 },
            #                                 "relation": "intersects"
            #                             } 
            #                         } } )
            # print("filterSnippet", filterSnippet)
            left, top = spatialArea.get('topLeft')
            right, bottom = spatialArea.get('bottomRight')
            try:
                float(left)
                float(top)
                float(right)
                float(bottom)
            except ValueError as ve:
                return createErrorResponse(400, "Validation error", "invalid lon or lat value (must be numeric)")
            # geometry ST_MakeEnvelope(double precision xmin, double precision ymin, double precision xmax, double precision ymax, integer srid);
            sqlWheres.append("ST_Intersects(ST_MakeEnvelope(%s, %s, %s, %s, 4326), filespatialarea)" % (left, bottom, right, top))
        
        # validate and parse productShortNames
        if edmCoreReq.get('productShortNames') is not None:
            if not isinstance(edmCoreReq.get('productShortNames'), list):
                edmCoreReq['productShortNames'] = [edmCoreReq.get('productShortNames')]
            
            shouldSnippet = []
            for psn in edmCoreReq.get('productShortNames'):
                if not isinstance(psn, str):
                    return createErrorResponse(400, "Validation error", "Invalid productShortName: " + psn)
                shouldSnippet.append( {"bool": {"must": {"match" : {"edmCore.productShortName.kw": psn} } } } )
            filterSnippet.append( {"bool": {"should": shouldSnippet} } )
            # This code implements tokenizing the shortname on - and _ 
            # It does OR between shortnames and AND for shortname parts: (sn1p1 AND sn1p2 AND ...) OR (sn2p1 AND sn2p2 AND ...)
            # shouldSnippet = []
            # for psn in edmCoreReq.get('productShortNames'):
            #     prodMustSnippet = []
            #     if not isinstance(psn, str):
            #         return createErrorResponse(400, "Validation error", "Invalid productShortName: " + psn)
            #     psnParts = re.split('-|_', psn)
            #     print(psnParts)
            #     for psnPart in psnParts:
            #         prodMustSnippet.append( {"match" : {"edmCore.productShortName": psnPart} } )
            #     # shouldSnippet.append( {"match" : {"edmCore.productShortName.kw": psn} } )
            #     shouldSnippet.append( {"bool": {"must": prodMustSnippet} } )
            # print("shouldSnippet", shouldSnippet)
            # mustSnippet.append( {"bool": {"should": shouldSnippet} } )
            print("filterSnippet", filterSnippet)
            if len(sqlWheres) > 0:
                sqlWheres.append("productId IN (SELECT productId FROM productdescription WHERE productshortname in ('%s') )" 
                    % "','".join(edmCoreReq.get('productShortNames')))
        
        # validate and parse fileIds
        if edmCoreReq.get('fileIds') is not None:
            if not isinstance(edmCoreReq.get('fileIds'), list):
                edmCoreReq['fileIds'] = [edmCoreReq.get('fileIds')]

            for fileId in edmCoreReq.get('fileIds'):
                try:
                    int(fileId)
                except ValueError as e:
                    print("ERROR: Failed to parse fileId as int/long: " + str(e))
                    return createErrorResponse(400, "Validation error", "Invalid fileId: " + fileId)

            if len(sqlWheres) > 0:
                sqlWheres.append("fileId in ('%s')" 
                    % "','".join(edmCoreReq.get('fileIds')))
            else:
                shouldSnippet = [ {"terms" : {"edmCore.fileId": edmCoreReq.get('fileIds')} } ] 
                filterSnippet.append( {"bool": {"should": shouldSnippet} } )
                print("filterSnippet", filterSnippet)
            
        # validate and parse fileNames
        if edmCoreReq.get('fileNames') is not None:
            if not isinstance(edmCoreReq.get('fileNames'), list):
                edmCoreReq['fileNames'] = [edmCoreReq.get('fileNames')]
            shouldSnippet = []
            for fileName in edmCoreReq.get('fileNames'):
                fnMustSnippet = []
                if not isinstance(fileName, str):
                    return createErrorResponse(400, "Validation error", "Invalid fileName: " + fileName)
                fileNameParts = re.split('-|_', fileName)
                print(fileNameParts)
                for fileNamePart in fileNameParts:
                    fnMustSnippet.append( {"match" : {"edmCore.fileName": fileNamePart} } )
                shouldSnippet.append( {"bool": {"must": fnMustSnippet} } )
            filterSnippet.append( {"bool": {"should": shouldSnippet} } )
            print("filterSnippet", filterSnippet)
    
        # validate and parse timeRange
        if edmCoreReq.get('timeRange') is not None:
            timeRange = edmCoreReq.get('timeRange')
            if not isinstance(timeRange, dict):
                return createErrorResponse(400, "Validation error", "timeRange must contain startTime and endTime keys")
            if timeRange.get('startTime') is None or timeRange.get('endTime') is None:
                return createErrorResponse(400, "Validation error", "startTime and endTime are required")
            try:
                sTime = datetime.strptime(timeRange.get('startTime'),'%Y-%m-%dT%H:%M:%S.%fZ')
                eTime = datetime.strptime(timeRange.get('endTime'),'%Y-%m-%dT%H:%M:%S.%fZ')
                if eTime < sTime:
                    return createErrorResponse(400, "Validation error", "endTime must not be before startTime")
            except ValueError as ve:
                return createErrorResponse(400, "Validation error",  
                    "startTime and endTime must be formatted as: YYYY-MM-DDTHH:mm:ss.sssZ")
            filterSnippet.append( {"range" : {"edmCore.fileStartTime": {"lte": timeRange.get('endTime')} } } )
            filterSnippet.append( {"range" : {"edmCore.fileEndTime": {"gte": timeRange.get('startTime')} } } )
            print("filterSnippet", filterSnippet)
            if len(sqlWheres) > 0:
                sqlWheres.append("filestarttime <= '%s'::timestamp AND fileendtime >= '%s'::timestamp " 
                    % (timeRange.get('endTime'), timeRange.get('startTime')) )
        
        # validate and parse orbitRange
        if edmCoreReq.get('orbitRange') is not None:
            orbitRange = edmCoreReq.get('orbitRange')
            if not isinstance(orbitRange, dict):
                return createErrorResponse(400, "Validation error", 
                    "orbitRange must contain fileBeginOrbitNum and fileEndOrbitNum keys")
            if orbitRange.get('fileBeginOrbitNum') is None or orbitRange.get('fileEndOrbitNum') is None:
                return createErrorResponse(400, "Validation error", "fileBeginOrbitNum and fileEndOrbitNum are required")
            if not isinstance(orbitRange.get('fileBeginOrbitNum'), int) or not isinstance(orbitRange.get('fileEndOrbitNum'), int):
                return createErrorResponse(400, "Validation error", "fileBeginOrbitNum and fileEndOrbitNum should be numeric")
            if int(orbitRange.get('fileEndOrbitNum')) < int(orbitRange.get('fileBeginOrbitNum')):
                return createErrorResponse(400, "Validation error", "fileEndOrbitNum must not be less than fileBeginOrbitNum")
            filterSnippet.append( {"range" : {"edmCore.fileBeginOrbitNum": {"lte": int(orbitRange.get('fileEndOrbitNum')) } } } )
            filterSnippet.append( {"range" : {"edmCore.fileEndOrbitNum": {"gte": int(orbitRange.get('fileBeginOrbitNum')) } } } )
            print("filterSnippet", filterSnippet)
        
        # validate and parse Flags
        if edmCoreReq.get('fileDayNightFlag') is not None:
            if edmCoreReq.get('fileDayNightFlag') not in ['Day', 'Night', 'Both']:
                return createErrorResponse(400, "Validation error", "fileDayNightFlag must be Day or Night or Both")
            filterSnippet.append( {"match" : {"edmCore.fileDayNightFlag": edmCoreReq.get('fileDayNightFlag') } } )
            print("filterSnippet", filterSnippet)
            
        if edmCoreReq.get('fileAscDescIndicator') is not None:
            if edmCoreReq.get('fileAscDescIndicator') not in ['0', '1', 0, 1]:
                return createErrorResponse(400, "Validation error", "fileAscDescIndicator must be 0 or 1")
            filterSnippet.append( {"match" : {"edmCore.fileAscDescIndicator": int(edmCoreReq.get('fileAscDescIndicator')) } } )
            print("filterSnippet", filterSnippet)
    
    # validate and parse objectGroupAttrs
    if inQuery.get('objectGroupAttrs') is not None:
        oga = inQuery.get('objectGroupAttrs')
        if isinstance(inQuery.get('objectGroupAttrs'), dict):
            for attrName, attrVal in inQuery.get('objectGroupAttrs').items():
                mustSnippet.append( {"match" : {"objectGroupAttrs." + attrName: attrVal } } )
        else:
            return createErrorResponse(400, "Validation error", 
                "objectGroupAttrs must be an object with keys/values: groupAttrName1: groupAttrValue1")
        print("mustSnippet", mustSnippet)
    
        
    # validate the "result" portion of the request
    inResult = inputBody.get('result')
    print("inResult", inResult)
    if inResult is None:
        print("No result output result format specified, using defaults (edmCore, 1000)")
        searchBody["_source"] = ["edmCore"]
        searchBody["from"] = 0
        searchBody["size"] = 1000
    else:
        # validate the format, create list for attrList
        if inResult.get('format') is None:
            inResult['format'] = 'edmCore'
        if inResult.get('format') == 'full':
            # do nothing, _source defaults to the entire doc
            pass
        elif inResult.get('format') == 'edmCore':
            searchBody["_source"] = ["edmCore"]
        elif inResult.get('format') == 'attrList':
            if inResult.get('attrList') is None:
                return createErrorResponse(400, "Validation error", "attrList required for format: attrList")
            if not isinstance(inResult.get('attrList'), list):
                return createErrorResponse(400, "Validation error", "attrList should be array of attribute names")
            newAttrList = []
            for attr in inResult.get('attrList'):
                if not attr.startswith("edmCore") and not attr.startswith("objectGroup"):
                    newAttrList.append(addFieldParentPrefix(attr))
                else:
                    newAttrList.append(attr)
            print(newAttrList)
            searchBody["_source"] = newAttrList
        else:
            return createErrorResponse(400, "format requires one of: full | edmCore | attrList")
    
        # validate and format sort
        if inResult.get('sort') is not None:
            if not isinstance(inResult.get('sort'), list):
                return createErrorResponse(400, "Validation error", "sort should be an array of " + 
                    "{\"attr1\": \"asc\" | \"desc\"}")
            newAttrList = []
            for sortItem in inResult.get('sort'):
                (attrName, sortDir), = sortItem.items()
                if sortDir not in ['asc', 'desc']:
                    return createErrorResponse(400, "Validation error", "sort value must be: asc | desc")
                if attrName in ['fileSpatialArea', 'fileName']:
                    return createErrorResponse(400, "Validation error", "results cannot be sorted on: "+ attrName)
                if attrName in ['productShortName', 'platformNames']:
                    attrName = attrName + ".kw"
                if not attrName.startswith("edmCore") and not attrName.startswith("objectGroup"):
                    newAttrList.append( {addFieldParentPrefix(attrName): sortDir} )
                else:
                    newAttrList.append(sortItem)
            print(newAttrList)
            searchBody["sort"] = newAttrList
            
        if inResult.get('limit') is not None:
            limit = inResult.get('limit')
            if not isinstance(limit, dict):
                return createErrorResponse(400, "Validation error", "limit must contain from and size keys")
            if limit.get('from') is None or limit.get('size') is None:
                return createErrorResponse(400, "Validation error", "from and size are required")
            try:
                int(limit.get('from'))
                int(limit.get('size'))
            except ValueError as ve: 
                return createErrorResponse(400, "Validation error", "from and size should be numeric")
            searchBody["from"] = int(limit.get('from'))
            searchBody["size"] = int(limit.get('size'))
        else:
            searchBody["from"] = 0
            searchBody["size"] = 1000

    # Query Postgres if there is spatial area param
    if len(sqlWheres) > 0:
        cur = conn.cursor()
        print("SELECT JSON_AGG(fileid) FROM filemetadata WHERE " + " AND ".join(sqlWheres))
        cur.execute("SELECT JSON_AGG(fileid) FROM filemetadata WHERE " + " AND ".join(sqlWheres))
        row = cur.fetchone()
        print("Got %s rows from Postgres" % len(row[0]))
        # print(row[0])
        # Use the fileIds from Postgres as an arg to Elastic query
        shouldSnippet = [ {"terms" : {"edmCore.fileId": row[0] } } ] 
        filterSnippet.append( {"bool": {"should": shouldSnippet} } )
        print("filterSnippet", filterSnippet)

    # build elastic search body json (query portion)
    if len(mustSnippet) > 0:
        searchBody["query"]['bool']['must'] = mustSnippet
    # if len(shouldSnippet) > 0:
    #     searchBody["query"]['bool']['should'] = shouldSnippet
    if len(filterSnippet) > 0:
        searchBody["query"]['bool']['filter'] = filterSnippet
        
        
    searchBodyJson = json.dumps(searchBody)
    print(searchBodyJson)

    es = elasticsearch.Elasticsearch(
        [os.environ['ES_HOST']],
        port=os.environ['ES_PORT'],
        use_ssl=True,
        verify_certs=True,
        ca_certs=certifi.where())
    
    # query elastic search and return the _source of each hit
    try:
        res = es.search(index = "file", doc_type = "_doc", body = searchBodyJson)
        print(res)
        if res['timed_out']:
            createErrorResponse(408, "Query Timeout", 
                "query to elastic search timed out, please try restricting parameters and try again")
        if res['hits']['total'] == 0:
            responseBody = {"message": "no results found for the provided parameters", "result": []}
        elif res['hits']['total'] > 0:
            matches = [doc['_source'] for doc in res['hits']['hits']]
            responseBody = {"message": "success", "total_matches": res['hits']['total'], "result": matches}
            print(responseBody)
            
    except Exception as e:
        print("ERROR while querying:")
        traceback.print_exc()
        return createErrorResponse(500, "Internal error", "Exception encountered while querying")

    return {
        "isBase64Encoded": True,
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": '*'
        },
        "body": json.dumps(responseBody)
    }
    
    
def addFieldParentPrefix(attr):
    if attr in ['productShortName', 'productShortName.kw', 'fileId', 'fileInsertTime', 'fileStartTime',
                'fileEndTime', 'platformNames', 'platformNames.kw', 'fileBeginOrbitNum', 'fileEndOrbitNum', 
                'fileDayNightFlag', 'fileAscDescIndicator', 'fileSpatialArea', 'fileName']:
        retAttr = "edmCore." + attr
    else:
        retAttr = "objectGroupAttrs." + attr
    return retAttr