import json

def createErrorResponse(code, error, msg):
    errorResponse = {
        "isBase64Encoded": False,
        "statusCode": code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": '*'
        },
        "body": json.dumps({
            "error": error,
            "message": msg
        })
    }
    print(errorResponse)
    return errorResponse


def isStrEmptyNull(strIn):
    if (strIn is None or strIn == "" or strIn.lower() == "null"):
        return True
    else:
        return False