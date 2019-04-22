"""
    Author: Hieu Phung; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import os
import json
import psycopg2

from ndeutil import *

def lambda_handler(event, context):
    
    response = {
        "isBase64Encoded": False,
        "statusCode": 201,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": "whoaaaaaa there cowboy, not yet supported"
    }

    return response