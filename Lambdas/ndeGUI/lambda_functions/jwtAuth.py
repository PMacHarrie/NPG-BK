"""
    Author: Hieu Phung; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import psycopg2
import jwt
import bcrypt
import json
import os
from datetime import datetime, timedelta


def lambda_handler(event, context):
    print('jwtAuth')
    print(event)
    print(json.dumps(event))
    
    inputBody = None
    if 'pathParameters' in event:
        resource = event.get('path').split('/')[-1]
        method = event.get('httpMethod')
        
        print('resource/method:', resource, method)
    else:
        createErrorResponse(404, "Error", "Invalid resource path")
    
    try:
        print('Connecting to Postgres...')
        conn = psycopg2.connect(
            host = os.environ['RD_HOST'],
            dbname = os.environ['RD_DBNM'],
            user = os.environ['RD_USER'],
            password = os.environ['RD_PSWD']
        )
        
        if resource == 'signin':
            if event.get('body') is None:
                return createErrorResponse(400, "Validation error", "Body is required")
            else:
                if isinstance(event.get('body'), dict):
                    inputBody = event.get('body')
                else:
                    inputBody = json.loads(event.get('body'))
                    
            return signin(conn, inputBody)
        else:
            userAuthToken = None;
            authHeader = event.get('headers').get('Authorization')
            if authHeader is not None and authHeader.startswith('Bearer'):
                userAuthToken = authHeader.split(' ')[-1]
            else:
                createErrorResponse(401, "Error", "Bearer token malformed")
            
            if resource == 'user':
                return user(conn, userAuthToken)
            elif resource == 'signout':
                return signout(conn, userAuthToken)
            else:
                createErrorResponse(404, "Error", "Invalid resource path")
            
        return ''
    except Exception as e:
        print("ERROR: " + str(e))
    finally:
        conn.rollback()
        conn.close()

    

def signin(conn, inputBody):
    print('signin')
    
    userId = inputBody.get('userid')
    pw = inputBody.get('password')

    cur = conn.cursor()
    cur.execute("SELECT user_id, password FROM gui_user WHERE user_id = %s", (userId, ))
    
    row = cur.fetchone()
    print(row)
    
    if row is not None:
        print('decrypting')
        if bcrypt.checkpw(str.encode(pw), str.encode(row[1])):
            print('valid user')
            
            payload = {
                'exp': datetime.utcnow() + timedelta(days=0, hours=12),
                # 'exp': datetime.utcnow() + timedelta(days=0, minutes=5),
                'iat': datetime.utcnow(),
                'sub': row[0]
            }
            
            authToken = jwt.encode(payload, os.environ['SCRTKEY'], algorithm='HS256')
            print("authToken", authToken)
            
            cur.execute("UPDATE gui_user SET jwt = %s WHERE user_id = %s", (authToken.decode(), userId))
            conn.commit()
            cur.close()
            
            return {
                "isBase64Encoded": False,
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": '*'
                },
                "body": json.dumps({
                    "message": "sign in successful",
                    "auth_token": authToken.decode()
                })
            }
            
        else:
            return createErrorResponse(401, "Authentication error", "Incorrect userId or password")
    else:
        return createErrorResponse(401, "Authentication error", "Unknown userId or password")
    
                
    print('signin')
    
    
def user(conn, userAuthToken):
    print('user')
    
    try:
        payload = jwt.decode(userAuthToken, os.environ['SCRTKEY'])
        print("payload", payload)
        userId = payload.get('sub')
        
        cur = conn.cursor()
        cur.execute("SELECT user_id, name, email, is_admin FROM gui_user " + 
            "WHERE user_id = %s AND jwt = %s", (userId, userAuthToken))
        row = cur.fetchone()
        print(row)
        
        if row is not None:
            userObj = {
                "userId": row[0],
                "userName": row[1],
                "userEmail": row[2],
                "userIsAdmin": row[3]
            }
            cur.close()
            
            return {
                "isBase64Encoded": False,
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": '*'
                },
                "body": json.dumps({
                    "message": "success",
                    "user": userObj
                })
            }
        else:
            return createErrorResponse(401, "Authentication error", "Invalid token")
        
        
    except jwt.ExpiredSignatureError as ese:
        print(ese)
        return createErrorResponse(401, "Error", str(ese))
    except jwt.InvalidTokenError as ite:
        print(ite)
        return createErrorResponse(401, "Error", str(ite))
    except Exception as e:
        print(e)
        return createErrorResponse(401, "Error", str(e))
        
    
    
def signout(conn, userAuthToken):
    print('signout')
    
    try:
        payload = jwt.decode(userAuthToken, os.environ['SCRTKEY'])
        print("payload", payload)
        userId = payload.get('sub')
        
        cur = conn.cursor()
        cur.execute("UPDATE gui_user SET jwt = null WHERE user_id = %s AND jwt = %s", 
            (userId, userAuthToken))
        conn.commit()
        cur.close()
        
        return {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": '*'
            },
            "body": json.dumps({
                "message": "success"
            })
        }
        
    except jwt.ExpiredSignatureError as ese:
        print(ese)
        return createErrorResponse(401, "Error", str(ese))
    except jwt.InvalidTokenError as ite:
        print(ite)
        return createErrorResponse(401, "Error", str(ite))
    except Exception as e:
        print(e)
        return createErrorResponse(401, "Error", str(e))
    
    
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