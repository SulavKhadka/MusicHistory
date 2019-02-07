import json
import time
import requests
import datetime
from pymongo import MongoClient


def build_link(baseApiPath, formatVars, methodKey, methodVars, apiCreds):
    
    # Base variables declaration
    formatKey = formatVars['URL_KEY']
    formatType = formatVars['TYPE']
    methodName = methodVars['NAME']
    methodParams = methodVars['PARAMS']
    acceptedParams = methodVars['ACCEPTED']

    methodString =  "{methodKey}{methodName}".format(methodKey=methodKey, methodName=methodName)
    formatString = "{formatKey}{formatType}".format(formatKey=formatKey, formatType=formatType)

    paramsString = ""
    # Checks and creates a parameter list to pass in to the api link
    apiParams = {**apiCreds, **methodParams}
    for k,v in apiParams.items():
        if v and k.lower() in acceptedParams:
            paramsString += "&{key}={value}".format(key=k.lower(), value=v)
            
    return "{base}?{methodString}{paramsString}&{formatString}".format(base=baseApiPath, methodString=methodString, paramsString=paramsString, formatString=formatString)


def time_to_unix_timestamp(dateString):
    try:
        dtObject = datetime.datetime.strptime(dateString, "%Y-%m-%d")
    except Exception as e:
        print(e)
        return ""

    return int(dtObject.replace(tzinfo=datetime.timezone.utc).timestamp())


def get_api_reponse(link):
    response = requests.get(link) 
    
    successFlag = False
    responseMsg = {"error": response.text, "status_code": response.status_code, "link": link, "response_msg": ""}
    if response.status_code == 200:
        try:
            responseMsg = response.json()
            successFlag = True
        except Exception as e:
            responseMsg['error'] = e
            responseMsg['response_msg'] = response.text
    
    return (successFlag, responseMsg)


def insert_to_mongo(dbCursor, dataList, retryAmount=0):
    for _ in range(retryAmount):
        try:
            newResult = dbCursor.update_many(dataList, upsert=True)
            return True
        except Exception as e:
            print("Error", e)
            print("Retrying in 5 seconds")
            time.sleep(5)
    
    return False 


def update_all(dbCursor, apiVars, retryAmount=0):
    
    # Runs till there are no more songs to put in. [Have yet to add continious error escape from the while loop]
    exitUpdateFlag = False
    while not exitUpdateFlag:
        # Parses appropiate methods from the variable JSON object and builds the request link
        apiCreds = apiVars['CREDS']
        basePath = apiVars['BASE_LINK_PATH']
        formatVars = apiVars['FORMAT']
        methodKey = apiVars['METHODS']['URL_KEY']
        methodVars = apiVars['METHODS']['USER']['GET_RECENT_TRACKS']

        # Time conversion to UNIX timestamp
        methodVars['PARAMS']['FROM'] = time_to_unix_timestamp(methodVars['PARAMS']['FROM'])
        methodVars['PARAMS']['TO'] = time_to_unix_timestamp(methodVars['PARAMS']['TO'])

        requestLink = build_link(baseApiPath=basePath, formatVars=formatVars, methodKey=methodKey, methodVars=methodVars, apiCreds=apiCreds)
        print(requestLink)

        # Makes the get request to API, until success or until the specified retry amount.
        apiCallSuccess = False
        for tryCount in range(retryAmount):
            print("Trial Number", tryCount+1)
            try:
                apiCallSuccess, apiResponse = get_api_reponse(requestLink)
                print (apiCallSuccess)
            except Exception as e:
                print("Error", e)
                time.sleep(5)
            
            if apiCallSuccess:
               break 
        
        # If the API call succeeds, parses the response and tries to update the database. [handles duplicates and updates]
        if apiCallSuccess:
                print("Page", apiResponse['recenttracks']['@attr']['page'])
                trackList = apiResponse['recenttracks']['track']
                if trackList:
                    try:
                        newResult = insert_to_mongo(dbCursor=dbCursor, dataList=trackList, retryAmount=3)
                        print("posted")
                    except Exception as e:
                        print("Darn")
                        print(e)    
                else:
                    print("Track list empty.")
                    exitUpdateFlag = True


def main():

    # Retreiving variables for the project from creds.json file.
    with open("userEnvVars/variables.json", "r") as credsFile:
        variables = json.load(credsFile)
    
    # Assigning variables to relative groups
    apiVars = variables['API']
    dbVars = variables['DB']
    userVars = variables['USER_VARIABLES']
    
    # Initializing the MongoDB connection and getting a cursor for operations.
    client = MongoClient(dbVars['SERVER_NAME'], dbVars['PORT'])
    db = client.dbVars['DB_NAME']
    dbCursor = db.posts

    update_all(dbCursor, apiVars, userVars['RETRY_AMOUNT'])

main()
