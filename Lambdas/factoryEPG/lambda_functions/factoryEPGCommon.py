"""
    Author: Jonathan Hansford; SOLERS INC
    Contact: 
    Last modified: 2019-01-31
    Python Version: 3.6
"""
from datetime import datetime

class CodeSegmentTimer:
    """
    A simple timer to measure the amount of wall clock time segments of code take.
    """
    def __init__(self, taskDescription):
        """
        Creates a starts a new CodeSegmentTimer.
        The taskDescription will be included in the message printed when this timer is finished.
        """
        self.taskDescription = taskDescription
        self.isFinished = False
        self.startTime = datetime.now()
        
    def finishAndPrintTime(self):
        """
        Finishes the timer and prints a message indicating how much time elapsed. 
        This function may only be called once per CodeSegmentTimer object.
        """
        finishTime = datetime.now()
        if not self.isFinished:
            isFinished = True
            print('%s took %s' % (self.taskDescription, str(finishTime - self.startTime)))
        else:
            raise Exception("CodeSegmentTimer for task \"%s\" was finished twice!" % self.taskDescription)


def runStoredProcedureToGetServerSideCursor(databaseConnection, storedProcedureName, storedProcedureArguments):
    """
    Runs a stored procedure that returns the name of a server-side cursor. Returns a psycopg2 cursor object which is the returned server-side cursor.
    
    databaseConnection should be an open databaseConnection. The storedProcedureArguments parameter should be a tuple.
    """
    
    with databaseConnection.cursor() as databaseCursor:
        databaseCursor.callproc(storedProcedureName, storedProcedureArguments)
    
        spResults = databaseCursor.fetchall()
        if len(spResults) == 1:
            serverSideCursorName = (spResults[0])[0]
        
            serverSideCursor = databaseConnection.cursor(serverSideCursorName)
        
            return serverSideCursor
        else:
            raise Exception('Stored procedure %s returned an unexpected number of results: %d' % (storedProcedureName, len(spResults)))