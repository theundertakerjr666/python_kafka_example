#!/usr/bin/python3
import datetime


class EventLog:
    clientProcess = "UNDEFINED"
    logMessageString = ""
    logDate = datetime.datetime.now()

    def __init__(self):
        print("Calling constructor")

    def EventLog(self, clientProcess, inputLogMessage, logDate=datetime.datetime.now()):
        self.clientProcess = clientProcess
        self.inputLogMessage = inputLogMessage
        self.logDate = logDate
        return self

    def setClientProcess(self, clientProcess):
        self.clientProcess = clientProcess

    def setInputLogMessage(self, inputLogMessage):
        self.inputLogMessage = inputLogMessage

    def setLogDate(self, logDate):
        self.logDate = logDate
