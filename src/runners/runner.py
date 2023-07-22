import logging


class Runner():

    def __init__(self, cmdArgs):
        self.cmdArgs = cmdArgs

    def __getattr__(self, attr):
        return self.cmdArgs[attr]

    @classmethod
    def setLogLevel(cls, level):
        for name in logging.root.manager.loggerDict:
            logger = logging.getLogger(name)
            logger.setLevel(level)

    def showStatus(self, msg) -> str:
        if not 'statusBar' in self.cmdArgs:
            print(str)
            return None
        return self.cmdArgs['statusBar'].queueMessage(msg)

    def hideStatus(self, statusId):
        if statusId is None:
            return False
        self.cmdArgs['statusBar'].cancelMessage(statusId)
