import bottombar as bb
from threading import Lock
from threading import Thread
from time import sleep
from uuid import uuid4 as uuid


def getStatusBar(_statusBar=None):

    if _statusBar is not None:
        return _statusBar

    class StatusBar():

        @staticmethod
        def runner(self):
            def noMessage(): return {'text': '', 'ttl': 0}
            message = noMessage()
            sleepTime = 0.0
            with bb.add(' ', label='Status') as item:

                while True:

                    message['ttl'] -= sleepTime

                    with self.mseeagesLock:

                        pendingCount = len(self.messages) - 1
                        if pendingCount > 0:
                            message = self.messages.pop(0)
                            if message['ttl'] > 0:
                                self.messages.append(message)

                        if len(self.messages) < 1:
                            self.messages.append(noMessage())

                        message = self.messages.pop(0)

                        message['ttl'] = float(
                            message['ttl']) if 'ttl' in message else float('inf')
                        message['ttl'] = 3.0 if message['ttl'] is None else message['ttl']

                        text = message['text']
                        self.messages.insert(0, message)

                    sleepTime = 1
                    if pendingCount > 0:
                        text += ' +{}'.format(pendingCount)

                    item.text = text
                    sleep(sleepTime)

        def __init__(self):

            self.messages = []

            self.mseeagesLock = Lock()

            self.bbThread = Thread(
                target=self.runner, name='bbThread', args=(self,), daemon=True)
            self.bbThread.start()

        def queueMessage(self, msg) -> str:
            id = str(uuid())
            with self.mseeagesLock:
                self.messages.append({'text': msg, 'id': id})
            return id

        def cancelMessage(self, id):
            with self.mseeagesLock:
                res = len(self.messages)
                self.messages = [
                    message for message in self.messages if 'id' in message and message['id'] != id]
                return res - len(self.messages) > 0

    x_statusBar = StatusBar()

    return x_statusBar
