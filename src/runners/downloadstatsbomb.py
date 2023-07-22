import requests

from .runner import Runner


class DownloadStatsBombRunner(Runner):

    def __init__(self, cmdArgs, *args, **kwargs):
        super().__init__(cmdArgs)

    def execute(self):
        batchSize = 1024*1024

        url = 'https://github.com/statsbomb/open-data/archive/refs/heads/master.zip'
        with requests.get(url, allow_redirects=True, stream=True) as r:
            with open(self.cmdArgs['zipfile'], 'wb') as f:
                totalSize = int(r.headers.get('content-length'))
                currentSize = 0
                statusId = None

                for batch in r.iter_content(chunk_size=batchSize):
                    currentSize += len(batch)
                    if statusId:
                        self.hideStatus(statusId)
                    statusId = self.showStatus(
                        'Downloaded {} / {} of {}'.format(
                            currentSize, totalSize, self.cmdArgs['zipfile'])
                    )
