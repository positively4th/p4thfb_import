FROM fedora:38
RUN dnf -y upgrade
RUN dnf -y install make python3.10 python3.10-devel git gcc sqlite3 sqlite-devel pcre pcre-devel 
RUN dnf -y install cmake ruby yajl 


WORKDIR /tmp
RUN git clone https://github.com/lloyd/yajl.git
WORKDIR /tmp/yajl
RUN ./configure && make install

RUN mkdir /app
WORKDIR /app
ADD  src/ ./src/
ADD  contrib/ ./contrib/
COPY requirements.txt Makefile ./

RUN ln -s /usr/bin/python3.10 /usr/local/bin/python
RUN make container

COPY .env ./

RUN echo "#!/bin/bash" > /usr/bin/import_v2
RUN echo "cd /app && source .venv/bin/activate &&  PYTHONPATH="." python src/import_v2.py importstatsbomb" >> /usr/bin/import_v2
RUN chmod +x /usr/bin/import_v2

RUN rm -rf /tmp/yajl