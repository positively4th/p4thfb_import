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

COPY .env.docker ./.env

RUN rm -rf /tmp/yajl