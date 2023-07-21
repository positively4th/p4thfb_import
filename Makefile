.PHONY: \
	setup setup-requirements setup-setup-contrib \
	data data-download data-clean \
	import download-data

default: setup docker-import

all: setup docker data

container: setup

#setup
setup: setup-requirements setup-contrib

setup-requirements: 
	python -m venv .venv \
	&& ( \
		source .venv/bin/activate \
		&& \
		pip install --upgrade pip \
		&& \
		pip install -r requirements.txt \
	)

setup-contrib: 
	make -C contrib


#docker
docker: docker-import

docker-import:
	docker build -t import -f import.dockerfile .

docker-run: import
	docker run --name import:latest -it import


#data
data: data-download

data-download:
	make -C data/statsbomb/open-data 

data-clean:
	make -C data/statsbomb/open-data clean 

clean: 
	rm -rf .venv \
	&& make -C contrib clean \
	&& docker rmi --force import

