build:
	mkdir -p ./dist
	cp ./src/main.py ./dist
	cd ./src && zip -x main.py -x \*libs\* \*data\* -r ../dist/jobs.zip .
	cd ./src/libs && zip -r ../../dist/libs.zip .
	cp -a ./src/data dist/
