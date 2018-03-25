production:
	pip install -r requirements.txt -t ./src/libs
	rm -rf dist && mkdir -p ./dist
	cp ./src/main.py ./dist
	cd ./src && zip -x main.py -x \*libs\* \*data\* -r ../dist/jobs.zip .
	cd ./src/libs && zip -r ../../dist/libs.zip .
	cp -a ./src/data dist/
