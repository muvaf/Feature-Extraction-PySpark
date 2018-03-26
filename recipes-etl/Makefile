production:
	pip install -r requirements.txt -t ./recipes/libs
	rm -rf dist && mkdir -p ./dist
	cp ./recipes/main.py ./dist
	cd ./recipes && zip -x main.py -x \*libs\* \*data\* -r ../dist/jobs.zip .
	cd ./recipes/libs && zip -r ../../dist/libs.zip .
