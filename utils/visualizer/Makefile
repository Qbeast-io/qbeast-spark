install:
	pip install --upgrade pip &&\
        pip install -r requirements.txt -e .

test:
	cd ./tests &&\
		python -m unittest discover -p "*_test.py" &&\
        cd ..
