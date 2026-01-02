.PHONY: build deploy localtest

build:
	sam build

deploy:
	sam deploy --guided

localtest:
	cd src/schema_diff && python local_test.py
