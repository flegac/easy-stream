
install:
	pip install --upgrade twine build

build:
	python -m build

deploy-test:
	twine upload --repository testpypi dist/*

deploy-pypi:
	twine upload --repository pypi dist/*
