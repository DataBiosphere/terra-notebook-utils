.PHONY: test lint mypy tests clean build install
MODULES=terra_notebook_utils tests

test: lint mypy tests

lint:
	flake8 $(MODULES) *.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

tests:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=terra_notebook_utils \
		-m unittest discover --start-directory tests --top-level-directory . --verbose

version: terra_notebook_utils/version.py

terra_notebook_utils/version.py: setup.py
	echo "__version__ = '$$(python setup.py --version)'" > $@

clean:
	-rm -rf build dist
	-rm -rf *.egg-info

build: version clean
	-rm -rf dist
	python setup.py bdist_wheel

install: build
	pip install --upgrade dist/*.whl
