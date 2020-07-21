include common.mk

MODULES=terra_notebook_utils tests

export TNU_TESTMODE?=workspace_access

test: lint mypy tests

all_test: 
	$(MAKE) TNU_TESTMODE="workspace_access controlled_access" test

controlled_access_test:
	$(MAKE) TNU_TESTMODE="controlled_access" test

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
	git clean -dfx

build: version clean
	python setup.py bdist_wheel

sdist: clean
	python setup.py sdist

install: build
	pip install --upgrade dist/*.whl

.PHONY: test lint mypy tests clean build install
