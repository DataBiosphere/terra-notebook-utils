include common.mk

MODULES=terra_notebook_utils tests
tests:=$(wildcard tests/test_*.py)

export TNU_TESTMODE?=workspace_access

test: lint mypy tests

dev_env_access_test:
	$(MAKE) TNU_TESTMODE="dev_env_access" test

all_test: 
	$(MAKE) TNU_TESTMODE="workspace_access controlled_access" test

controlled_access_test:
	$(MAKE) TNU_TESTMODE="controlled_access" test

lint:
	flake8 $(MODULES) *.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

test: $(tests)
	coverage combine
	rm -f .coverage.*

# A pattern rule that runs a single test script
$(tests): %.py : mypy lint
	coverage run -p --source=terra-notebook-utils $*.py --verbose

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
