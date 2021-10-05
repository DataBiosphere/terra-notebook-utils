include common.mk

MODULES=terra_notebook_utils tests
tests:=$(wildcard tests/test_*.py)

export TNU_TESTMODE?=workspace_access

test: lint mypy tests

dev_env_access_test:
	$(MAKE) TNU_TESTMODE="dev_env_access" test

all_test: 
	$(MAKE) TNU_TESTMODE="workspace_access controlled_access" test
	$(MAKE) dev_scripts/test_installed_cli.py

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

dev_scripts/test_installed_cli.py:
	python dev_scripts/test_installed_cli.py

dev_scripts/test_notebook.py:
	herzog dev_scripts/test_notebook.py | gsutil cp - gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10/notebooks/tnu-tests.ipynb

# Test the wdl script locally using miniwdl
wdl_tests/copy_batch.wdl:
	miniwdl run --verbose --copy-input-files wdl_tests/copy_batch.wdl --input wdl_tests/copy_batch_input.json

version: terra_notebook_utils/version.py

terra_notebook_utils/version.py: setup.py
	echo "__version__ = '$$(python setup.py --version)'" > $@

build: version
	python setup.py bdist_wheel

sdist:
	python setup.py sdist

install: build
	pip install --upgrade dist/*.whl

.PHONY: test lint mypy tests build install dev_scripts/test_installed_cli.py dev_scripts/test_notebook.py wdl_tests/copy_batch.wdl
