"""This is the main CLI entry point."""
import sys

# Only the commands modules imported here are available to the CLI
# These must be imported before dispatch
import terra_notebook_utils.cli.commands.config
import terra_notebook_utils.cli.commands.vcf
import terra_notebook_utils.cli.commands.workspace
import terra_notebook_utils.cli.commands.profile
import terra_notebook_utils.cli.commands.drs
import terra_notebook_utils.cli.commands.table
import terra_notebook_utils.cli.commands.workflows
from terra_notebook_utils.cli import dispatch


def main():
    if 2 == len(sys.argv) and "--version" == sys.argv[1].strip():
        from terra_notebook_utils import version
        print(version.__version__)
    else:
        dispatch(sys.argv[1:])
