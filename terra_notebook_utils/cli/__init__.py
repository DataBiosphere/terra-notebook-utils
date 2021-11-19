"""
Configure the CLI
"""
"""Configure the CLI and CLI dispatch using the cli-builder package:
https://github.com/xbrianh/cli-builder

This cannot be combined with the main CLI entrypoint module due to circular imports. Command groups must import the
dispatcher to access cli-builder decorators.
"""
import os
import json
from typing import Optional, Tuple

import cli_builder

from terra_notebook_utils import version, WORKSPACE_NAME, WORKSPACE_NAMESPACE
from terra_notebook_utils.blobstore.progress import Indicator


class CLIConfig:
    info = dict(workspace=None, workspace_namespace=None, copy_progress_indicator_type="auto")
    path = os.path.join(os.path.expanduser("~"), ".tnu_config")

    @classmethod
    def progress_indicator_type(cls) -> Optional[Indicator]:
        val = CLIConfig.info['copy_progress_indicator_type']
        if "log" == val:
            return Indicator.log
        elif "auto" == val:
            return None
        else:
            raise ValueError(f"Unsupported copy progress indicator type '{val}'")

    @classmethod
    def load(cls):
        if os.path.isfile(cls.path):
            with open(cls.path) as fh:
                cls.info.update(json.loads(fh.read()))

    @classmethod
    def write(cls):
        with open(cls.path, "w") as fh:
            fh.write(json.dumps(cls.info, indent=2))

    @classmethod
    def resolve(cls, override_workspace: Optional[str], override_namespace: Optional[str]) -> Tuple[str, str]:
        workspace = override_workspace or (cls.info['workspace'] or WORKSPACE_NAME)
        namespace = override_namespace or (cls.info['workspace_namespace'] or WORKSPACE_NAMESPACE)
        if not workspace:
            raise RuntimeError("This command requires a workspace. Either pass in with `--workspace`,"
                               " or configure the CLI (see `tnu config --help`). A default may also be configured by"
                               " setting the `WORKSPACE_NAME` env var")
        if namespace is None:
            from terra_notebook_utils.workspace import get_workspace_namespace
            namespace = get_workspace_namespace(workspace)
        if not namespace:
            raise RuntimeError("This command requires a workspace namespace. "
                               "Either pass in with `--workspace-namespace`, "
                               "or configure the CLI (see `tnu config --help`). A default may also be"
                               " configured by setting the `WORKSPACE_NAMESPACE` env var")
        return workspace, namespace
CLIConfig.load()

descr = f"""
    Welcome to the terra-notebook-utils cli, version {version.__version__}
             ___
           _(((,|    What's DNA??
          /  _-\\\\
         / C o\o \\
       _/_    __\ \\     __ __     __ __     __ __     __
      /   \ \___/  )   /--X--\   /--X--\   /--X--\   /--
      |    |\_|\  /   /--/ \--\ /--/ \--\ /--/ \--\ /--/
      |    |#  #|/          \__X__/   \__X__/   \__X__/
      (   /     |
       |  |#  # |
       |  |    #|
       |  | #___n_,_
    ,-/   7-' .     `\\
    `-\...\-_   -  o /
       |#  # `---U--'
       `-v-^-'\/
         \  |_|_ Wny
         (___mnnm
"""
dispatch = cli_builder.Dispatch(description=descr, debug=os.environ.get('TNU_CLI_DEBUG', False))
