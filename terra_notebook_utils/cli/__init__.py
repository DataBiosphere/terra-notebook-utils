"""
Configure the CLI
"""
import os
import json
from typing import Optional, Tuple
from terra_notebook_utils import version

import cli_builder

from terra_notebook_utils import WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT


class Config:
    info = dict(workspace=None, workspace_namespace=None)
    path = os.path.join(os.path.expanduser("~"), ".tnu_config")

    @classmethod
    def load(cls):
        if not os.path.isfile(cls.path):
            cls.write()
        with open(cls.path) as fh:
            cls.info = json.loads(fh.read())

    @classmethod
    def write(cls):
        with open(cls.path, "w") as fh:
            fh.write(json.dumps(cls.info, indent=2))

    @classmethod
    def resolve(cls, override_workspace: Optional[str], override_namespace: Optional[str]) -> Tuple[str, str]:
        workspace = override_workspace or (cls.info['workspace'] or WORKSPACE_NAME)
        namespace = override_namespace or (cls.info['workspace_namespace'] or WORKSPACE_GOOGLE_PROJECT)
        if not workspace:
            raise RuntimeError("This command requies a workspace. Either pass in with `--workspace`,"
                               " or configure the CLI (see `tnu config --help`). A default may also be configured by"
                               " setting the `WORKSPACE_NAME` env var")
        if namespace is None:
            from terra_notebook_utils.workspace import get_workspace_namespace
            namespace = get_workspace_namespace(workspace)
        if not namespace:
            raise RuntimeError("This command requies a workspace namespace. Either pass in with `--workspace-namespace`"
                               ", or configure the CLI (see `tnu config --help`). A default may also be"
                               " configued by setting the `WORKSPACE_GOOGLE_PROJECT` env var")
        return workspace, namespace
Config.load()


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
