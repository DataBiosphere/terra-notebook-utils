"""
Configure the CLI
"""
import os
import json
import typing

import cli_builder

from terra_notebook_utils import WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT


class Config:
    info = dict(workspace=None, workspace_google_project=None)
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
    def resolve(cls, override_workspace: str, override_namespace: str):
        workspace = override_workspace or (cls.info['workspace'] or WORKSPACE_NAME)
        namespace = override_namespace or (cls.info['workspace_google_project'] or WORKSPACE_GOOGLE_PROJECT)
        if workspace and namespace is None:
            from terra_notebook_utils.workspace import get_workspace_namespace
            namespace = get_workspace_namespace(workspace)
        if not workspace:
            raise RuntimeError("This command requies a workspace. Either pass in a workspace with `--workspace`,"
                               " or configure a default workspace for the cli (see `tnu config --help`)."
                               " A default workspace may also be configued by setting the `WORKSPACE_NAME` env var")
        return workspace, namespace
Config.load()


dispatch = cli_builder.Dispatch(
    """
    Welcome to the terra-notebook-utils cli
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
)
