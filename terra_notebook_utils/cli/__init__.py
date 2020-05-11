"""
Configure the CLI
"""
import os
import json
import typing

import cli_builder


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
