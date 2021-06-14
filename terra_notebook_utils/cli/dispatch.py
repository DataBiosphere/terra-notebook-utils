"""Set up the CLI dispatch using command decorators from the cli-builder package:
https://github.com/xbrianh/cli-builder

This cannot be combined with the main CLI entrypoint module due to circular imports. Command groups must import the
dispatcher to access cli-builder decorators.
"""
import os
from terra_notebook_utils import version

import cli_builder


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
