"""
Configure the CLI
"""
import cli_builder

from terra_notebook_utils import Config


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
