"""
This file provides an interface to build the TNU operations CLI.

Commands are defined with a target-action model, where targets represent command groups. Arguments
can be configured for both targets and actions.

Example:
```
dispatch = TNUCommandDispatch()

vcf_cli = dispatch.target("vcf")

@storage.action("head", arguments={"path": dict(help="location of VCF, either local, gs://path, or drs://path"),
                                   "--billing-project": dict(type=str, required=False)})
def head(args):
    pass

# execution:
scripts/tnu vcf head --billing-project my_billing_project
```
"""
import os
import argparse
from argparse import RawTextHelpFormatter
import traceback


class _target:
    def __init__(self, target_name, dispatcher):
        self.target_name = target_name
        self.dispatcher = dispatcher

    def action(self, name: str, *, arguments: dict=None, mutually_exclusive: list=None):
        dispatcher = self.dispatcher
        arguments = arguments or dict()
        if mutually_exclusive is None:
            mutually_exclusive = dispatcher.targets[self.target_name]['mutually_exclusive'] or list()

        def register_action(obj):
            parser = dispatcher.targets[self.target_name]['subparser'].add_parser(
                name,
                help=obj.__doc__,
                formatter_class=RawTextHelpFormatter
            )
            action_arguments = dispatcher.targets[self.target_name]['arguments'].copy()
            action_arguments.update(arguments)
            for argname, kwargs in action_arguments.items():
                if argname not in mutually_exclusive:
                    parser.add_argument(argname, **(kwargs or dict()))
            if mutually_exclusive:
                group = parser.add_mutually_exclusive_group(required=True)
                for argname in mutually_exclusive:
                    kwargs = action_arguments.get(argname) or dict()
                    group.add_argument(argname, **kwargs)
            parser.set_defaults(func=obj)
            dispatcher.actions[obj] = dict(target=dispatcher.targets[self.target_name], name=name)
            return obj
        return register_action

class TNUCommandDispatch:
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
    targets: dict = dict()
    actions: dict = dict()

    def __init__(self):
        self.parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=RawTextHelpFormatter)
        self.parser_targets = self.parser.add_subparsers()

    def target(self, name: str, *, arguments: dict=None, mutually_exclusive: list=None, help=None):
        arguments = arguments or dict()
        target = self.parser_targets.add_parser(name, help=help)
        self.targets[name] = dict(subparser=target.add_subparsers(),
                                  arguments=arguments,
                                  mutually_exclusive=mutually_exclusive)
        return _target(name, self)

    def __call__(self, argv):
        try:
            args = self.parser.parse_args(argv)
            action = args.func
            try:
                action(args)
            except Exception:
                print(traceback.format_exc())
        except SystemExit:
            pass
        except AttributeError:
            # TODO: dump error message when executed in Lambda
            self.parser.print_help()

dispatch = TNUCommandDispatch()
