import importlib
import click
import sys
from lib.helpers.logs import LogHelper
from pathlib import Path


@click.group(help='MC SDK SAMPLE CATALOG', context_settings=dict(help_option_names=["-h", "--help"]))
@click.pass_context
def main_module(cmd):
    pass


def bind_function(name):

    def func(args):
        call = importlib.import_module(f"{module}.{submodule.replace('-', '_')}")
        try:
            call.main(args)
        except AttributeError as e:
            click.echo(click.style(f"The '{submodule}' command is not supported yet", fg='red'))

    func.__name__ = name
    return func


if __name__ == '__main__':

    # Define folders to track
    modules = {'admin': {'description': 'Admin related operations and utilities.'},
               'tables': {'description': 'Collection of actions and utilities around tables/views'},
               'monitors': {'description': 'Collection of actions and utilities for MC monitors.'},
               'lineage': {'description': 'Collection of actions and utilities around lineage'},}

    for module in modules:
        subpaths = sorted(Path(module).glob('[!__]*.py'))

        @click.command(name=module, help=modules[module]['description'],
                       context_settings=dict(help_option_names=["-h", "--help"]))
        def command():
            pass

        main_module.add_command(command)

        if len(sys.argv) > 1:
            if module == sys.argv[1]:

                @click.group(name=module, help=modules[module]['description'],
                             context_settings=dict(help_option_names=["-h", "--help"]))
                def main_submodule():
                    pass


                LogHelper.banner()
                for path in subpaths:
                    submodule = str(path).split('/')[-1].replace('.py', '').replace('_', '-')
                    script = bind_function(f'_{submodule}')

                    @click.command(name=submodule, context_settings=dict(help_option_names=["-h", "--help"]))
                    def subcommand():
                        pass

                    if len(sys.argv) >= 3:
                        if submodule == sys.argv[2]:
                            script(sys.argv[3:])
                            exit(0)

                    main_submodule.add_command(subcommand)

                main_module.add_command(main_submodule)
                main_module(max_content_width=120)

            else:
                continue

    LogHelper.banner()
    main_module(max_content_width=120)
