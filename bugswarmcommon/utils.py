import subprocess

from .shell_wrapper import ShellWrapper

_CommitSHA = str


def get_current_component_version_message(component_name: str) -> str:
    """
    Get a message that can be logged to indicate the version of the currently executing BugSwarm component.

    :param component_name: The name of the component. This name is not used to obtain the version. Instead, it is merely
                           used to compose the returned message.
    :return: A string that can be logged to indicate the version of the currently executing BugSwarm component.
    """
    version = _get_current_component_version()[:7]
    return 'Using version {} of {}.'.format(version, component_name)


def _get_current_component_version() -> _CommitSHA:
    stdout, _, _ = ShellWrapper.run_commands('git rev-parse HEAD', stdout=subprocess.PIPE, shell=True)
    return stdout
