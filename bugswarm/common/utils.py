import subprocess

from typing import Union

from .shell_wrapper import ShellWrapper

_CommitSHA = str


def get_image_tag(repo: str, failed_job_id: Union[str, int]) -> str:
    """
    Construct the unique image tag identifying the pair with the given repository slug and failed job ID.

    :param repo: A repository slug.
    :param failed_job_id: A failed job ID for the pair for which the image tag will represent.
    :return: An image tag.
    """
    if not isinstance(repo, str):
        raise TypeError
    if not (isinstance(failed_job_id, str) or isinstance(failed_job_id, int)):
        raise TypeError
    if repo.count('/') != 1:
        raise ValueError('The repository slug should contain exactly one slash.')
    return '{}-{}'.format(repo.replace('/', '-'), failed_job_id)


def get_current_component_version_message(component_name: str) -> str:
    """
    Get a message that can be logged to indicate the version of the currently executing BugSwarm component.

    :param component_name: The name of the component. This name is not used to obtain the version. Instead, it is merely
                           used to compose the returned message.
    :return: A string that can be logged to indicate the version of the currently executing BugSwarm component.
    """
    if not isinstance(component_name, str):
        raise TypeError
    version = _get_current_component_version()[:7]
    return 'Using version {} of {}.'.format(version, component_name)


def _get_current_component_version() -> _CommitSHA:
    stdout, _, _ = ShellWrapper.run_commands('git rev-parse HEAD', stdout=subprocess.PIPE, shell=True)
    return stdout
