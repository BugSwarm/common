import subprocess

from typing import Optional


class ShellWrapper(object):

    Command = str
    ReturnCode = int
    StreamOr = Optional[str]

    @staticmethod
    def run_commands(*commands: Command, **kwargs) -> (StreamOr, StreamOr, ReturnCode):
        """
        Run a sequence of commands in a shell environment.

        :param commands: Strings that represent commands to run.
        :param kwargs: Keyword arguments that are passed to the Popen constructor. By default, stdout and stderr are
                       subprocess.PIPE and shell is True.
        :return: A 3-tuple of the subprocess' stdout stream, stderr stream, and return code. The first two members of
                 the tuple can be None depending on the values of stdout and stderr passed to run_commands.
        """
        command = ' ; '.join(commands)
        kwargs.setdefault('stdout', subprocess.PIPE)
        kwargs.setdefault('stderr', subprocess.PIPE)
        kwargs.setdefault('shell', True)
        process = subprocess.Popen(command, **kwargs)
        stdout, stderr = process.communicate()
        if stdout is not None:
            stdout = str(stdout, 'utf-8')
        if stderr is not None:
            stderr = str(stderr, 'utf-8')
        return stdout, stderr, process.returncode
