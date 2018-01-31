import subprocess

from typing import Optional


class ShellWrapper(object):

    Command = str
    ReturnCode = int
    StreamOr = Optional[str]

    @staticmethod
    def run_commands(*commands: Command, **kwargs) -> (StreamOr, StreamOr, ReturnCode):
        """
        Run a list of commands sequentially in the same shell environment and wait for the commands to complete.
        All keyword arguments are passed to the subprocess.Popen constructor.

        :param commands: Strings that represent commands to run.
        :param kwargs: Keyword arguments that are passed to the Popen constructor.
        :return: A 3-tuple of the subprocess' stdout stream, stderr stream, and return code. The streams in the tuple
                 can be None depending on the passed values of `stdout` and `stderr`.
        """
        command = ' ; '.join(commands)
        process = subprocess.Popen(command, **kwargs)
        stdout, stderr = process.communicate()  # Indirectly wait for a return code.
        if stdout is not None:
            stdout = str(stdout, 'utf-8').strip()
        if stderr is not None:
            stderr = str(stderr, 'utf-8').strip()
        return stdout, stderr, process.returncode
