from .shell_wrapper import ShellWrapper

Commit = str


def get_current_component_version() -> Commit:
    stdout, _, _ = ShellWrapper.run_commands('git rev-parse HEAD')
    return stdout[:7]
