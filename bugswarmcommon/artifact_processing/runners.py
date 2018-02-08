from concurrent.futures import wait
from concurrent.futures import ThreadPoolExecutor
from typing import List


class ParallelArtifactRunner(object):
    """
    ParallelArtifactRunner is an abstract base class that facilitates parallel processing of BugSwarm artifacts.

    A concrete subclass must override the process_artifact method where it can perform some operation on each artifact.
    A concrete subclass may also override the following methods:
    - pre_run:   Called before processing any artifacts
    - post_run:  Called after processing all artifacts
    """

    def __init__(self, image_tags: List[str], *, max_workers: int = 1):
        """
        :param image_tags: A list of image tags representing BugSwarm artifacts.
        :param max_workers: The maximum number of worker threads to spawn, clamped to (0, length of `image_tags`].
                            Defaults to 1, in which case each image tag is processed sequentially.
        """
        if not image_tags:
            raise ValueError
        if max_workers <= 0:
            raise ValueError
        self._image_tags = image_tags
        self._max_workers = max_workers

    def run(self):
        """
        Start processing image tags.

        Overriding is forbidden.
        """
        self.pre_run()
        num_workers = min(self._max_workers, len(self._image_tags))
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_image_tag = {executor.submit(self._thread_main, image_tag): image_tag
                                   for image_tag in self._image_tags}
        wait(future_to_image_tag)
        self.post_run()

    def _thread_main(self, image_tag):
        return self.process_artifact(image_tag)

    def process_artifact(self, image_tag):
        """
        Subclasses must override this method to process each item in the workload.
        :param image_tag: The image tag representing the artifact to process.
        """
        raise NotImplementedError

    def pre_run(self):
        """
        Called before any items have been processed.

        Overriding is optional. Defaults to no-op.
        """
        pass

    def post_run(self):
        """
        Called after all items have been processed.

        Overriding is optional. Defaults to no-op.
        """
        pass
