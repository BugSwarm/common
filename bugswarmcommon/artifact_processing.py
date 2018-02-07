from concurrent.futures import wait
from concurrent.futures import ThreadPoolExecutor
from typing import List


class ParallelArtifactRunner(object):
    """
    ParallelArtifactRunner is an abstract base class that facilitates parallel processing of BugSwarm artifacts.

    For most use cases, a concrete subclass will define its own process_image_tag method. There, the subclass can
    perform a specialized workflow on each artifact. To facilitate such a workflow, ParallelArtifactRunner provides
    convenience methods and constants for usage by subclasses.
    """
    def __init__(self, image_tags: List[str], max_workers: int = 1):
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
        num_workers = min(self._max_workers, len(self._image_tags))
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_image_tag = {executor.submit(self._thread_main, image_tag): image_tag
                                   for image_tag in self._image_tags}
        wait(future_to_image_tag)

    def _thread_main(self, image_tag):
        return self.process_image_tag(image_tag)

    def process_image_tag(self, image_tag):
        """
        Subclasses must override this method to process each item in the workload.
        :param image_tag: The image tag to process.
        """
        raise NotImplementedError
