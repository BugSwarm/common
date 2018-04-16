import json
import pprint
from typing import Dict
from typing import List
from urllib.parse import urljoin

import requests
from requests import Response

from . import log

_BASE_URL = 'http://52.173.92.238/api/v1'
_ARTIFACTS_RESOURCE = 'artifacts'
_MINED_BUILD_PAIRS_RESOURCE = 'minedBuildPairs'
_MINED_PROJECTS_RESOURCE = 'minedProjects'
_EMAIL_SUBSCRIBERS_RESOURCE = 'emailSubscribers'

Endpoint = str


###################################
# Artifact REST methods
###################################

def insert_artifact(artifact) -> Response:
    return _insert(_artifacts_endpoint(), artifact, 'artifact')


def find_artifact(image_tag: str, error_if_not_found: bool = True) -> Response:
    log.debug('Trying to find artifact with image_tag {}.'.format(image_tag))
    return _get(_artifact_image_tag_endpoint(image_tag), error_if_not_found)


def list_artifacts() -> List:
    return _list(_artifacts_endpoint())


def filter_artifacts(api_filter: str) -> List:
    return _filter(_artifacts_endpoint(), api_filter)


def count_artifacts() -> int:
    return _count(_artifacts_endpoint())


def upsert_artifact(artifact) -> Response:
    image_tag = artifact.get('image_tag')
    assert image_tag
    return _upsert(_artifact_image_tag_endpoint(image_tag), artifact, 'artifact')


def set_artifact_metric(image_tag: str, metric_name: str, metric_value) -> Response:
    """
    Add a metric to an existing artifact.
    The value of the metric can be any valid database type.

    If the metric with name `metric_name` already exists, then its value will be overridden with `metric_value`.
    Otherwise, the metric will be created with value `metric_value`.

    :param image_tag: The image tag identifying the artifact to update.
    :param metric_name: The name of the metric to add or update.
    :param metric_value: The new value of the metric.
    :return: The response object.
    """
    if not isinstance(image_tag, str):
        raise TypeError
    if not image_tag:
        raise ValueError
    if not isinstance(metric_name, str):
        raise TypeError
    if not metric_name:
        raise ValueError
    updates = {'metrics.{}'.format(metric_name): metric_value}
    return _patch(_artifact_image_tag_endpoint(image_tag), updates)


###################################
# Mined Build Pair REST methods
###################################

def insert_mined_build_pair(mined_build_pair) -> Response:
    return _insert(_mined_build_pairs_endpoint(), mined_build_pair, 'mined build pair')


def find_mined_build_pair(object_id: str, error_if_not_found: bool = True) -> Response:
    log.debug('Trying to find mined build pairs for ObjectId {}.'.format(object_id))
    return _get(_mined_build_pair_object_id_endpoint(object_id), error_if_not_found)


def list_mined_build_pairs() -> List:
    return _list(_mined_build_pairs_endpoint())


def filter_mined_build_pairs(api_filter: str) -> List:
    return _filter(_mined_build_pairs_endpoint(), api_filter)


def count_mined_build_pairs() -> int:
    return _count(_mined_build_pairs_endpoint())


def filter_mined_build_pairs_for_repo(repo: str) -> List:
    """
    Returns a list of build pairs mined from `repo`. Returns an empty list if no build pairs for `repo` are found or if
    `repo` has not yet been mined.
    """
    return _filter(_mined_build_pairs_endpoint(), '{{"repo": "{}"}}'.format(repo))


def remove_mined_build_pairs_for_repo(repo: str) -> bool:
    """
    Non-atomically remove the existing mined build pairs for `repo`.

    If your goal is to replace mined build pairs for `repo`, you should use `replace_mined_build_pairs_for_repo`.
    """
    for bp in filter_mined_build_pairs_for_repo(repo):
        if not _delete(_mined_build_pair_object_id_endpoint(bp['_id'])):
            log.error('Could not remove an existing mined build pair for {}.'.format(repo))
            return False
    return True


def replace_mined_build_pairs_for_repo(repo: str, new_build_pairs: List[Dict]) -> bool:
    """
    Non-atomically remove the existing mined build pairs for `repo` and then non-atomically insert the newly mined build
    pairs for `repo` in `new_build_pairs`.
    """
    if not remove_mined_build_pairs_for_repo(repo):
        return False
    for bp in new_build_pairs:
        if not insert_mined_build_pair(bp):
            log.error('While replacing mined build pairs, an insertion failed.')
            return False
    return True


###################################
# Mined Project REST methods
###################################

def insert_mined_project(mined_project) -> Response:
    return _insert(_mined_projects_endpoint(), mined_project, 'mined project')


def find_mined_project(repo: str, error_if_not_found: bool = True) -> Response:
    log.debug('Trying to find mined project with repo {}.'.format(repo))
    return _get(_mined_project_repo_endpoint(repo), error_if_not_found)


def list_mined_projects() -> List:
    return _list(_mined_projects_endpoint())


def filter_mined_projects(api_filter: str) -> List:
    return _filter(_mined_projects_endpoint(), api_filter)


def count_mined_projects() -> int:
    return _count(_mined_projects_endpoint())


def upsert_mined_project(mined_project) -> Response:
    """
    Upsert a mined project. Can be used for initial mining or re-mining of a project.
    """
    repo = mined_project.get('repo')
    assert repo
    return _upsert(_mined_project_repo_endpoint(repo), mined_project, 'mined project')


def set_mined_project_progression_metric(repo: str, metric_name: str, metric_value) -> Response:
    """
    Add a mining progression metric to an existing mined project.
    The value of the metric can be any valid database type.

    If the metric with name `metric_name` already exists, then its value will be overridden with `metric_value`.
    Otherwise, the metric will be created with value `metric_value`.

    :param repo: The repository slug identifying the mined project to update.
    :param metric_name: The name of the metric to add or update.
    :param metric_value: The new value of the metric.
    :return: The response object.
    """
    if not isinstance(repo, str):
        raise TypeError
    if not repo:
        raise ValueError
    if not isinstance(metric_name, str):
        raise TypeError
    if not metric_name:
        raise ValueError
    updates = {'progression_metrics.{}'.format(metric_name): metric_value}
    return _patch(_mined_project_repo_endpoint(repo), updates)


###################################
# Email Subscriber REST methods
###################################

def insert_email_subscriber(email_subscriber) -> Response:
    return _insert(_email_subscribers_endpoint(), email_subscriber, 'email subscriber')


def find_email_subscriber(email: str, error_if_not_found: bool = True) -> Response:
    log.debug('Trying to find email subscriber with email {}.'.format(email))
    return _get(_email_subscriber_email_endpoint(email), error_if_not_found)


def list_email_subscribers() -> List:
    return _list(_email_subscribers_endpoint())


def filter_email_subscribers(api_filter: str) -> List:
    return _filter(_email_subscribers_endpoint(), api_filter)


def count_email_subscribers() -> int:
    return _count(_email_subscribers_endpoint())


def confirm_email_subscriber(email: str) -> Response:
    # Set confirmed to True and clear the confirmation token.
    updates = {'confirmed': True, 'confirm_token': ''}
    return _patch(_email_subscriber_email_endpoint(email), updates)


def unsubscribe_email_subscriber(email: str) -> Response:
    return _delete(_email_subscriber_email_endpoint(email))


###################################
# Convenience REST methods
###################################

def _get(endpoint: Endpoint, error_if_not_found: bool = True) -> Response:
    if not isinstance(endpoint, Endpoint):
        raise TypeError
    if not endpoint:
        raise ValueError
    resp = requests.get(endpoint)
    # Do not print an error message if the entity was not expected to be found and we got a 404 status code.
    not_found = resp.status_code == 404
    if not_found and not error_if_not_found:
        return resp
    elif not resp.ok:
        log.error(resp.url)
        log.error(resp.content)
    return resp


def _post(endpoint: Endpoint, data) -> Response:
    if not isinstance(endpoint, Endpoint):
        raise TypeError
    if not endpoint:
        raise ValueError
    headers = {'Content-Type': 'application/json'}
    resp = requests.post(endpoint, json.dumps(data), headers=headers)
    if not resp.ok:
        log.error(resp.url)
        log.error(resp.content)
    return resp


def _patch(endpoint: Endpoint, data) -> Response:
    if not isinstance(endpoint, Endpoint):
        raise TypeError
    if not endpoint:
        raise ValueError
    # First get the entity's etag.
    etag = _get(endpoint).json()['_etag']
    # Now patch the entity.
    headers = {
        'Content-Type': 'application/json',
        'If-Match': etag,
    }
    resp = requests.patch(endpoint, json.dumps(data), headers=headers)
    if not resp.ok:
        log.error(resp.url)
        log.error(resp.content)
    return resp


def _put(endpoint: Endpoint, data, etag: str = None) -> Response:
    if not isinstance(endpoint, Endpoint):
        raise TypeError
    if not endpoint:
        raise ValueError
    headers = {'Content-Type': 'application/json'}
    if etag:
        headers['If-Match'] = etag
    # Now replace the entity.
    resp = requests.put(endpoint, json.dumps(data), headers=headers)
    if not resp.ok:
        log.error(resp.url)
        log.error(resp.content)
    return resp


def _delete(endpoint: Endpoint) -> Response:
    if not isinstance(endpoint, Endpoint):
        raise TypeError
    if not endpoint:
        raise ValueError
    # First get the entity's etag.
    etag = _get(endpoint).json()['_etag']
    headers = {'If-Match': etag}
    # Now delete the entity.
    resp = requests.delete(endpoint, headers=headers)
    if not resp.ok:
        log.error(resp.url)
        log.error(resp.content)
    return resp


###################################
# Convenience methods
###################################

def _endpoint(resource: str) -> Endpoint:
    if not isinstance(resource, str):
        raise TypeError
    if not resource:
        raise ValueError
    return '/'.join([_BASE_URL, resource])


def _insert(endpoint: Endpoint, entity, singular_entity_name: str = 'entity') -> Response:
    if entity is None:
        raise TypeError
    if not isinstance(endpoint, Endpoint):
        raise TypeError
    if not endpoint:
        raise ValueError
    if not isinstance(singular_entity_name, str):
        raise TypeError
    if not singular_entity_name:
        raise ValueError
    log.debug('Trying to insert {}.'.format(singular_entity_name))
    resp = _post(endpoint, entity)
    if resp.status_code == 422:
        log.error('The', singular_entity_name, 'was not inserted because it failed validation.')
        log.error(pprint.pformat(entity))
        log.error(resp.content)
    return resp


def _upsert(endpoint: Endpoint, entity, singular_entity_name: str = 'entity') -> Response:
    if entity is None:
        raise TypeError
    if not isinstance(endpoint, Endpoint):
        raise TypeError
    if not endpoint:
        raise ValueError
    if not isinstance(singular_entity_name, str):
        raise TypeError
    if not singular_entity_name:
        raise ValueError
    log.debug('Trying to upsert {}.'.format(singular_entity_name))
    # First, check if the entity exists. If it does, then pass its etag.
    get_resp = _get(endpoint, error_if_not_found=False)
    etag = get_resp.json()['_etag'] if get_resp.ok else None
    resp = _put(endpoint, entity, etag)
    if resp.status_code == 422:
        log.error('The', singular_entity_name, 'was not upserted because it failed validation.')
        log.error(pprint.pformat(entity))
        log.error(resp.content)
    return resp


# Returns a list of all the results by following the next link chain starting with start_link.
def _iter_pages(start_link: str) -> List:
    if not isinstance(start_link, str):
        raise TypeError
    if not start_link:
        raise ValueError
    results = []
    next_link = start_link
    while next_link:
        next_json = _get(next_link).json()
        try:
            results += next_json['_items']
        except KeyError:
            break
        try:
            next_link = urljoin(next_link, next_json['_links']['next']['href'])
        except KeyError:
            break
    return results


# Returns all results from the current page to the last page, inclusive.
def _list(endpoint: Endpoint) -> List:
    if not isinstance(endpoint, Endpoint):
        raise TypeError
    if not endpoint:
        raise ValueError
    return _iter_pages(endpoint)


def _filter(endpoint: Endpoint, api_filter: str) -> List:
    if not isinstance(endpoint, Endpoint):
        raise TypeError
    if not endpoint:
        raise ValueError
    if not isinstance(api_filter, str):
        raise TypeError
    if not api_filter:
        raise ValueError
    # Append the filter as a url parameter.
    url = '{}?where={}'.format(endpoint, api_filter)
    return _iter_pages(url)


def _count(endpoint: Endpoint) -> int:
    if not isinstance(endpoint, Endpoint):
        raise TypeError
    if not endpoint:
        raise ValueError
    resp = _get(endpoint)
    result = resp.json()
    try:
        total = result['_meta']['total']
        assert isinstance(total, int)
        return total
    except KeyError:
        return -1


def _artifacts_endpoint() -> Endpoint:
    return _endpoint(_ARTIFACTS_RESOURCE)


def _artifact_image_tag_endpoint(image_tag: str) -> Endpoint:
    if not isinstance(image_tag, str):
        raise TypeError
    if not image_tag:
        raise ValueError
    return '/'.join([_artifacts_endpoint(), image_tag])


def _mined_build_pairs_endpoint() -> Endpoint:
    return _endpoint(_MINED_BUILD_PAIRS_RESOURCE)


def _mined_build_pair_object_id_endpoint(object_id: str) -> Endpoint:
    if not isinstance(object_id, str):
        raise TypeError
    if not object_id:
        raise ValueError
    return '/'.join([_mined_build_pairs_endpoint(), object_id])


def _mined_projects_endpoint() -> Endpoint:
    return _endpoint(_MINED_PROJECTS_RESOURCE)


def _mined_project_repo_endpoint(repo: str) -> Endpoint:
    if not isinstance(repo, str):
        raise TypeError
    if not repo:
        raise ValueError
    return '/'.join([_mined_projects_endpoint(), repo])


def _email_subscribers_endpoint() -> Endpoint:
    return _endpoint(_EMAIL_SUBSCRIBERS_RESOURCE)


def _email_subscriber_email_endpoint(email: str) -> Endpoint:
    if not isinstance(email, str):
        raise TypeError
    if not email:
        raise ValueError
    return '/'.join([_email_subscribers_endpoint(), email])
