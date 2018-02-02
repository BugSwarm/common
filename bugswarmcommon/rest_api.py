import json
import pprint

from urllib.parse import urljoin

import requests

from . import log

_BASE_URL = 'http://52.173.92.238/api/v1'
_ARTIFACTS_RESOURCE = 'artifacts'
_MINED_PROJECTS_RESOURCE = 'minedProjects'
_EMAIL_SUBSCRIBERS_RESOURCE = 'emailSubscribers'


###################################
# Artifact REST methods
###################################

def insert_artifact(artifact):
    return _insert(artifact, _artifacts_endpoint(), 'artifact')


def find_artifact(image_tag: str, error_if_not_found: bool = True):
    log.debug('Trying to find artifact with image_tag {}.'.format(image_tag))
    return _get(_artifact_image_tag_endpoint(image_tag), error_if_not_found)


def list_artifacts():
    return _list(_artifacts_endpoint())


def filter_artifacts(api_filter: str):
    return _filter(_artifacts_endpoint(), api_filter)


def count_artifacts():
    return _count(_artifacts_endpoint())


###################################
# Mined Project REST methods
###################################

def insert_mined_project(mined_project):
    return _insert(mined_project, _mined_projects_endpoint(), 'mined project')


def find_mined_project(repo: str, error_if_not_found: bool = True):
    log.debug('Trying to find mined project with repo {}.'.format(repo))
    return _get(_mined_projects_repo_endpoint(repo), error_if_not_found)


def list_mined_projects():
    return _list(_mined_projects_endpoint())


def filter_mined_projects(api_filter: str):
    return _filter(_mined_projects_endpoint(), api_filter)


def count_mined_projects():
    return _count(_mined_projects_endpoint())


###################################
# Email Subscriber REST methods
###################################

def insert_email_subscriber(email_subscriber):
    return _insert(email_subscriber, _email_subscribers_endpoint(), 'email subscriber')


def find_email_subscriber(email: str, error_if_not_found: bool = True):
    log.debug('Trying to find email subscriber with email {}.'.format(email))
    return _get(_email_subscriber_email_endpoint(email), error_if_not_found)


def list_email_subscribers():
    return _list(_email_subscribers_endpoint())


def filter_email_subscribers(api_filter: str):
    return _filter(_email_subscribers_endpoint(), api_filter)


def count_email_subscribers():
    return _count(_email_subscribers_endpoint())


def confirm_email_subscriber(email: str):
    # Set confirmed to True and clear the confirmation token.
    updates = {'confirmed': True, 'confirm_token': ''}
    return _patch(_email_subscriber_email_endpoint(email), updates)


def unsubscribe_email_subscriber(email: str):
    return _delete(_email_subscriber_email_endpoint(email))


###################################
# Convenience REST methods
###################################

def _get(endpoint: str, error_if_not_found: bool = True):
    if not isinstance(endpoint, str):
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


def _post(endpoint: str, data):
    if not isinstance(endpoint, str):
        raise TypeError
    if not endpoint:
        raise ValueError
    headers = {'Content-Type': 'application/json'}
    resp = requests.post(endpoint, json.dumps(data), headers=headers)
    if not resp.ok:
        log.error(resp.url)
        log.error(resp.content)
    return resp


def _patch(endpoint: str, data):
    if not isinstance(endpoint, str):
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


def _delete(endpoint: str):
    if not isinstance(endpoint, str):
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

def _endpoint(resource: str):
    if not isinstance(resource, str):
        raise TypeError
    if not resource:
        raise ValueError
    return '/'.join([_BASE_URL, resource])


def _insert(entity, endpoint: str, singular_entity_name: str = 'entity'):
    if entity is None:
        raise TypeError
    if not isinstance(endpoint, str):
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
        log.error('The', singular_entity_name, 'was not added because it failed validation.')
        log.error(pprint.pformat(entity))
        log.error(resp.content)
        return False
    return True


# Returns a list of all the results by following the next link chain starting with start_link.
def _iter_pages(start_link: str):
    if not isinstance(start_link, str):
        raise TypeError
    if not start_link:
        raise ValueError
    results = []
    next_link = start_link
    while next_link:
        next_json = _get(next_link).json()
        results += next_json['_items']
        if not ('_links' in next_json and 'next' in next_json['_links'] and 'href' in next_json['_links']['next']):
            break
        next_link = urljoin(next_link, next_json['_links']['next']['href'])
    return results


# Returns all results from the current page to the last page, inclusive.
def _list(endpoint: str):
    if not isinstance(endpoint, str):
        raise TypeError
    if not endpoint:
        raise ValueError
    return _iter_pages(endpoint)


def _filter(endpoint: str, api_filter: str):
    if not isinstance(endpoint, str):
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


def _count(endpoint: str):
    if not isinstance(endpoint, str):
        raise TypeError
    if not endpoint:
        raise ValueError
    resp = _get(endpoint)
    result = resp.json()
    if result is not None and '_meta' in result and 'total' in result['_meta']:
        return result['_meta']['total']
    return -1


def _artifacts_endpoint():
    return _endpoint(_ARTIFACTS_RESOURCE)


def _artifact_image_tag_endpoint(image_tag: str):
    if not isinstance(image_tag, str):
        raise TypeError
    if not image_tag:
        raise ValueError
    return '/'.join([_artifacts_endpoint(), image_tag])


def _mined_projects_endpoint():
    return _endpoint(_MINED_PROJECTS_RESOURCE)


def _mined_projects_repo_endpoint(repo: str):
    if not isinstance(repo, str):
        raise TypeError
    if not repo:
        raise ValueError
    return '/'.join([_artifacts_endpoint(), repo])


def _email_subscribers_endpoint():
    return _endpoint(_EMAIL_SUBSCRIBERS_RESOURCE)


def _email_subscriber_email_endpoint(email: str):
    if not isinstance(email, str):
        raise TypeError
    if not email:
        raise ValueError
    return '/'.join([_email_subscribers_endpoint(), email])
