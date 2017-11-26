import json
import pprint

import requests

from . import log

_BASE_URL = 'http://52.173.92.238/api/v1'
_ARTIFACTS_RESOURCE = 'artifacts'
_EMAIL_SUBSCRIBERS_RESOURCE = 'emailSubscribers'


###################################
# Artifact REST methods
###################################

def insert_artifact(artifact):
    return _insert(artifact, _artifacts_endpoint(), 'artifact')


def find_artifact(image_tag):
    if not isinstance(image_tag, str):
        raise ValueError
    log.debug('Trying to find artifact with image_tag', image_tag + '.')
    return _perform_get(_artifact_image_tag_resource(image_tag))


def list_artifacts():
    return _list(_artifacts_endpoint())


def count_artifacts():
    return _count(_artifacts_endpoint())


###################################
# Email Subscriber REST methods
###################################

def insert_email_subscriber(email_subscriber):
    return _insert(email_subscriber, _email_subscribers_endpoint(), 'email subscriber')


def find_email_subscriber(email):
    if not isinstance(email, str):
        raise ValueError
    log.debug('Trying to find email subscriber with email', email + '.')
    return _perform_get(_email_subscriber_email_endpoint(email))


def list_email_subscribers():
    return _list(_email_subscribers_endpoint())


def count_email_subscribers():
    return _count(_email_subscribers_endpoint())


def confirm_email_subscriber(email):
    # Set confirmed to True and clear the confirmation token.
    updates = {'confirmed': True, 'confirmation_token': ''}
    return _perform_patch(_email_subscriber_email_endpoint(email), updates)


###################################
# Convenience REST methods
###################################

def _perform_get(endpoint):
    resp = requests.get(endpoint)
    if not resp.ok:
        log.error(resp.url)
        log.error(resp.content)
    return resp


def _perform_post(endpoint, data):
    headers = {'Content-Type': 'application/json'}
    resp = requests.post(endpoint, json.dumps(data), headers=headers)
    if not resp.ok:
        log.error(resp.url)
        log.error(resp.content)
    return resp


def _perform_patch(endpoint, data):
    # First get the entity's etag.
    etag = _perform_get(endpoint).json()['_etag']
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


def _perform_delete(endpoint):
    resp = requests.delete(endpoint)
    if not resp.ok:
        log.error(resp.url)
        log.error(resp.content)
    return resp


###################################
# Convenience methods
###################################

def _endpoint(resource):
    if resource is None:
        raise ValueError
    return '/'.join([_BASE_URL, resource])


def _insert(entity, endpoint, singular_entity_name='entity'):
    if entity is None:
        raise ValueError
    if endpoint is None:
        raise ValueError
    if singular_entity_name is None:
        raise ValueError
    log.debug('Trying to insert', singular_entity_name + '.')
    resp = _perform_post(endpoint, entity)
    if resp.status_code == 422:
        log.error('The', singular_entity_name, 'was not added because it failed validation.')
        log.error(pprint.pformat(entity))
        log.error(resp.content)
        return False
    return True


# TODO: Handle pagination.
def _list(endpoint):
    if endpoint is None:
        raise ValueError
    return _perform_get(endpoint)


def _count(endpoint):
    if endpoint is None:
        raise ValueError
    resp = _perform_get(endpoint)
    result = resp.json()
    if result is not None and '_meta' in result and 'total' in result['_meta']:
        return result['_meta']['total']
    return -1


def _artifacts_endpoint():
    return _endpoint(_ARTIFACTS_RESOURCE)


def _artifact_image_tag_resource(image_tag):
    if not isinstance(image_tag, str):
        raise ValueError
    return '/'.join([_artifacts_endpoint(), image_tag])


def _email_subscribers_endpoint():
    return _endpoint(_EMAIL_SUBSCRIBERS_RESOURCE)


def _email_subscriber_email_endpoint(email):
    if not isinstance(email, str):
        raise ValueError
    return '/'.join([_email_subscribers_endpoint(), email])
