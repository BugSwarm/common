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
    if artifact is None:
        raise ValueError
    log.debug('Trying to insert artifact.')
    resp = _perform_post(_artifacts_endpoint(), artifact)
    if resp.status_code == 422:
        log.error('The artifact was not added because it failed validation.')
        log.error(pprint.pformat(artifact))
        return False
    return True


def find_artifact(image_tag):
    if not isinstance(image_tag, str):
        raise ValueError
    log.debug('Trying to find artifact with image_tag', image_tag + '.')
    return _perform_get(_artifact_image_tag_resource(image_tag))


def list_artifacts():
    return _perform_get(_artifacts_endpoint())


def count_artifacts():
    raise NotImplementedError


###################################
# Email Subscriber REST methods
###################################

def insert_email_subscriber(email_subscriber):
    if email_subscriber is None:
        raise ValueError
    log.debug('Trying to insert email subscriber.')
    resp = _perform_post(_email_subscribers_endpoint(), email_subscriber)
    if resp.status_code == 422:
        log.error('The email subscriber was not added because it failed validation.')
        log.error(pprint.pformat(email_subscriber))
        return False
    return True


def find_email_subscriber(email):
    if not isinstance(email, str):
        raise ValueError
    log.debug('Trying to find email subscriber with email', email + '.')
    return _perform_get(_email_subscriber_email_endpoint(email))


def list_email_subscribers():
    return _perform_get(_email_subscribers_endpoint())


def count_email_subscribers():
    raise NotImplementedError


###################################
# Convenience REST methods
###################################

def _perform_get(endpoint):
    resp = requests.get(endpoint)
    log.debug(resp.url)
    log.debug(resp.content)
    return resp


def _perform_post(endpoint, data):
    headers = {'Content-Type': 'application/json'}
    resp = requests.post(endpoint, json.dumps(data), headers=headers)
    log.debug(resp.url)
    log.debug(resp.content)
    return resp


###################################
# Convenience methods
###################################

def _endpoint(resource):
    return '/'.join([_BASE_URL, resource])


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
