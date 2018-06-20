class APIException(IOError):
    """
    The base exception class for the rest_api package.
    """
    pass


class InvalidTokenError(APIException):
    """
    An invalid authentication token was provided.

    The authentication token is either malformed or is not associated with an account.
    """
    pass
