class UserServiceException(Exception):
    pass


class ResourceNotFoundException(UserServiceException):
    pass


class MultipleResourcesFoundException(UserServiceException):
    pass
