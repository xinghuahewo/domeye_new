"""H1 owner清理：全部尝试释放，保留主异常及独立清理错误。"""
from contextlib import contextmanager


def release(resource, label, primary=None, errors=None):
    if resource is None:
        return primary
    try:
        resource.close()
    except BaseException as error:
        detail = {'resource': label, 'type': type(error).__name__, 'message': str(error)}
        if errors is not None:
            errors.append(detail)
        if primary is None:
            primary = error
        previous = list(getattr(primary, 'cleanup_errors', ()))
        previous.append(detail)
        primary.cleanup_errors = previous
        if hasattr(primary, 'add_note'):
            primary.add_note('H1 cleanup_errors: ' + str(detail))
    return primary


@contextmanager
def owner(resource, label='H1 owner', errors=None):
    primary = None
    try:
        yield resource
    except BaseException as error:
        primary = error
        raise
    finally:
        cleanup = release(resource, label, primary, errors)
        if primary is None and cleanup is not None:
            raise cleanup
