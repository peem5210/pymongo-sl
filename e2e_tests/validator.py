import time


def error_log(function):
    def wrapper(*args, **kwargs):
        result = None
        try:
            result = function(*args, **kwargs)
        except AssertionError as e:
            print(f"[ERROR]: Assertion failed: {str(e)}")
        return result
    return wrapper


@error_log
def validate_cursor(timing, func_nt, func_sl, *args, **kwargs):
    """Validation for functions that returns cursor
    """
    try:
        start = time.perf_counter()
        result_nt = [x for x in func_nt(*args, **kwargs)]
        mid = time.perf_counter()
        result_sl = [x for x in func_sl(*args, **kwargs)]
        last = time.perf_counter()
        assert result_nt == result_sl, f"With {args = } and {kwargs = }, the assertion is failed \n " \
                                       f"{result_nt = } should equal to {result_sl = }"
    except AssertionError as e:
        print(f"[ERROR]: Assertion failed: {str(e)}")
    return timing.append((mid - start, last - mid))


@error_log
def validate_document(timing, func_nt, func_sl, *args, **kwargs):
    """Validation for functions that returns single document
    """
    start = time.perf_counter()
    result_nt = func_nt(*args, **kwargs)
    mid = time.perf_counter()
    result_sl = func_sl(*args, **kwargs)
    last = time.perf_counter()
    assert result_nt == result_sl, f"With {args = } and {kwargs = }, the assertion is failed \n " \
                                   f"{result_nt = } should equal to {result_sl = }"
    return timing.append((mid - start, last - mid))


@error_log
def validate_result(timing, undo_func, undo_kwargs, func_nt, func_sl, filter, *args, **kwargs):
    """Validation for functions that returns result
    """
    start = time.perf_counter()
    result_nt = func_nt(filter, *args, **kwargs)
    mid1 = time.perf_counter()
    undo_func(**undo_kwargs)
    mid2 = time.perf_counter()
    result_sl = func_sl(filter, *args, **kwargs)
    last = time.perf_counter()
    undo_func(**undo_kwargs)
    # TODO: add a proper validation to make sure that the updated data is the same,
    #  for now the validation only check for the number of `matched_count` and `modified_count` only
    result_nt = [result_nt.matched_count, result_nt.modified_count]
    result_sl = [result_sl.matched_count, result_sl.modified_count]
    assert result_nt == result_sl, f"With {func_nt.__name__ = }, {func_sl.__name__ = }, {args = } and {kwargs = }, the assertion is failed \n " \
                                   f"{result_nt = } should equal to {result_sl = }"
    return timing.append((mid1 - start, last - mid2))


def validate_args_list(timing, validator, func_nt, func_sl, args_list):
    """Validation function for easily validate same function with different arguments
    """
    for args, kwargs in args_list:
        validator(timing, func_nt, func_sl, *args, **kwargs)


def args(*args, **kwargs):
    return args, kwargs
