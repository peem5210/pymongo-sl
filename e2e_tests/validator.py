import time


def metric_log(function):
    def wrapper(*args, **kwargs):
        nt = sl = None
        try:
            nt, sl = function(*args, **kwargs)
        except AssertionError as e:
            print("[ERROR]: Assertion failed: " + str(e))
        print("[INFO] gained " + format(sl - nt, '.5f') + " sec with function: " + args[1].__name__)
    return wrapper

TEST_ARGS = ["same_region"]

@metric_log
def validate_cursor(timing,  func_nt, func_sl, *args,  **kwargs):
    """Validation for functions that returns cursor
    """
    temp = {}
    for arg in TEST_ARGS:
        if arg in kwargs:
            temp[arg] = kwargs.pop(arg)
    start = time.time()
    result_nt = [x for x in func_nt(*args, **kwargs)]
    mid = time.time()
    kwargs.update(temp)
    result_sl = [x for x in func_sl(*args, **kwargs)]
    last = time.time()
    assert result_nt == result_sl, "With args = " + str(args) + " and kwargs = " + str(kwargs) + ", the assertion is failed \n " \
                                   "result_nt = "+ str(result_nt) +" should equal to result_sl = " + str(result_sl)

    nt = mid - start
    sl = last - mid
    timing.append((nt, sl))
    return nt, sl


@metric_log
def validate_document(timing, func_nt, func_sl, *args, **kwargs):
    """Validation for functions that returns single document
    """
    start = time.time()
    result_nt = func_nt(*args, **kwargs)
    mid = time.time()
    result_sl = func_sl(*args, **kwargs)
    last = time.time()
    assert result_nt == result_sl, "With args = " + str(args) + " and kwargs = " + str(kwargs) + ", the assertion is failed \n " \
                                   "result_nt = "+ str(result_nt) +" should equal to result_sl = " + str(result_sl)
    nt = mid - start
    sl = last - mid
    timing.append((nt, sl))
    return nt, sl


@metric_log
def validate_result(timing, func_nt, func_sl, filter, undo_func=None, undo_kwargs=None, *args, **kwargs):
    """Validation for functions that returns result
    """
    start = time.time()
    result_nt = func_nt(filter, *args, **kwargs)
    mid1 = time.time()
    undo_func(**undo_kwargs)
    mid2 = time.time()
    result_sl = func_sl(filter, *args, **kwargs)
    last = time.time()
    undo_func(**undo_kwargs)
    # TODO: add a proper validation to make sure that the updated data is the same,
    #  for now the validation only check for the number of `matched_count` and `modified_count` only
    result_nt = [result_nt.matched_count, result_nt.modified_count]
    result_sl = [result_sl.matched_count, result_sl.modified_count]
    assert result_nt == result_sl, "With func_nt = " + func_nt.__name__ +  " , func_sl = " + func_sl.__name__  + " , args = " + str(args) + " and kwargs = " + str(kwargs) + ", the assertion is failed \n " \
                                   "result_nt: " + str(result_nt)  + "should equal to result_sl: " + str(result_sl)
    nt = mid1 - start
    sl = last - mid2
    timing.append((nt, sl))
    return nt, sl


def validate_args_list(timing, validator, func_nt, func_sl, args_list):
    """Validation function for easily validate same function with different arguments
    """
    for args, kwargs in args_list:
        validator(timing, func_nt, func_sl, *args, **kwargs)


def args(*args, **kwargs):
    return args, kwargs
