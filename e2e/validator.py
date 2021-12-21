import time

SL_ONLY_ARGS = ["same_region", "enable_cache"]


def metric_log(function):
    def wrapper(*args, **kwargs):
        report = args[0]
        try:
            nt, sl = function(*args, **kwargs)
            report.passed_count += 1
            print("[PASSED]: gained " + format(sl - nt, '.5f') + " sec with function: " + args[1].__name__)
        except Exception as e:
            report.failed_count += 1
            print("[ERROR]: failed: " + str(e) + " with function: " + args[1].__name__)
    return wrapper


@metric_log
def validate_cursor(report,  func_nt, func_sl, *args,  **kwargs):
    """Validation for functions that returns cursor
    """
    temp = {}
    for arg in SL_ONLY_ARGS:
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
    report.nt_timing.append(nt)
    report.sl_timing.append(sl)
    return nt, sl

@metric_log
def validate_document(report, func_nt, func_sl, *args, **kwargs):
    """Validation for functions that returns single document
    """
    temp = {}
    for arg in SL_ONLY_ARGS:
        if arg in kwargs:
            temp[arg] = kwargs.pop(arg)
    start = time.time()
    result_nt = func_nt(*args, **kwargs)
    mid = time.time()
    kwargs.update(temp)
    result_sl = func_sl(*args, **kwargs)
    last = time.time()
    assert result_nt == result_sl, "With args = " + str(args) + " and kwargs = " + str(kwargs) + ", the assertion is failed \n " \
                                   "result_nt = "+ str(result_nt) +" should equal to result_sl = " + str(result_sl)
    nt = mid - start
    sl = last - mid
    report.nt_timing.append(nt)
    report.sl_timing.append(sl)
    return nt, sl


@metric_log
def validate_result(report, func_nt, func_sl, filter, *args, **kwargs):
    """Validation for functions that returns result
    """
    undo_func = kwargs.pop("undo_func", None)
    undo_kwargs = kwargs.pop("undo_kwargs", None)
    assert undo_func and undo_kwargs, "undo_func and undo_kwargs must not be empty for validate result"
    temp = {}
    for arg in SL_ONLY_ARGS:
        if arg in kwargs:
            temp[arg] = kwargs.pop(arg)
    start = time.time()
    result_nt = func_nt(filter, *args, **kwargs)
    mid1 = time.time()
    undo_func(**undo_kwargs)
    kwargs.update(temp)
    mid2 = time.time()
    result_sl = func_sl(filter, *args, **kwargs)
    last = time.time()
    undo_func(**undo_kwargs)
    # TODO: add a proper validation to make sure that the updated data is the same,
    #  for now the validation only check for the number of `matched_count` and `modified_count` only
    result_nt = [result_nt.matched_count, result_nt.modified_count]
    result_sl = [result_sl.matched_count, result_sl.modified_count]
    assert result_nt == result_sl, "With func_nt = " + func_nt.__name__ + " , func_sl = " + func_sl.__name__ + " , args = " + str(args) + " and kwargs = " + str(kwargs) + ", the assertion is failed \n " \
                                   "result_nt: " + str(result_nt) + " should equal to result_sl: " + str(result_sl)
    nt = mid1 - start
    sl = last - mid2
    report.nt_timing.append(nt)
    report.sl_timing.append(sl)
    return nt, sl


