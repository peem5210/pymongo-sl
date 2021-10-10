import os
def ENV(variable:str, to_int = False):
    if to_int:
        return int(os.environ.get(variable))
    result = os.environ.get(variable)
    assert result is not None, f"Environment variable '{variable}' not exist"
    return result