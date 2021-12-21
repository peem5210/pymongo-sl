import numpy as np
class Report:
    def __init__(self):
        self.sl_timing = []
        self.nt_timing = []
        self.passed_count = 0
        self.failed_count = 0

    def __str__(self):
        return "passed_count: %d \nfailed_count: %d\n" % (self.passed_count, self.failed_count,) + \
               "average nt: " + str(np.average(self.nt_timing)) + "\naverage sl: " + str(np.average(self.sl_timing))
