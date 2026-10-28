Fix tasks waiting forever when their HTCondor job is held, removed or ends without writing the results: the task now raises `HTCondorJobError` with the reason and the location of the job logs.
