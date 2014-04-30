from buildbot.status import results

class BuildStatus():
    INQUEUE = -1
    SCHEDULING = -2
    SCHEDULED = -3
    BUILDING = -4
    SUCCESS = results.SUCCESS
    WARNINGS = results.WARNINGS
    FAILURE = results.FAILURE
    SKIPPED = results.SKIPPED
    EXCEPTION = results.EXCEPTION
    RETRY = results.RETRY
    toString = {
        INQUEUE: "In queue",
        SCHEDULING: "Scheduling",
        SCHEDULED: "Scheduled",
        BUILDING: "Building",
        SUCCESS: results.Results[SUCCESS],
        WARNINGS: results.Results[WARNINGS],
        FAILURE: results.Results[FAILURE],
        SKIPPED: results.Results[SKIPPED],
        EXCEPTION: results.Results[EXCEPTION],
        RETRY: results.Results[RETRY]
    }
