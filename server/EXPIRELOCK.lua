-- @ARGS
-- key string - the key to acquire the lock on
-- timeAcquiring number - the time the lock is being acquired at
-- timeMaybeAcquired number - the time the lock was supposedly acquired at
-- timeExpire number - the time to expire lock in the case no one ever needs this lock again
-- @RETURNS -1 if the time is updated, the time stored at the key if not

-- updates the key to newValue if the current value is less than cmp
-- this is used to implement a lock on the key, where when one client obtains a permit (-1), another client may not use the key until cmp is exceeded
local function updateLt(key, timeAcquiring, timeMaybeAcquired, timeExpire)
    local timeActuallyAcquired = redis.call('get', key)
    if timeActuallyAcquired == false or timeActuallyAcquired < timeMaybeAcquired then
        -- the time threshold has been met or time was never recorded, set a new time and acquire the lock for the client
        timeActuallyAcquired = redis.call('setex', key, timeAcquiring, timeExpire)
        return -1
    else
        -- time threshold has not been met, return at what time the event happened
        return timeActuallyAcquired
    end
end

return updateLt(ARGV[1], ARGV[2], ARGV[3])