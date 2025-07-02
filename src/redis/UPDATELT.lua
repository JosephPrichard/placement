-- @ARGS keys string, value number, cmp number
-- @RETURNS -1 if the time is updated, the time stored at the key if not

-- updates the key to newValue if the current value is less than cmp
local function updateLt(key, newValue, cmp)
    local value = redis.call('get', key)
    if value == false or value < cmp then
        -- the time threshold has been met or time was never recorded, set a new time,
        value = redis.call('set', key, newValue)
        return -1
    else
        -- time threshold has not been met, return at what time the event happened
        return value
    end
end

return updateLt(ARGV[1], ARGV[2], ARGV[3])