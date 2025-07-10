-- @ARGS
-- key string - the key to initialize the array at
-- len number - the length of the array to initialize with zeros
-- @RETURNS 1 if a WRITE happens and 0 otherwise

-- initializes a key with an array of all zeros if it hasn't been set yet
local function zeroInit(key, len)
    if redis.call('exists', key) == 1 then
        return 0
    else
        -- zero initialize an array when key is not set
        local buf = string.rep('\0', len)
        redis.call('set', key, buf)
        return 1
    end
end

return zeroInit(ARGV[1], ARGV[2])