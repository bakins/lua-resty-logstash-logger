-- you would probably do this in an init_by_lua loaded
-- module.
local logger = require "resty.logstash.logger"

local l = logger.new({
                         codec = "json",
                         servers = { { host = "127.0.0.1", port = 6379 } },
                         interval = 0.001,
                         type = "redis"
                     })

local rc = l:log({ uri = ngx.uri })

ngx.say(rc)
