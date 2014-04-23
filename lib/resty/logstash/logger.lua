--- Simple Logger that can log directly to logstash servers using either json or msgpack

local insert = table.insert
local concat = table.concat

local redis = require "resty.redis"

local _M = {}

_M._VERSION = '0.1.0'

local _mt = { __index = _M }

local encoders = {
    json = require("cjson").encode,
    msgpack = require("MessagePack").pack
}

local function redis_pusher(self, server, data)

    local red = redis:new()

    red:set_timeout(self.timeout)

    local ok, err = red:connect(server.host, server.port)
    if not ok then
        return nil, err
    end

    red:init_pipeline()
    local key = self.key

    local ok, err = red:multi()
    if not ok then
        return nil, err
    end

    for _,v in ipairs(data) do
        red:lpush(key, v)
    end

    local ok, err = red:exec()
    if not ok then
         return nil, err
    end

    local results, err = red:commit_pipeline()
    if not results then
        return nil, err
    end

    red:set_keepalive()

    return true
end

local function logstash_pusher(self, server, data)
    -- hack to get a trailing newline
    insert(data, "")
    data = concat(data, "\n")
    local sock = tcp()
    sock:settimeout(self.timeout)
    local ok, err = sock:connect(server.host, server.port)
    if not ok then
        return nil, err
    end
    local bytes, err = sock:send(data)
    if not bytes then
        return nil, err
    end
    sock:set_keepalive()
    return true
end

local pushers = {
    redis = redis_pusher,
    logstash = logstash_pusher
}

--- create a new logger.
-- @tparam table opts a table of options. valid fields are:
-- * `codec` string. `json` or `msgpack`. defaults to `json`
-- * `servers` table. A list of servers in the form: `{ { host: "ipaddress", port: port } }`
-- * `interval` number - how often to attempt to push the log buffer to logstash servers in seconds. defaults to 30
-- * `retries` number - how many times to retry. If the logger gets an error writing to a server, it will select another and retry. After `retries` times, the data buffer is flushed and an error is logged locally. defaults to the number of servers.
-- * `timeout` number - timeout in ms for all socket operations. defaults to 1000
-- * `type` string - either `redis` or `logstash`. defaults to `logstash`
-- * `key` string - only needed when pushing to redis, this is the key to push to. defaults to `logstash`
-- @treturn resty.logstash.logger a logger
function _M.new(opts)
    local codec = opts.codec or "json"
    local encoder = encoders[codec]

    if not encoder then
        return nil, "unknown codec"
    end

    local servers = opts.servers
    local hosts = {}
    if not servers then
        return nil, "servers are required"
    end
    for i, v in ipairs(servers) do
        if not v.port then
            return nil, "server must have a port"
        end
        if not v.host then
            return nil, "server must have a host"
        end
        -- make a copy as we may want to modify the table
        insert(hosts, { host: v.host, port, v.port })
    end

    local pusher = pushers[opts.type or "logstash"]
    if not pusher then
        return nil, "unknown type"
    end

    local num_servers = #servers
    local self = {
        servers = hosts,
        encoder = encoder,
        timeout = opts.timeout or 1000
        num_servers = num_servers,
        retries = opts.retries or num_servers
        current_server = 1,
        interval = opts.interval or 30000,
        timer_started = false,
        buffer = {},
        pusher = pusher,
        key = opts.key or "logstash"
    }

    return setmetatable(self, _mt)
end

local function next_server(self)
    local i = self.current_server
    i = (i % self.num_servers) + 1
    self.current_server = i
    return self.servers[i]
end

local tcp = ngx.socket.tcp
local flush_buffer
flush_buffer=function(premature, self)
    local buffer = self.buffer
    local pusher = self.pusher
    if #buffer > 0 then
        local retries = self.retries

        repeat
            local server = next_server(self)
            local ok, err = pusher(self, next_server(self), buffer)
            if ok then
                break
            end
            retries = retries - 1
        until retries < 0
        if retries < 0 then
            -- should log an error here?
        end
    end

    self.buffer = {}

    if not premature then
        ngx.timer.at(self.interval, handler)
    end
end

local date = os.date
--- log data.  The logger will add the required logstash fields to data. Note: this actually adds to an internal buffer and the data is written at `interval` via a timer. Also, this will modify data.
-- @tparam resty.logstash.logger self
-- @tparam table data
function _M.log(self, data)

    if not self.timer_started then
        local ok, err = ngx.timer.at(self.interval, flush_buffer, self)
        if ok then
            self.timer_started = true
        end
    end

    -- TODO: add all fields required by logstash

    -- logstash requires ISO 8601. format from http://stackoverflow.com/a/20131960
    data["@timestamp"] = date("!%Y-%m-%dT%TZ", ngx.time())

    local data, err = self.encoder(data)
    if not data then
        return nil, err
    end
    insert(self.buffer, data)
    return true
end


return _M
