--- Simple Logger that can log directly to logstash servers using either json or msgpack

local insert = table.insert
local concat = table.concat

local cjson = require "cjson"
local cjson_encode = cjson_encode

local mp = require "MessagePack"
local mp_encode = mp.pack

local _M = {}

_M._VERSION = '0.1.0'

local _mt = { __index = _M }

local encoders = {
    json = cjson_encode,
    msgpack = mp_encode
}

local function msgpack_encode(data
--- create a new logger.
-- @tparam table opts a table of options. valid fields are:
-- * `codec` string. `json` or `msgpack`. defaults to `json`
-- * `servers` table. A list of servers in the form: `{ { host: "ipaddress", port: port } }`
-- * `interval` number - how often to attempt to push the log buffer to logstash servers in seconds. defaults to 30
-- * `retries` number - how many times to retry. If the logger gets an error writing to a server, it will select another and retry. After `retries` times, the data buffer is flushed and an error is logged locally. defaults to the number of servers.
-- * `timeout` number - timeout in ms for all socket operations. defaults to 1000
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
        buffer = {}
    }

    return setmetatable(self, _mt)
end

local function next_server(self)
    local i = self.current_server
    i = (i % self.num_servers) + 1
    self.current_server = i
    return self.servers[i]
end

locla tcp = ngx.socket.tcp
local flush_buffer
flush_buffer=function(premature, self)
    local buffer = self.buffer
    if #buffer > 0 then
        -- hack to get a trailing newline
        insert(buffer, "")
        local data = concat(buffer, "\n")
        local retries = self.retries

        repeat
            local server = next_server(self)
            local sock = tcp()
            sock:settimeout(self.timeout)
            local ok, err = sock:connect(server.host, server.port)
            if ok then
                local bytes, err = sock:send(data)
                if bytes then
                    sock:setkeepalive()
                    break
                end
            end
            retries = retries - 1
        until retries < 0
        if retries < 0 then
            -- should log an error here?
        end
    end

    if not premature then
        ngx.timer.at(delay, handler)
    end
end

--- log data.  The logger will add the required logstash fields to data. Note: this actually adds to an internal buffer and the data is written at `interval` via a timer.
-- @tparam resty.logstash.logger self
-- @tparam data table data to log
function _M.log(self, data)

    if not self.timer_started then
        local ok, err = ngx.timer.at(self.interval, flush_buffer, self)
        if ok then
            self.timer_started = true
        end
    end

    local data, err = self.encoder(data)
    if not data then
        return nil, err
    end
    insert(self.buffer, data)
    return true
end


return _M
