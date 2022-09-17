local skynet = require "skynet"
local sprotoloader = require "sprotoloader"

local max_client = 64

skynet.start(function ()
	skynet.error("game server start")
	skynet.uniqueservice("protoloader")
	skynet.newservice("debug_console", 8088)
	-- 启动 watchdog 服务
	local watchdog = skynet.newservice("watchdog")
	-- 阻塞调用 watchdog 服务的 start 方法
	local addr, port = skynet.call(watchdog, "lua", "start", {
		port = 8888,
		max_client = max_client,
		nodelay = true,
	})
	skynet.error("Watchdog listen on " .. addr .. ":" .. port)
	skynet.exit()
end)
