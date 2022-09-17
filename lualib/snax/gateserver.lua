local skynet = require "skynet"
local netpack = require "skynet.netpack"
local socketdriver = require "skynet.socketdriver"

local gateserver = {}

local socket	-- listen socket
local queue		-- message queue
local maxclient	-- max client
local client_number = 0
local CMD = setmetatable({}, { __gc = function() netpack.clear(queue) end })
local nodelay = false

local connection = {}
-- true : connected
-- nil : closed
-- false : close read

function gateserver.openclient(fd)
	if connection[fd] then
		socketdriver.start(fd)
	end
end

function gateserver.closeclient(fd)
	local c = connection[fd]
	if c ~= nil then
		connection[fd] = nil
		socketdriver.close(fd)
	end
end

-- where does the handler comes from
function gateserver.start(handler)
	skynet.error("snax.gateserver started,handler.message=", handler.message, ",handler.name", handler.name)
	assert(handler.message)
	assert(handler.connect)

	local listen_context = {}

	function CMD.open( source, conf )
		for key, value in pairs(conf) do
			skynet.error("gateserver.start, key=", key, ",value=", value)
		end
		assert(not socket)
		local address = conf.address or "0.0.0.0"
		local port = assert(conf.port)
		maxclient = conf.maxclient or 1024
		nodelay = conf.nodelay
		skynet.error(string.format("snax.gateserver, Listen on %s:%d", address, port))
		socket = socketdriver.listen(address, port)
		listen_context.co = coroutine.running()
		listen_context.fd = socket
		-- 让出当前的任务执行流程，直到用 wakeup 唤醒它
		skynet.error("CMD.open,wait wakeup")
		skynet.wait(listen_context.co)
		skynet.error("CMD.open, waked up")
		conf.address = listen_context.addr
		conf.port = listen_context.port
		skynet.error("CMD.open,listen_context.co=", listen_context.co, ",socket=", socket, ",handler.open=", handler.open, ",conf.address=", conf.address, ",conf.port=", conf.port)
		listen_context = nil
		-- 开始处理 该 socket
		skynet.error("socket=", socket)
		socketdriver.start(socket)
		if handler.open then
			return handler.open(source, conf)
		end
	end

	function CMD.close()
		assert(socket)
		socketdriver.close(socket)
	end

	local MSG = {}

	local function dispatch_msg(fd, msg, sz)
		if connection[fd] then
			handler.message(fd, msg, sz)
		else
			skynet.error(string.format("Drop message from fd (%d) : %s", fd, netpack.tostring(msg,sz)))
		end
	end

	MSG.data = dispatch_msg

	local function dispatch_queue()
		local fd, msg, sz = netpack.pop(queue)
		skynet.error("dispatch_queue,fd=", fd, ",msg=", msg, ",sz=", sz)
		if fd then
			-- may dispatch even the handler.message blocked
			-- If the handler.message never block, the queue should be empty, so only fork once and then exit.
			skynet.fork(dispatch_queue)
			dispatch_msg(fd, msg, sz)

			for fd, msg, sz in netpack.pop, queue do
				dispatch_msg(fd, msg, sz)
			end
		end
	end

	MSG.more = dispatch_queue

	function MSG.open(fd, msg)
		skynet.error("gateserver.MSG.open", fd, msg)
		if client_number >= maxclient then
			socketdriver.shutdown(fd)
			return
		end
		if nodelay then
			socketdriver.nodelay(fd)
		end
		connection[fd] = true
		client_number = client_number + 1
		handler.connect(fd, msg)
	end

	function MSG.close(fd)
		if fd ~= socket then
			client_number = client_number - 1
			if connection[fd] then
				connection[fd] = false	-- close read
			end
			if handler.disconnect then
				handler.disconnect(fd)
			end
		else
			socket = nil
		end
	end

	function MSG.error(fd, msg)
		if fd == socket then
			skynet.error("gateserver accept error:",msg)
		else
			socketdriver.shutdown(fd)
			if handler.error then
				handler.error(fd, msg)
			end
		end
	end

	function MSG.warning(fd, size)
		if handler.warning then
			handler.warning(fd, size)
		end
	end

	function MSG.init(id, addr, port)
		if listen_context then
			local co = listen_context.co
			if co then
				assert(id == listen_context.fd)
				listen_context.addr = addr
				listen_context.port = port
				skynet.wakeup(co)
				listen_context.co = nil
				skynet.error("MSG.init", listen_context.addr, listen_context.port, listen_context.co)
			end
		end
	end

	-- 在当前服务内注册 socket 类型的消息的处理机制
	skynet.register_protocol {
		name = "socket",
		id = skynet.PTYPE_SOCKET,	-- PTYPE_SOCKET = 6
		unpack = function ( msg, sz )
			return netpack.filter( queue, msg, sz)
		end,
		dispatch = function (_, _, q, type, ...)
			queue = q
			if type then
				skynet.error("dispatch socket msg", type, q, ...)
				MSG[type](...)
			end
		end
	}

	local function init()
		skynet.dispatch("lua", function (_, address, cmd, ...)
			local f = CMD[cmd]
			skynet.error("snax.gatesrever.init,cmd=", cmd, ..., ",address=", address, ",f=", f)
			if f then
				skynet.ret(skynet.pack(f(address, ...)))
			else
				skynet.ret(skynet.pack(handler.command(cmd, address, ...)))
			end
		end)
	end

	skynet.error("handler.embed", handler.embed)
	if handler.embed then
		init()
	else
		-- 启动1新服务去处理 lua 消息
		skynet.start(init)
	end
end

return gateserver
