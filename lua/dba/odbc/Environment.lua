--- Extend @{Environment.Environment|Environment} class with ODBC specific function
-- @class module
-- @name odbc.Environment

local utils       = require "dba.utils"
local Environment = require "dba.Environment"

local ERR_MSGS      = assert(utils.ERR_MSGS)

--- Возвращает список установленных в системе драйверов ODBC
-- 
-- @tparam[opt] dba.callback_function fn 
-- @treturn {dba.driverinfo,...} если не указана callback функция
-- @see dba.driverinfo
function Environment:drivers(fn)
  assert(self.private_.env)
  if not self.private_.env.getdrivers then return nil, ERR_MSGS.not_support end
  if fn then return self.private_.env:drivers(fn) end
  return self.private_.env:drivers()
end

--- Возвращает массив DSN 
-- 
-- @tparam[opt] dba.callback_function fn 
-- @treturn {dba.dsninfo,...} если не указана callback функция
-- @see dba.dsninfo
function Environment:datasources(fn)
  assert(self.private_.env)
  if not self.private_.env.getdatasources then return nil, ERR_MSGS.not_support end
  if fn then return self.private_.env:datasources(fn) end
  return self.private_.env:datasources()
end

function Environment:set_login_timeout(ms)
  assert(self.private_.env)
  assert((type(ms) == 'number') or (ms == nil))
  self.private_.login_timeout = ms
  return true
end

function Environment:get_login_timeout()
  assert(self.private_.env)
  return self.private_.login_timeout
end


return Environment