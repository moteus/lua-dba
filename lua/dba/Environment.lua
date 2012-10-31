--- Implement environment class
-- @class module
-- @name Environment

local utils      = require "dba.utils"
local Connection = require "dba.Connection"

local OPTIONS       = assert(utils.OPTIONS)
local ERR_MSGS      = assert(utils.ERR_MSGS)

--- 
-- @type Environment

local Environment = {} Environment.__index = Environment
local Environment_private = {}

------------------------------------------------------------------
do -- Environment ctor/dtor

--- ������� ����� ������ `Environment`.
-- 
-- @local
--
-- @return ������ `Environment`
function Environment:new(env)
  local t = setmetatable({
    private_ = {
      env = env;
    }
  },self)
  
  return t
end

--- ���������� ������ `Environment`.
-- 
-- <br> ���� ������������ ������ ������������� ��� ������������� 
-- ����������� � ��� �� ����������, �� ���������� ����������
function Environment:destroy()
  local env = self.private_.env
  if not env then return true end
  (env.destroy or env.close)(env)
  self.private_.env = nil
  return true
end

--- ���������� ������������������ ����������.
-- 
-- @return handle
function Environment:handle()
  return self.private_.env
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Environment connection

--- ������� ����� ������ Connection.
--
-- @class function
-- @name Environment:connection
-- @param dsn [optional]  �������� DSN
-- @param login [optional] 
-- @param password [optional] 
-- @param autocommit [optional] 
-- @return ������ Connection
-- @usage local db = env:connection('demo','DBA','sql',false)
-- @usage local db = env:connection('demo',false) --����� � ������ �������, �� autocommit ����������

--- ������� ����� ������ Connection.
-- 
-- @class function
-- @name Environment:connection
-- @param params ������� ��� ������������ ������ �����������
-- @param autocommit
-- @return ������ Connection 
-- @return ������ �����������
-- @usage local db = env:connection{DSN='demo',UID='DBA',PWD='sql'}

--
function Environment:connection(...)
  return Connection:new(self, false, ...)
end

--- ������� ����� ������ Connection � ��������� �����������.
-- <br>������������ cnn = env:connection(...) cnn:open().
-- <br>���� �� ������� ����������� � ��, �� ������ Connection ������������.
-- @class function
-- @name Environment:connect
-- @see Environment:connection
-- @see Environment:connection
-- @see Connection.Connection:connect

function Environment:connect(...)
  local cnn, err = self:connection(...)
  if not cnn then return nil, err end
  local ok, err = cnn:connect()
  if not ok then 
    cnn:destroy()
    return nil, err
  end
  return cnn
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Environment config

function Environment:get_config(name )     return Environment_private.get_config_param(self, name)      end

function Environment:set_config(name, val) return Environment_private.set_config_param(self, name, val) end

function Environment_private:get_config_param(name)
  if self.private_.lib_opt then
    local val = self.private_.lib_opt[name]
    if val ~= nil then return val end
  end
  return OPTIONS[name]
end

function Environment_private:set_config_param(name, value)
  if not self.private_.lib_opt then
    self.private_.lib_opt = {}
  end
  self.private_.lib_opt[name] = value

  return Environment_private.get_config_param(self, name)
end

end
------------------------------------------------------------------

return Environment