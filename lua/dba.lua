--- ������ lua-dba
-- @class module
-- @name dba
--[=[-------------------------------------------------------------
@usage
local odbc = require "dba.lodbc"
local db = assert(odbc.Connect{dsn='demodb'})

sql_text = [[...]]
params = { ... }
db:each(sql_text,params,function(row) ... end)


local qry = db:prepare([[insert ...]])
for ... do
  qry:exec{ some params }
end
qry:destroy()


db:destroy()
--]=]-------------------------------------------------------------


local utils       = require "dba.utils"
local Environment = require "dba.Environment"
local Connection  = require "dba.Connection"

PARAM_DEFAULT = assert(utils.PARAM_DEFAULT)
PARAM_NULL    = assert(utils.PARAM_NULL)

local LIBS = setmetatable({},{__mode='v'})

return {load = function (ctor)
  local lib = LIBS[ctor]
  if lib then return lib end
  lib = {
    PARAM_DEFAULT = PARAM_DEFAULT;
    PARAM_NULL    = PARAM_NULL;
  }

  lib.Environment = function() return Environment:new(ctor()) end;

  lib.Connection  = function(...) 
    local env, err = Environment:new(ctor())
    if not env then return nil, err end
    local cnn, err = Connection:new(env, true, ...)
    if not cnn then
      env:destroy()
      return nil, err
    end
    return cnn
  end;

  lib.Connect     = function(...) 
    local cnn, err = lib.Connection(...)
    if not cnn then return nil, err end
    local ok, err = cnn:connect()
    if not ok then 
      cnn:destroy()
      return nil, err
    end
    return cnn
  end;

  LIBS[ctor] = lib
  return lib
end}

--- ������� ���������� ����������.
-- ������ �������� ����� ���� ���������� �� ������ Environment, Connection ��� Query
--
-- @class table
-- @name OPTIONS
-- @field FORCE_REPLACE_PARAMS ������ �������� ��������� ������������ ������. 
-- ���� �������� ����� ������������� �������� ��� ���������� batch �������� � �����������.
-- @field IGNORE_NAMED_PARAMS  �� ������� ��������������� ����������� ���������
-- ��� ���������� ��� �������������� ��������� ������ SQL ����� �����������
-- ��� ���� ��������� ����� ������������� ������ ���� ��������������� bind(����� �������������� ������ '?')
-- @usage
-- local sql = [[begin if :ID > 5 then select 5 else selct 0 end if end]]
-- qry:set_config('FORCE_REPLACE_PARAMS', true)
-- qry:rows({ID=10}, print)
-- @usage
-- local sql = [[select 'hello :world']]
-- qry:set_config('IGNORE_NAMED_PARAMS', true)
-- qry:rows(print)

--- callback ������� ��� �������� ������� ������.
-- <br> ���� ������� ���������� ����� ��������, �� ������� ������������
-- � ������������(���) �������� ���������� ����������� �������, ������� ������������ �������.
-- <br> ������ � ������� ���� � ���� ������ ���������� ���� � ���� �������.
-- <br> � ������� ����� ����������� ���� � ���� ������� � ������� ����������.
-- @class function
-- @name callback_function
-- @param row or list - ��������� ������
-- @see Connection.Connection:each
-- @see Query.Query:each

--- ���������, ����������� ��������� �������.
-- @class table
-- @name driverinfo
-- @field 1 �������� ��������
-- @field 2 ����� ���������� 
-- @see lodbc.Environment.Environment:drivers

--- ���������, ����������� ��������� �������
-- @class table
-- @name dsninfo
-- @field 1 �������� DSN
-- @field 2 �������� ��������
-- @see lodbc.Environment.Environment:datasources

--- ������ �������� ����������
-- @class table
-- @name transaction_level
-- @field 1 "TRANSACTION_NONE"
-- @field 2 "TRANSACTION_READ_UNCOMMITTED"
-- @field 3 "TRANSACTION_READ_COMMITTED"
-- @field 4 "TRANSACTION_REPEATABLE_READ"
-- @field 5 "TRANSACTION_SERIALIZABLE"

