--- Extend @{Connection.Connection|Connection} class with ODBC specific function
-- @class module
-- @name dba.lodbc.Connection

local utils       = require "dba.utils"
local Connection  = require "dba.Connection"

local ERR_MSGS      = assert(utils.ERR_MSGS)
local cursor_utils  = assert(utils.cursor_utils)
local param_utils   = assert(utils.param_utils)
local connect_utils = assert(utils.connect_utils)

------------------------------------------------------------------
do -- Connection catalog

local function callable(fn)
  if not fn then return false end
  local t = type(fn)
  if t == 'function' then return true  end
  if t == 'boolean'  then return false end
  if t == 'number'   then return false end
  if t == 'string'   then return false end
  return true
end

local function implement(name, impl)
  Connection[        name ] = function(self, ...) return impl(self, nil,  ...) end
  Connection[ 'i' .. name ] = function(self, ...) return impl(self, 'n',  ...) end
  Connection[ 'n' .. name ] = function(self, ...) return impl(self, 'a',  ...) end
  Connection[ 't' .. name ] = function(self, ...) return impl(self, 'an', ...) end
end

local function typeinfo_impl(self, fetch_mode, ...)
  local arg = utils.pack_n(...)
  local fn = arg[arg.n]
  if callable(fn) then -- assume this callback
    arg[arg.n] = nil
    arg.n = arg.n - 1
  else fn = nil end

  local stmt, err  = self.private_.cnn:typeinfo(utils.unpack_n(arg))
  if not stmt then return nil, err end
  stmt:setdestroyonclose(true)

  if fn then return stmt:foreach(fetch_mode, true, fn) end
  return cursor_utils.fetch_all(stmt, fetch_mode or 'a', true)
end

local function pack_n_fn(self, ...)
  local arg
  if self:supports_catalg_name() then arg = utils.pack_n(...)
  else arg = utils.pack_n(nil, ...) end
  local fn = arg[arg.n]
  if callable(fn) then -- assume this callback
    arg[arg.n] = nil
    arg.n = arg.n - 1
  else fn = nil end

  return fn, arg
end

local function make_catalog_fn(name, catalog_fn)
  local impl = function (self, fetch_mode, ...)
    local fn, arg = pack_n_fn(self, ...)

    local stmt, err  = self.private_.cnn[ catalog_fn or name ] ( self.private_.cnn, utils.unpack_n(arg) )
    if not stmt then return nil, err end
    stmt:setdestroyonclose(true)

    if fn then return stmt:foreach(fetch_mode, true, fn) end
    return cursor_utils.fetch_all(stmt, fetch_mode or 'a', true)
  end
  
  implement(name, impl)
end

local function crossreference_impl(self, fetch_mode, ...)
  local arg = utils.pack_n(...)
  local fn = arg[arg.n]
  if callable(fn) then -- assume this callback
    arg[arg.n] = nil
    arg.n = arg.n - 1
  else fn = nil end

  local n = math.floor(arg.n/2)
  assert(0 == math.mod(arg.n,2)) -- ��� ������� ������ ��������� ���������� ������ ���������

  local primaryCatalog, primarySchema, primaryTable
  local foreignCatalog, foreignSchema, foreignTable
  if n > 0 then
    if self:supports_catalg_name() then 
      primaryCatalog, primarySchema, primaryTable = utils.unpack(arg,1,n)
      foreignCatalog, foreignSchema, foreignTable = utils.unpack(arg,n+1,arg.n)
    else
      primarySchema, primaryTable = utils.unpack(arg,1,n)
      foreignSchema, foreignTable = utils.unpack(arg,n+1,arg.n)
    end
  end

  local stmt, err  = self.private_.cnn:crossreference(
    primaryCatalog, primarySchema, primaryTable,
    foreignCatalog, foreignSchema, foreignTable
  )
  if not stmt then return nil, err end
  stmt:setdestroyonclose(true)

  if fn then return stmt:foreach(fetch_mode, true, fn) end
  return cursor_utils.fetch_all(stmt, fetch_mode or 'a', true)
end

implement('typeinfo', typeinfo_impl)
make_catalog_fn('tabletypes')
make_catalog_fn('schemas')
make_catalog_fn('catalogs')
make_catalog_fn('statistics')
make_catalog_fn('tables')
make_catalog_fn('table_privileges','tableprivileges')
make_catalog_fn('primary_keys',    'primarykeys')
make_catalog_fn('index_info',      'indexinfo')
implement('crossreference', crossreference_impl)
make_catalog_fn('columns')
make_catalog_fn('special_columns','specialcolumns')
make_catalog_fn('procedures')
make_catalog_fn('procedure_columns','procedurecolumns')
make_catalog_fn('column_privileges','columnprivileges')


--- ���������� ������ �������������� ����� ������.
--
-- @param tcode [optional] numeric type code
-- @param fn [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:typeinfo

--- ���������� ������ �������������� ����� ������.
--
-- @param fn [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:tabletypes

--- ���������� ������ ���� ��.
--
-- @param fn [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:schemas

--- ���������� ������ ���������.
--
-- @param fn [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:catalogs

---
-- @param catalog [optional] ������ ���� ������� ����������� ��������
-- @param schema [optional]
-- @param tableName [optional]
-- @param unique [optional] boolean
-- @param reserved [optional] boolean
-- @param fn [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:statistics 

---
-- @param catalog [optional] ������ ���� ������� ����������� ��������
-- @param schema [optional]
-- @param tableName [optional]
-- @param types [optional]
-- @param fn [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:tables 

---
-- @param catalog [optional] ������ ���� ������� ����������� ��������
-- @param schema [optional]
-- @param tableName [optional]
-- @param fn [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:table_privileges

---
-- @param catalog [optional] ������ ���� ������� ����������� ��������
-- @param schema [optional]
-- @param tableName [optional]
-- @param fn [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:primary_keys 

---
-- @param catalog [optional] ������ ���� ������� ����������� ��������
-- @param schema [optional]
-- @param tableName [optional]
-- @param fn [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:index_info 

---
-- @param pc [optional] primary catalog (������ ���� ������� ����������� ��������)
-- @param ps [optional] primary schema  
-- @param pt [optional] primary table   
-- @param fc [optional] foreign catalog (������ ���� ������� ����������� ��������)
-- @param fs [optional] foreign schema  
-- @param ft [optional] foreign table   
-- @param fn [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:crossreference

---
-- @param catalog          [optional] ������ ���� ������� ����������� ��������
-- @param schema           [optional] 
-- @param tableName        [optional] 
-- @param columnName       [optional] 
-- @param fn               [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:columns 

---
-- @param catalog          [optional] ������ ���� ������� ����������� ��������
-- @param schema           [optional] 
-- @param tableName        [optional] 
-- @param fn               [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:special_columns 

---
-- @param catalog          [optional] ������ ���� ������� ����������� ��������
-- @param schema           [optional] 
-- @param procName         [optional]  
-- @param fn               [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:procedures 

---
-- @param catalog          [optional] ������ ���� ������� ����������� ��������
-- @param schema           [optional] 
-- @param procName         [optional]   
-- @param colName          [optional]   
-- @param fn               [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:procedure_columns 

---
-- @param catalog          [optional] ������ ���� ������� ����������� ��������
-- @param schema           [optional] 
-- @param tableName        [optional] 
-- @param columnName       [optional] 
-- @param fn               [optional] callback
-- @return ������ �������
-- @see dba.callback_function
-- @class function
-- @name Connection:column_privileges 

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Connection ODBC specific

--- ���������� �������� ����.
--
--
function Connection:dbmsname()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:dbmsname()
end

---
--
--
function Connection:drvname()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:drvname()
end

--- ���������� ������ ��������.
--
--
function Connection:drvver()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:drvver()
end

--- ���������� ������ ODBC � ���� ������.
--
--
function Connection:odbcver()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:odbcver()
end

--- ���������� ������ ODBC � ���� ���� �����.
--
--
function Connection:odbcvermm()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:odbcvermm()
end

--- ���������� �������� ������������.
--
--
function Connection:username()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:userName()
end

---
--
--
function Connection:set_catalog(value)
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:setcatalog(value)
end

---
--
--
function Connection:get_catalog()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:getcatalog()
end

---
--
--
function Connection:set_readonly(value)
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:setreadonly(value)
end

---
--
--
function Connection:get_readonly()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:getreadonly()
end

---
--
--
function Connection:set_trace_file(value)
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:settracefile(value)
end

---
--
--
function Connection:get_trace_file()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:gettracefile()
end


---
--
--
function Connection:set_trace(value)
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:settrace(value)
end

---
--
--
function Connection:get_trace()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:gettrace()
end

---
--
--
function Connection:supports_catalg_name()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end

  -- or supportsCatalogsInDataManipulation ???
  return self.private_.cnn:isCatalogName()
end

--
--
--
local TRANSACTION_LEVEL = {
  "TRANSACTION_NONE","TRANSACTION_READ_UNCOMMITTED","TRANSACTION_READ_COMMITTED",
  "TRANSACTION_REPEATABLE_READ","TRANSACTION_SERIALIZABLE"
}
for i = 1, #TRANSACTION_LEVEL do TRANSACTION_LEVEL[ TRANSACTION_LEVEL[i] ] = i end

--- ��������� ������������ �� ������� ����������.
-- <br> �������� �������� ������������� ������ ��������
-- @param lvl [optional] ������� �������� ���������� (�����/������)
-- @see dba.transaction_level
function Connection:supports_transaction(lvl)
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  if lvl == nil then return self.private_.cnn:supportsTransactions() end
  if type(lvl) == 'string' then 
    local lvl_n = TRANSACTION_LEVEL[lvl] 
    if not lvl_n then return nil, ERR_MSGS.unknown_txn_lvl .. lvl end
    lvl = lvl_n
  end

  assert(type(lvl) == 'number')
  return self.private_.cnn:supportsTransactionIsolationLevel(lvl)
end

--- ���������� ������� ���������� �� ��������� ��� �����������.
-- @return �������� �������� 
-- @return ��������� ��������
function Connection:default_transaction()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  local lvl, err = self.private_.cnn:getDefaultTransactionIsolation()
  if not lvl then return nil, err end
  return lvl, TRANSACTION_LEVEL[lvl]
end

--- ������������� ������� ������� ���������� ��� �����������.
-- @param lvl [optional] ������� �������� ���������� (�����/������). ���� nil, �� ������������ �������� �� ���������.
-- 
function Connection:set_transaction_level(lvl)
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end

  local err 
  if lvl == nil then
    lvl, err = self:default_transaction()
    if not lvl then return nil, err end;
  elseif type(lvl) == 'string' then 
    local lvl_n = TRANSACTION_LEVEL[lvl] 
    if not lvl_n then return nil, ERR_MSGS.unknown_txn_lvl .. lvl end
    lvl = lvl_n
  end

  assert(type(lvl) == 'number')
  return self.private_.cnn:settransactionisolation(lvl)
end

--- ���������� ������� ������� ���������� ��� �����������.
-- @return �������� �������� 
-- @return ��������� ��������
function Connection:get_transaction_level()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end

  local lvl, err = self.private_.cnn:gettransactionisolation()
  if not lvl then return nil, err end
  return lvl, TRANSACTION_LEVEL[lvl]
end

--- ��������� ������������ �� ������� �������� ���������� ��������.
--
function Connection:supports_bind_param()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:supportsBindParam()
end

--- ��������� ������������ �� ������� �������������� �������.
--
function Connection:supports_prepare()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:supportsPrepare()
end

--- ������������� ������� ��� ����������� � ��
-- 
-- @param ms [optional] - �������� � �������������. nil �� �������������.
-- @return - true
function Connection:set_login_timeout(ms)
  assert(self.private_.cnn)
  assert((type(ms) == 'number') or (ms == nil))
  self.private_.cnn:setlogintimeout(ms or -1)
  return true
end

--- ���������� ������� ��� ����������� � ��
-- 
-- @return - �������� � �������������. nil �� ������������
function Connection:get_login_timeout()
  assert(self.private_.env)
  local ms = self.private_.cnn:getlogintimeout(ms or -1)
  if ms == -1 then return nil end
  return ms
end

end
------------------------------------------------------------------

return Connection