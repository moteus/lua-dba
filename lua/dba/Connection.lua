--- Implement Connection class
-- @class module
-- @name Connection

local utils  = require "dba.utils"
local Query  = require "dba.Query"

local ERR_MSGS      = assert(utils.ERR_MSGS)
local cursor_utils  = assert(utils.cursor_utils)
local param_utils   = assert(utils.param_utils)
local connect_utils = assert(utils.connect_utils)

--- 
-- @type Connection

local Connection = {} Connection.__index = Connection
local Connection_private = {}

------------------------------------------------------------------
do -- Connection ctor/dtor

--- ������� ����� ������ Connection.
-- 
-- @local
-- 
-- <br> ��� �������� ��� �� ��������� ������ `Environment`.
-- 
-- <br> ��������� ������ �� ��������� � ��. ��� ����������� ���������� ������� Connection:connect.
-- <br> ��������� ����������� ����� ���� ����������� �����.
-- @param env - [required] ������ Environment 
-- @param own - [required] ���� true, �� ���������� �������� �� env.
--              (���� ������ ������������ ������ � �������� Connection.)
-- @param ... [optional] ��������� ��� ����������� � ��.
-- @return ������ `Connection`
-- @see Environment.Environment:connection
-- @see Environment.Environment:connect
-- @see Connection.Connection:connect
function Connection:new(env, own, ...)
  assert(env)
  local henv = assert(env:handle())

  local cnn
  if henv.connection then
    local err cnn, err = henv:connection()
    if not cnn then return nil, err end
  end

  local t = setmetatable({
    private_ = {
      cursors = setmetatable({},{__mode='k'});
      is_own_env = own;
      env = env;
      cnn = cnn;
    }
  }, self)

  Connection_private.set_cnn_data(t, ...)

  return t;
end

--- ���������� ������ Connection.
-- 
-- <br> ���� ������������ ������ ������������� ��� ������������� 
-- �������� � ��� �� ����������, �� ���������� ����������
function Connection:destroy()
  self:disconnect()
  if self.private_.cnn then
    self.private_.cnn:destroy()
  end
  self.private_.cnn  = nil
  if self.private_.env and self.private_.is_own_env then 
    self.private_.env:destroy()
  end
  self.private_.env  = nil
  return true
end

--- ���������� ������������������ ����������.
-- 
-- @return handle
function Connection:handle()
  return self.private_.cnn
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Connection connect

function Connection_private:set_cnn_data(...)
  self.private_.cnn_data = utils.pack_n(...)
end

function Connection_private:get_cnn_data(...)
  if not self.private_.cnn_data then return end
  return utils.unpack_n(self.private_.cnn_data)
end

--- ��������� ����������� � ��.
--
-- ��������� ������ ���� ������������� � ����� �����. ������ ������� �������� �� � ������������, 
-- � �����/������ ��� ��������.
-- @param ... [optional] ��������� ��� ����������� � ��.
-- @return ������� ���������� �����������
-- @see Environment.Environment:connection
-- @see Environment.Environment:connect
function Connection:connect(...)
  self:disconnect()
  if select('#', ...) > 0 then Connection_private.set_cnn_data(self, ...) end

  local cnn, err = connect_utils.connect(
    self.private_.cnn or self.private_.env:handle(),
    Connection_private.get_cnn_data( self )
  )
  if not cnn then return nil, err end

  self.private_.cnn = cnn
  return true
end

--- ��������� ����������� � ��.
--
-- ������ �������� ��������� ��� ����������� �������������
-- @return true
function Connection:disconnect()
  local cnn = self.private_.cnn
  if not cnn then return true end

  local cur = next(self.private_.cursors)
  while(cur)do
    if cur.destroy then cur:destroy() else cur:close() end
    self.private_.cursors[cur] = nil
    cur = next(self.private_.cursors)
  end

  if cnn.disconnect then
    cnn:disconnect()
  else
    cnn:close()
    self.private_.cnn = nil
  end
  return true
end

--- ���������� ������� ����������� � ��.
--
function Connection:connected()
  if not self.private_.cnn then return false end
  return connect_utils.connected(self.private_.cnn)
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Connection query 

--- ������� ����� ������ Query.
-- 
-- @param sql [optional]
-- @param params [optional]
-- @return ������ Query
-- @class function
-- @name Connection:query

--
function Connection:query(...)
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return Query:new(self, ...)
end

--- ������� ����� �������������� ������ Query.
-- 
-- <br>���� prepare ����������� � ������� - ����� Query ������������
-- @param sql [required] ����� �������
-- @param params [optional] ������� ���������� ��� �������
-- @return ������ Query
-- @class function
-- @name Connection:prepare

--
function Connection:prepare(sql,params)
  assert(type(sql) == 'string')
  assert(params == nil or type(params) == 'table')
  local q, err = self:query(sql)
  if not q then return nil, err end
  local ok ok, err = q:prepare()
  if not ok then q:destroy() return nil, err end
  if params then ok, err = q:bind(params) end
  if not ok then q:destroy() return nil, err end
  return q
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Connection transaction 

--- ������������ ������� ����������.
--
-- <br> �� ����� ������ ���� autocommit=true
-- @see Connection:rollback
-- @see Connection:connect
-- @see Environment.Environment:connection
-- @see Environment.Environment:connect
function Connection:commit()
  if self:connected() then
    return self.private_.cnn:commit()
  end
  return nil, ERR_MSGS.cnn_not_opened
end


--- �������� ������� ����������.
--
-- <br> �� ����� ������ ���� autocommit=true
-- @see Connection:commit
-- @see Connection:connect
-- @see Environment.Environment:connection
-- @see Environment.Environment:connect
function Connection:rollback()
  if self:connected() then
    return self.private_.cnn:rollback()
  end
  return nil, ERR_MSGS.cnn_not_opened
end

--- ������������� ����� �������������� �������� ����������.
--
function Connection:set_autocommit(value)
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:setautocommit(value)
end

--- ���������� �������� ������ �������������� �������� ����������.
--
function Connection:get_autocommit()
  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end
  return self.private_.cnn:getautocommit()
end


end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Connection execute

function Connection_private:need_replace_params()
  if self:get_config('FORCE_REPLACE_PARAMS') then return true end
  if self.private_.cnn.statement then return false end
  return true
end

function Connection_private:execute(sql, params)
  assert((type(sql) == 'string'))
  assert((params == nil)or(type(params) == 'table'))

  if not self:connected() then return nil, ERR_MSGS.cnn_not_opened end

  local cnn = self.private_.cnn
  if (params == nil) or (next(params) == nil) then
    return connect_utils.execute(cnn, sql)
  end

  if Connection_private.need_replace_params(self) then
    -- ����������� ��������� � ���������
    -- �������������� ������ ����������� ���������
    if self:get_config('IGNORE_NAMED_PARAMS') then
      return nil, ERR_MSGS.deny_named_params
    end

    local psql, err = param_utils.apply_params(sql,params)
    if not psql then return nil, err end
    return connect_utils.execute(cnn, psql)
  end

  -- ������� ������������ ����� statement

  -- ���� ��������� ����������� ���������, �� ����������� ������
  local psql, plst 
  if not self:get_config('IGNORE_NAMED_PARAMS') then
    psql, plst = param_utils.translate_params(sql)
    if not psql then return nil, plst end
  end

  local stmt, err = cnn:statement()
  if not stmt then return nil, err end

  if plst and next(plst) then -- ���� ����������� ���������
    for i, pname in ipairs(plst) do
      local val = params[pname]
      if val == nil then
        stmt:destroy()
        return nil, ERR_MSGS.unknown_parameter .. pname
      end
      local ok, err = param_utils.bind_param(stmt, i, val)
      if not ok then 
        stmt:destroy()
        return nil, err
      end
    end
  else
    for i, v in ipairs(params) do
      local ok, err = param_utils.bind_param(stmt, i, v)
      if not ok then 
        stmt:destroy()
        return nil, err
      end
    end
  end

  local ok, err = stmt:execute(psql or sql)
  if not ok then
    stmt:destroy()
    return nil, err
  end
  
  if ok == stmt then return stmt end
  stmt:destroy()
  return ok
end

--- ��������� ������ ������� �� ������ ���������� Recordset.
--
-- ���� ������ ������ ������, �� �� �����������, �� �� ������������ ����� ����������
-- @param sql [required] ����� �������
-- @param params [optional] ������� ���������� ��� �������
-- @return ���������� ������� ��������������� � �������
-- @class function
-- @name Connection:exec

--
function Connection:exec(...)
  local res, err = Connection_private.execute(self, ...)
  if not res then return nil, err end
  if 'userdata' == type(res) then
    if res.destroy then res:destroy() else res:close() end
    return nil, ERR_MSGS.ret_cursor
  end
  return res
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Connection iterator

--- �������� ��� �������� Recordset.
--
-- ������������� �������� ������� ����� ����������� ������
-- @param sql [required] ����� �������
-- @param params [optional] ��������� ��� �������
-- @param fn [required] callback 
-- @see dba.callback_function
-- @usage
-- local sql = 'select ID, NAME from Clients where NAME = :NAME'
-- db:each(sql, {NAME='ALEX'}, print)
-- db:ieach(sql, {NAME='ALEX'}, function(r)print(r[1],r[2])end)
-- db:neach(sql, {NAME='ALEX'}, function(r)print(r.ID,r.NAME)end)
--
-- @usage
-- local alex_id = db:each('select ID, NAME from Clients', function(ID, NAME)
--   if NAME == 'ALEX' then return ID end
-- end)
--
-- @class function
-- @name Connection:each

-- fetch_mode, sql, [params,] fn
function Connection_private:each(fetch_mode, sql, ...)
  assert(type(sql) == 'string')

  local n, params = 2, ...
  if type(params) ~= 'table' then n, params = 1, nil end
  local cur, err = Connection_private.execute(self, sql, params)
  if not cur then return nil, err end
  if 'userdata' ~= type(cur) then return nil, ERR_MSGS.no_cursor end
  if cur.setdestroyonclose then cur:setdestroyonclose(true) end

  return cursor_utils.foreach(cur, fetch_mode, true, select(n, ...))
end

function Connection:each(...)  return Connection_private.each(self, nil,  ...) end

function Connection:ieach(...) return Connection_private.each(self, 'n',  ...) end

function Connection:neach(...) return Connection_private.each(self, 'a',  ...) end

function Connection:teach(...) return Connection_private.each(self, 'an', ...) end

--- �������� ��� �������� Recordset.
--
-- <br> �������� ��� generic for
-- <br> ������������� �������� ������� �� ���������� ����� ��� ��� �������� �����������.
-- <br> ������� ������������ �� ����� ������� ��� ���� ������ ���� �� ����� ����� NULL.
-- @param sql [required] ����� �������
-- @param params [optional] ��������� ��� �������
-- @usage
-- local sql = 'select ID, NAME from Clients where NAME = :NAME'
-- local params = {NAME='ALEX'}
-- for ID,NAME in db:rows(sql,params) do print(ID,NAME) end
-- for r in db:irows(sql,params) do print(r[1],r[2]) end
-- for r in db:nrows(sql,params) do print(r.ID,r.NAME) end
--
-- @class function
-- @name Connection:rows

-- fetch_mode, sql [,params] 
function Connection_private:rows(fetch_mode, sql, params)
  local cur, err = Connection_private.execute(self, sql, params)
  if not cur then error(tostring(err)) end
  if 'userdata' ~= type(cur) then error(ERR_MSGS.no_cursor) end
  if cur.setdestroyonclose then cur:setdestroyonclose(true) end
  self.private_.cursors[cur] = true

  return cursor_utils.rows(cur, fetch_mode, true)
end

function Connection:rows(...)  return Connection_private.rows(self, nil,  ...) end

function Connection:irows(...) return Connection_private.rows(self, 'n',  ...) end

function Connection:nrows(...) return Connection_private.rows(self, 'a',  ...) end

function Connection:trows(...) return Connection_private.rows(self, 'an', ...) end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Connection fetch

--- ���������� ������ ������ Recordset.
--
-- <br> ������������ Connection:each(sql,params,function(...) return ... end)
-- @see Connection:rows
-- @class function
-- @name Connection:first_row


-- fetch_mode, sql [,params] 
function Connection_private:first_row(fetch_mode, sql, params)
  local cur, err = Connection_private.execute(self, sql, params)
  if not cur then return nil, err end
  if 'userdata' ~= type(cur) then return nil, ERR_MSGS.no_cursor end
  return cursor_utils.fetch_row_destroy(cur, fetch_mode)
end

function Connection:first_row (...) return Connection_private.first_row(self, nil,  ...) end

function Connection:first_irow(...) return Connection_private.first_row(self, 'n',  ...) end

function Connection:first_nrow(...) return Connection_private.first_row(self, 'a',  ...) end

function Connection:first_trow(...) return Connection_private.first_row(self, 'an', ...) end

--- ���������� ������ �������� ������ ������.
--
-- <br> ������������ (Connection:first_row(sql,params)) � ������ �������� �� ������
-- @usage local cnt, err = db:first_value('select count(*) from Clients')
function Connection:first_value(...)
  local t, err = self:first_irow(...)
  if t then return t[1] end
  return nil, err
end


--- ���������� ������ ��������� �������
-- 
-- @param fetch_mode [required] 'a' - ����������� ������ 'n' - ������������ ������
-- @param sql [required] ����� �������
-- @param params [optional] ������� ���������� ��� �������
-- @return ������ �������
-- @see Connection:new
-- @see Environment.Environment:connection
function Connection:fetch_all(fetch_mode, sql, params)
  local cur, err = Connection_private.execute(self, sql, params)
  if not cur then return nil, err end
  if 'userdata' ~= type(cur) then return nil, ERR_MSGS.no_cursor end
  if cur.setdestroyonclose then cur:setdestroyonclose(true) end
  return cursor_utils.fetch_all(cur, fetch_mode, true)
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Connection config

function Connection:get_config(name )     return Connection_private.get_config_param(self, name)      end

function Connection:set_config(name, val) return Connection_private.set_config_param(self, name, val) end

function Connection_private:get_config_param(name)
  if self.private_.lib_opt then
    local val = self.private_.lib_opt[name]
    if val ~= nil then return val end
  end
  return self.private_.env:get_config(name)
end

function Connection_private:set_config_param(name, value)
  if not self.private_.lib_opt then
    self.private_.lib_opt = {}
  end
  self.private_.lib_opt[name] = value

  return Connection_private.get_config_param(self, name)
end

end
------------------------------------------------------------------

return Connection