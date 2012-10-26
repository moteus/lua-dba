local PARAM_DEFAULT = {}
local PARAM_NULL = {}

local OPTIONS = {
  -- всегда заменять параметры подстановкой строки
  FORCE_REPLACE_PARAMS = false;

  -- не пытатся преобразовывать именованные параметры
  -- это необходимо для предотвращения изменения текста SQL перед выполнением
  -- при этом параметры будут поддерживатся только если проддерживается bind(будут использоваться только '?')
  IGNORE_NAMED_PARAMS = false;
};

local ERR_MSGS = {
  unsolved_parameter   = 'unsolved name of parameter: ';
  unknown_parameter    = 'unknown parameter: ';
  no_cursor            = 'query has not returned a cursor';
  ret_cursor           = 'query has returned a cursor';
  query_opened         = 'query is already opened';
  cnn_not_opened       = 'connection is not opened';
  query_not_opened     = 'query is not opened';
  query_prepared       = 'query is already prepared';
  deny_named_params    = 'named parameters are denied';
  no_sql_text          = 'SQL text was not set';
  pos_params_unsupport = 'positional parameters are not supported';
  not_support          = 'not supported';
  unknown_txn_lvl      = 'unknown transaction level: '; 
};

local unpack = unpack or table.unpack

local function pack_n(...)
  return {n = select('#', ...), ...}
end

local function unpack_n(t, s)
  return unpack(t, s or 1, t.n or #t)
end


------------------------------------------------------------------
local cursor_utils = {} do

function cursor_utils.colcount(cur)
  return #((cur.colnames or cur.getcolnames)(cur))
end

function cursor_utils.closed(cur)
  if cur.destroyed then return cur:destroyed() or cur:closed() end
  return not not tostring(cur):find('closed')
end

function cursor_utils.destroyed(cur)
  if cur.destroyed then return cur:destroyed() end
  return not not tostring(cur):find('closed')
end

function cursor_utils.destroy(cur)
  if cur.destroy then return cur:destroy() end
  return cur:close()
end

-- cur, [fetch_mode,] [autoclose,] fn
function cursor_utils.foreach(cur, ...)
  if cur.foreach then return cur:foreach(...) end

  local fetch_mode, autoclose, fn
  local n = select('#', ...)
  if n >= 3 then fetch_mode, autoclose, fn = ...
  elseif n == 2 then
    fetch_mode, fn = ...
    if type(fetch_mode) == 'boolean' then
      autoclose, fetch_mode = fetch_mode
    end
  else
    assert(n == 1)
    fn = ...
  end

  local do_ = function()
    local res, err = {}
    local n = cursor_utils.colcount(cur)
    while(1)do
      res, err = cur:fetch(res, fetch_mode or 'n')
      if res == nil then 
        if err ~= nil then return {nil, err} end
        return {}
      end

      local t
      if fetch_mode then t = pack_n(fn(res))
      else t = pack_n(fn(unpack(res, 1, n))) end

      if t.n > 0 then return t end
    end
  end

  if not fn then fn = autoclose end

  local ok, t 
  if (autoclose == nil) or autoclose then
    ok, t = pcall(do_)
    cur:close()
    if not ok then error(t) end
  else t = do_() end

  return unpack_n(t)
end

function cursor_utils.rows(cur, fetch_mode, close)
  local res = {}
  if fetch_mode then
    return function ()
      local res, err = cur:fetch(res, fetch_mode)
      if res then return res end
      if err then error(tostring(err)) end
      if close and not cursor_utils.closed(cur) then cur:close() end
    end
  end

  local n = cur.colnames and #cur:colnames() or #cur:getcolnames()
  return function ()
    local res, err = cur:fetch(res, 'n')
    if res then return unpack(res, 1, n) end
    if err then error(tostring(err)) end
    if close and not cursor_utils.closed(cur) then cur:close() end
  end
end

function cursor_utils.fetch_row(cur, fetch_mode, close)
  local res = {}
  if fetch_mode then
    local res, err = cur:fetch(res, fetch_mode)
    if close and not cursor_utils.closed(cur) then cur:close() end
    if res then return res end
    return nil, err
  end

  local n = cursor_utils.colcount(cur)
  local res, err = cur:fetch(res, 'n')
  if close and not cursor_utils.closed(cur) then cur:close() end
  if res then return unpack(res, 1, n) end
  return nil, err
end

function cursor_utils.fetch_all(cur, fetch_mode, close)
  assert(fetch_mode)
  local t, err = {}
  while true do
    local res 
    res, err = cur:fetch({}, fetch_mode)
    if not res then 
      if close and not cursor_utils.closed(cur) then cur:close() end
      break
    end
    table.insert(t, res)
  end
  if err then return nil, err, t end
  return t
end

end
------------------------------------------------------------------

------------------------------------------------------------------
local connect_utils = {} do

function connect_utils.connected(cnn)
  if cnn.destroyed then return (not cnn:destroyed()) and cnn:connected() end
  return not tostring(cnn):find('closed')
end

function connect_utils.destroyed(cnn)
  if cnn.destroyed then return cnn:destroyed() end
  return not not tostring(cnn):find('closed')
end

function connect_utils.connect(obj, ...)
  local dsn, lgn, pwd, autocommit = ...
  local cnndrv_params
  if type(dsn) == 'table' then
    if not obj.driverconnect then return nil, ERR_MSGS.not_support end
    cnndrv_params, autocommit = ...
  else
    if type(lgn) == 'boolean' then
      assert(pwd == nil)
      assert(autocommit == nil)
      autocommit = lgn
      lgn = nil
    elseif type(pwd) == 'boolean' then
      assert(autocommit == nil)
      autocommit = pwd
      pwd = nil
    end
  end

  if autocommit == nil then autocommit = true end

  local cnn, err
  if cnndrv_params then cnn, err = obj:driverconnect(cnndrv_params)
  else cnn, err  = obj:connect(dsn, lgn or "", pwd or "") end

  if not cnn then return nil, err end
  cnn:setautocommit(autocommit)

  return cnn, err
end

function connect_utils.execute(cnn, sql)
  if cnn.execute then return cnn:execute(sql) end

  local stmt, err = cnn:statement()
  if not stmt then return nil, err end
  local ok, err = stmt:execute(sql)
  if not ok then
    stmt:destroy()
    return nil, err
  end

  if ok == stmt then return stmt end
  stmt:destroy()
  return ok
end

end
------------------------------------------------------------------

------------------------------------------------------------------
local param_utils = {} do

--
-- заключает строку в ковычки
--
function param_utils.quoted (s,q) return (q .. string.gsub(s, q, q..q) .. q) end

--
--
--
function param_utils.bool2sql(v) return v and 1 or 0 end

--
-- 
--
function param_utils.num2sql(v)  return tostring(v) end

--
-- 
--
function param_utils.str2sql(v, q) return param_utils.quoted(v, q or "'") end

--
-- возвращает индекс значения val в массиве t
--
function param_utils.ifind(val,t)
  for i,v in ipairs(t) do
    if v == val then
      return i
    end
  end
end

--
-- паттерн для происка именованных параметров в запросе
--
param_utils.param_pattern = "[:]([^%d%s][%a%d_]+)"

--
-- Подставляет именованные параметры
--
-- @param sql      - текст запроса
-- @param params   - таблица значений параметров
-- @return         - новый текст запроса
--
function param_utils.apply_params(sql, params)
  params = params or {}
  -- if params[1] ~= nil then return nil, ERR_MSGS.pos_params_unsupport end

  local err
  local str = string.gsub(sql,param_utils.param_pattern,function(param)
    local v = params[param]
    local tv = type(v)
    if    ("number"      == tv)then return param_utils.num2sql (v)
    elseif("string"      == tv)then return param_utils.str2sql (v)
    elseif("boolean"     == tv)then return param_utils.bool2sql(v)
    elseif(PARAM_NULL    ==  v)then return 'NULL'
    elseif(PARAM_DEFAULT ==  v)then return 'DEFAULT'
    end
    err = ERR_MSGS.unknown_parameter .. param
  end)
  if err then return nil, err end
  return str
end

--
-- Преобразует именованные параметры в ?
-- 
-- @param sql      - текст запроса
-- @param parnames - таблица разрешонных параметров
--                 - true - разрешены все имена
-- @return  новый текст запроса
-- @return  массив имен параметров. Индекс - номер по порядку данного параметра
--
function param_utils.translate_params(sql,parnames)
  if parnames == nil then parnames = true end
  assert(type(parnames) == 'table' or (parnames == true))
  local param_list={}
  local err
  local function replace()
    local function t1(param)
      -- assert(type(parnames) == 'table')
      if not param_utils.ifind(param, parnames) then
        err = ERR_MSGS.unsolved_parameter .. param
        return
      end
      table.insert(param_list, param)
      return '?'
    end

    local function t2(param)
      -- assert(parnames == true)
      table.insert(param_list, param)
      return '?'
    end

    return (parnames == true) and t2 or t1
  end

  local str = string.gsub(sql,param_utils.param_pattern,replace())
  if err then return nil, err end
  return str, param_list;
end

function param_utils.bind_param(stmt, i, val, ...)
  if     val == PARAM_NULL    then return stmt:bindnull(i)
  elseif val == PARAM_DEFAULT then return stmt:binddefault(i)
  else return stmt:bind(i, val, ...) end
end

end
------------------------------------------------------------------

return {
  OPTIONS       = OPTIONS;
  ERR_MSGS      = ERR_MSGS;
  cursor_utils  = cursor_utils;
  param_utils   = param_utils;
  connect_utils = connect_utils;
  unpack        = unpack;
  pack_n        = pack_n;
  unpack_n      = unpack_n;
  PARAM_DEFAULT = PARAM_DEFAULT;
  PARAM_NULL    = PARAM_NULL;
}