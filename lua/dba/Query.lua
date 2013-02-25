--- Implement query class
-- @class module
-- @name Query

local utils  = require "dba.utils"

local ERR_MSGS     = assert(utils.ERR_MSGS)
local cursor_utils = assert(utils.cursor_utils)
local param_utils  = assert(utils.param_utils)

--- 
-- @type Query

local Query = {} Query.__index = Query
local Query_private = {}

------------------------------------------------------------------
do -- ctor/dtor/close/open

--- Создается новый объект @{Query.Query|Query}.
-- 
-- @local
--
-- @param cnn [required] открытый объект Connection
-- @param sql [optional] текст запроса
-- @param params [optional] таблица параметров для запроса
-- @return объект @{Query.Query|Query}
function Query:new(cnn, sql, params)
  assert(cnn)
  local hcnn = assert(cnn:handle())
  local stmt
  if hcnn.statement then
    local err stmt, err = hcnn:statement()
    if not stmt then return nil, err end
  end

  local t = setmetatable({
    private_ = {
      cnn  = cnn;
      stmt = stmt;
    }
  },self)

  if sql then
    local ok, err = t:set_sql(sql)
    if not ok then 
      t:destroy()
      return nil, err
    end
  end

  if params then
    local ok, err = t:bind(params)
    if not ok then 
      t:destroy()
      return nil, err
    end
  end

  return t
end

--- Уничтожает объект Query
--
-- @see Connection.Connection:query
-- @see Connection.Connection:prepare
function Query:destroy()
  self:close()
  if self.private_.stmt then
    self.private_.stmt:destroy()
  end
  self.private_.stmt = nil
  self.private_.cnn  = nil
  return true
end

--- Закрывает открытый курсор.
-- 
function Query:close()
  if self:closed() then 
    self.private_.cur = nil
    return true 
  end
  local cur = assert(self.private_.cur)
  cur:close()
  self.private_.cur = nil
end

--- Возвращает статус курсора.
-- 
function Query:closed()
  local cur = self.private_.cur
  if not cur then return true end
  return cursor_utils.closed(cur)
end

--- Возвращает статус курсора.
-- 
function Query:opened()
  return not self:closed()
end

--- Возвращает плотформозависимый дескриптор.
-- 
-- @return handle
function Query:handle()
  return self.private_.stmt
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Query prepare

--- Prepare query.
--
-- <br>Если библиотека или драйвер не поддерживают подготовленные запросы, то
-- используется подстановка параметров в текст при выполнении. При этом 
-- функция возвращает успех, но Query:prepared возвращает false.
-- @param sql [optional] текст запроса
-- @return true в случае удачи
function Query:prepare(sql)
  local ok, err 
  if sql then
    ok, err = self:set_sql(sql)
    if not ok then return nil, err end
  end

  if self:get_config('FORCE_REPLACE_PARAMS') then
    -- возможна только эмуляция во время вызова 
    return true
  end

  -- явно поддерживаются подгатовленные запросы и они разрешены
  if self:supports_prepare() then
    ok, err = self.private_.stmt:prepare(self.private_.translated_sql or self.private_.sql)
    if not ok then return nil, err end
    return true
  end

  -- возможна только эмуляция во время вызова 
  return true
end

--- Возвращает признак того что запрос подготовлен.
--
function Query:prepared()
  return (self.private_.stmt ~= nil) and (self.private_.stmt:prepared())
end

--- Unprepare query.
--
function Query:unprepare()
  self:close() -- or error 
  if self:prepared() then
    self.private_.stmt:reset()
  end
  return true
end

--- 
--
function Query:supports_prepare()
  if not (self.private_.stmt and self.private_.stmt.prepare) then 
    return false
  end
  return true
  -- return self.private_.cnn:supports_prepare()
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Query bind/execute

function Query_private:need_replace_params()
  if Query_private.get_config_param(self,'FORCE_REPLACE_PARAMS') then
    return true
  end
  if self.private_.stmt == nil then return true end
  return false
end

function Query_private:execute(sql, params)
  assert((sql == nil) or type(sql) == 'string')
  assert((params == nil) or type(params) == 'table')

  if sql then 
    local ok, err = self:set_sql(sql)
    if not ok then return nil, err end
  end

  if not self.private_.sql then return nil, ERR_MSGS.no_sql_text end

  if type(params) == 'table' then
    ok, err = self:bind(params)
    if not ok then return nil, err end
  end

  if not Query_private.need_replace_params(self) then
    -- 1. поддержка явных ODBC statement'ов
    -- запрос подгатовлен
    if self:prepared() then return self.private_.stmt:execute() end

    -- 2. поддержка явных ODBC statement'ов
    -- запрос не подгатовлен
    assert(self.private_.stmt)
    return self.private_.stmt:execute(self.private_.translated_sql or self.private_.sql)
  end

  -- 3. Параметры с заменой
  if (self.private_.params) and (not self:get_config('IGNORE_NAMED_PARAMS')) then
    local q = param_utils.apply_params(self.private_.sql, self.private_.params)
    if self.private_.stmt then return self.private_.stmt:execute(q) end
    return self.private_.cnn:handle():execute(q)
  end

  -- 4. запрос без параметров
  if self.private_.stmt then return self.private_.stmt:execute(self.private_.sql) end
  return self.private_.cnn:handle():execute(self.private_.sql)
end

--- Устанавливает текст запроса.
--
-- <br> После изменения SQL все параметры становятся невалидными.
-- <br> Запрос не должен быть открыт или подготовлен.
-- @param sql текст запроса
-- @return true в случае удачи
function Query:set_sql(sql)
  assert(type(sql) == 'string')

  if self:prepared() then return nil, ERR_MSGS.query_prepared end
  if self:opened()   then return nil, ERR_MSGS.query_opened   end

  local psql, plst
  -- явно поддерживаются параметры в запросе и они разрешены
  if not Query_private.need_replace_params(self) then
    -- для поддержки именованных параметров необходимо преобразовать запрос
    -- преобразование именованных параметров
    if not self:get_config('IGNORE_NAMED_PARAMS') then
      psql, plst = param_utils.translate_params(sql)
      if not psql then return nil, plst end 
    end
  end

  if self.private_.stmt then self.private_.stmt:reset() end

  self.private_.sql                     = sql
  self.private_.translated_sql          = psql

  -- массив для соответствия номера парамеира - имени
  -- Если не используется явный bind, то эта таблица не обязательна
  self.private_.translated_param_list   = plst

  -- используется для хранения значений параметров для их
  -- дальнейшей подстановки. Если используется явный bind, 
  -- то эта таблица не обязательна
  self.private_.params                  = nil

  return true
end

--- Присваивает значение параметру.
--
-- <br> В качестве значения можно указывать 2 специальные константы PARAM_NULL и PARAM_DEFAULT.
-- @param paramID номер параметра (начиная с 1) или имя параметра
-- @param value   значение параметра. 
-- @class function 
-- @name Query:bind[1]

--- Присваивает значение параметру.
--
-- <br> Эта функция может использоватся только если параметризованные запросы поддерживается драйвером.
-- <br> В качестве значения можно указывать 2 специальные константы PARAM_NULL и PARAM_DEFAULT.
-- @param paramID   номер параметра (начиная с 1) или имя параметра
-- @param func  Функция используется для получения данных в момент выполнения запроса.
-- <br>Для строк может выдавать данные порциями. Конец данных наступает либо в момент возврата nil если 
-- не использовался параметр len, либо после передачи len байт если был указан параметр len.
-- @param len [optional] реальная длинна данных.
-- @class function 
-- @name Query:bind[2]

--- Присваивает значение параметрам.
--
-- @param params - таблица параметров(название/номер => значение)
-- @class function 
-- @name Query:bind[3]

--
function Query:bind(paramID, val, ...)
  local paramID_type = type(paramID)
  assert((paramID_type == 'string')or(paramID_type == 'number')or(paramID_type == 'table'))

  if self:opened() then  return nil, ERR_MSGS.query_opened end

  -- нет поддержки явного bind или необходимо использовать подстановку
  if Query_private.need_replace_params(self) then 
    -- поддерживаются только именованные параметры
    if Query_private.get_config_param(self,'IGNORE_NAMED_PARAMS') then
      return nil, ERR_MSGS.deny_named_params
    end

    self.private_.params = self.private_.params or {}
    -- Сохраняем все параметры для дальнейшей подстановки
    if paramID_type == 'table' then
      for k, v in pairs(paramID) do self.private_.params[k] = v  end
    else
      if paramID_type == 'number' then return nil, ERR_MSGS.pos_params_unsupport end
      self.private_.params[ paramID ] = val
    end

    return true
  end

  assert(self.private_.stmt)
  assert(self.private_.stmt.bind)

  if 'number' == paramID_type then
    return param_utils.bind_param(self.private_.stmt, paramID, val, ...)
  end

  if paramID_type == 'table' then
    local k, v = next(paramID)
    if (type(k) == 'string') and (Query_private.get_config_param(self,'IGNORE_NAMED_PARAMS')) then
      -- в одном запросе нельзя совмещать позиционные и именованные параметры
      -- поэтому определяем тип параметров по одному ключу
      return nil, ERR_MSGS.deny_named_params
    end

    if type(k) == 'number' then
      -- можно заполнить не все подряд параметры, поэтому не используем ipairs
      for k, v in pairs(paramID)do
        local ok, err = param_utils.bind_param(self.private_.stmt, k, v)
        if not ok then return nil, err, k end
      end
    elseif type(k) == 'string' then
      -- эта проверка возвращает ошибку если нет ни одного параметра
      -- но попытка присвоить несуществующие параметры не вызывает ошибку
      -- наверное стоит синхронизировать поведение
      if not self.private_.translated_param_list then 
        return nil, ERR_MSGS.unknown_parameter .. k
      end

      for k, v in pairs(paramID) do
        for i, name in ipairs(self.private_.translated_param_list) do
          if name == k then
            local ok, err = param_utils.bind_param(self.private_.stmt, i, v)
            if not ok then return nil, err, i end
          end
        end
      end
    end
    return true
  end

  assert(paramID_type == 'string')
  if Query_private.get_config_param(self,'IGNORE_NAMED_PARAMS') then
    return nil, ERR_MSGS.deny_named_params
  end

  if (not self.private_.translated_param_list) or (not param_utils.ifind(paramID, self.private_.translated_param_list)) then 
    -- в запросе нет именованного параметра с таким именем
    return nil, ERR_MSGS.unknown_parameter .. paramID
  end

  -- параметр с одним именем может встречатся несколько раз
  for i, name in ipairs(self.private_.translated_param_list) do
    if name == paramID then
      local ok, err = param_utils.bind_param(self.private_.stmt, i, val, ...)
      if not ok then return nil, err, i end
    end
  end

  return true
end

--- Открывает курсор.
--
-- @param sql [optional] текст запроса
-- @param param [optional] параметры запроса
-- @see Query:closed
-- @see Query:close
function Query:open(sql, param)
  -- запрос выполняется всегда
  -- возможно только отменить транзакцию если нет autocommit

  if self:opened() then return nil, ERR_MSGS.query_opened end
  if param == nil then
    if type(sql) == 'table' then 
      param = sql
      sql   = nil
    end
  end

  local cur, err = Query_private.execute(self, sql, param)
  if not cur then return nil, err end
  if not cursor_utils.is_cursor(cur) then return nil, ERR_MSGS.no_cursor end
  self.private_.cur = cur
  return true
end

--- Выполняет запрос который не должен возвращать Recordset.
--
-- <br> Если запрос вернул курсор, то он закрывается, но не производится откат транзакции
-- @param sql [optional] текст запроса
-- @param params [optional] параметры для запроса
-- @return количество записей задействованных в запросе,
-- @see Connection.Connection:query
-- @see Connection.Connection:prepare
-- @see Query:set_sql
-- @see Query:prepare
function Query:exec(sql, params)
  if self:opened() then return nil, ERR_MSGS.query_opened end
  if type(sql) == 'table' then
    assert(params == nil)
    params = sql
    sql = nil
  end

  local res, err = Query_private.execute(self, sql, params)
  if not res then return nil, err end
  if cursor_utils.is_cursor(res) then
    res:close()
    return nil, ERR_MSGS.ret_cursor
  end
  return res
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Query iterator

--- Итератор для перебора Recordset.
--
-- <br> 
-- @param sql [optional] текст запроса
-- @param params [optional] параметры для запроса
-- @param autoclose [optional] признак того что запрос должен быть закрыт перез завершением функции.
-- @param fn [required] callback 
-- @see dba.callback_function
-- @class function
-- @name Query:each


-- fetch_mode, [sql,] [params,] [autoclose,] fn
function Query_private:each(fetch_mode, ...)
  if self:opened() then 
    if (type(...) == 'string') or (type(...) == 'table') then
      return nil, ERR_MSGS.query_opened
    end
    return cursor_utils.foreach(self.private_.cur, fetch_mode, ...)
  end

  local n = 1
  local sql, params  = ...
  if type(sql) == 'string' then
    n = n + 1
    if type(params) == 'table' then 
      n = n + 1
    else 
      params = nil
    end
  elseif type(sql) == 'table' then
    params = nil
    n = n + 1
  else 
    sql, params = nil
  end

  local ok, err = self:open(sql, params)
  if not ok then return nil, err end

  return cursor_utils.foreach(self.private_.cur, fetch_mode, select(n, ...))
end

function Query:each(...)  return Query_private.each(self, nil,  ...) end

function Query:ieach(...) return Query_private.each(self, 'n',  ...) end

function Query:neach(...) return Query_private.each(self, 'a',  ...) end

function Query:teach(...) return Query_private.each(self, 'an', ...) end

--- Итератор для перебора Recordset.
--
-- <br> Итератор для generic for
-- @param sql [optional] текст запроса
-- @param params [optional] параметры для запроса
--
-- @class function
-- @name Query:rows

-- fetch_mode, [sql,] [params,] 
function Query_private:rows(fetch_mode, ...)
  if self:closed() then 
    local ok, err
    local sql, params = ...
    if type(sql) then 
      if type(params) ~= 'table' then params = nil end
      ok, err = self:open(sql, params)
    else ok, err = self:open() end
    if not ok then error(tostring(err)) end
  else
    if (type(...) == 'string') or (type(...) == 'table') then
      error(ERR_MSGS.query_opened)
    end
  end

  local cur = assert(self.private_.cur)
  return cursor_utils.rows(cur, fetch_mode)
end

function Query:rows(...)  return Query_private.rows(self, nil,  ...) end

function Query:irows(...) return Query_private.rows(self, 'n',  ...) end

function Query:nrows(...) return Query_private.rows(self, 'a',  ...) end

function Query:trows(...) return Query_private.rows(self, 'an', ...) end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Query fetch

--- Возвращает первую строку Recordset.
--
-- <br> Эквивалентна Query:each(sql,params,function(...) return ... end)
-- @see Query:rows
-- @class function
-- @name Query:first_row

-- fitch_mode, [sql,] [params]
function Query_private:first_row(fetch_mode, sql, params)
  assert(self:closed())
  if type(sql) == 'table' then
    assert(params == nil)
    params, sql = sql
  end
  local cur, err = Query_private.execute(self, sql, params)
  if not cur then return nil, err end
  if not cursor_utils.is_cursor(cur) then return nil, ERR_MSGS.no_cursor end

  return cursor_utils.fetch_row(cur, fetch_mode, true)
end

function Query:first_row(...)  return Query_private.first_row(self, nil,  ...) end

function Query:first_irow(...) return Query_private.first_row(self, 'n',  ...) end

function Query:first_nrow(...) return Query_private.first_row(self, 'a',  ...) end

function Query:first_trow(...) return Query_private.first_row(self, 'an', ...) end


--- Возвращает первое значение первой записи.
--
-- <br> Эквивалентна (Query:first_row(sql,params)) с учетом проверки на ошибки
-- @see Query:first_row
function Query:first_value(...)
  local t, err = self:first_irow(...)
  if t then return t[1] end
  return nil, err
end

--- Возвращает полный результат запроса
-- 
-- @param fetch_mode [required] 'a' - именованные записи 'n' - нимерованные записи
-- @param sql [optional] текст запроса
-- @param params [optional] таблица параметров для запроса
-- @return массив записей
function Query:fetch_all(fetch_mode, sql, params)
  assert(self:closed())
  if type(sql) == 'table' then
    assert(params == nil)
    params, sql = sql
  end
  local cur, err = Query_private.execute(self, sql, params)
  if not cur then return nil, err end
  if not cursor_utils.is_cursor(cur) then return nil, ERR_MSGS.no_cursor end

  return cursor_utils.fetch_all(cur, fetch_mode)
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Query config

function Query:get_config(name )     return Query_private.get_config_param(self, name)      end

function Query:set_config(name, val) return Query_private.set_config_param(self, name, val) end

function Query_private:get_config_param(name)
  if self.private_.lib_opt then
    local val = self.private_.lib_opt[name]
    if val ~= nil then return val end
  end
  return self.private_.cnn:get_config(name)
end

function Query_private:set_config_param(name, value)
  if not self.private_.lib_opt then
    self.private_.lib_opt = {}
  end
  self.private_.lib_opt[name] = value

  return Query_private.get_config_param(self, name)
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Query multiresultset

--- Переключает курсор не следующий Recordset.
-- <br>Курсор должен быть открыт.
-- @return ничего если нет следующего Recordset
-- @return курсор если есть следующий Recordset
-- @see Query:open
-- @see Query:closed
-- @see Query:close
-- @see Query:each
-- @see Query:rows
function Query:next_resultset() 
  if self:closed() then return nil, ERR_MSGS.query_not_opened end
  if not self.private_.cur.nextresultset then return nil, ERR_MSGS.not_support end
  return self.private_.cur:nextresultset()
end

function Query:set_autoclose(value)
  if self.private_.stmt then self.private_.stmt:setautoclose(value) end
  self.private_.autoclose = value
end

function Query:get_autoclose()
  if self.private_.stmt then return self.private_.stmt:getautoclose(value) end
  return self.private_.autoclose
end

end
------------------------------------------------------------------

return Query