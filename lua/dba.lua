--- Модуль lua-dba
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

local PARAM_DEFAULT = assert(utils.PARAM_DEFAULT)
local PARAM_NULL    = assert(utils.PARAM_NULL)

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

--- Создает объект @{Environment.Environment|Environment}.
-- 
-- @class function
-- @name Environment

--- Создает объект @{Connection.Connection|Connection}.
-- 
-- При этом для этого подключения создается собственный объект @{Environment.Environment|Environment}
-- который уничтажается вместе с объектом @{Connection.Connection|Connection}.
-- @class function
-- @name Connection

--- Создает объект @{Connection.Connection|Connection} и подключает его к БД.
-- 
-- Если не удалось подключится к БД то объeкт @{Connection.Connection|Connection} уничтожается.
-- @class function
-- @name Connect


--- Таблица параметров библиотеки.
-- Каждый параметр может быть установлен на уровне Environment, Connection или Query
--
-- @class table
-- @name OPTIONS
-- @field FORCE_REPLACE_PARAMS всегда заменять параметры подстановкой строки. 
-- Этот параметр может использоватся например для выполнения batch запросов с параметрами.
-- @field IGNORE_NAMED_PARAMS  не пытатся преобразовывать именованные параметры
-- это необходимо для предотвращения изменения текста SQL перед выполнением
-- при этом параметры будут поддерживатся только если проддерживается bind(будут использоваться только '?')
-- @usage
-- local sql = [[begin if :ID > 5 then select 5 else selct 0 end if end]]
-- qry:set_config('FORCE_REPLACE_PARAMS', true)
-- qry:rows({ID=10}, print)
-- @usage
-- local sql = [[select 'hello :world']]
-- qry:set_config('IGNORE_NAMED_PARAMS', true)
-- qry:rows(print)

--- callback функция для перебора запесей набора.
-- <br> Если функция возвращает любое значение, то перебор прекращается
-- и возвращенные(все) значения становятся результатом функции, которая осуществляет перебор.
-- <br> Запись в функцию передается либо в виде набора параметров либо в виде таблицы.
-- <br> В функцию может передаватся одна и таже таблица с разными значениями.
-- @class function
-- @name callback_function
-- @param row or list - очередная запись
-- @see Connection.Connection:each
-- @see Query.Query:each

--- Структура, описывающая отдельный драйвер.
-- @class table
-- @name driverinfo
-- @tfield string 1 название драйвера
-- @tfield table 2 набор параметров 
-- @see odbc.Environment.Environment:drivers

--- Структура, описывающая отдельный драйвер
-- @class table
-- @name dsninfo
-- @tfield string 1 название DSN
-- @tfield string 2 название драйвера
-- @see odbc.Environment.Environment:datasources

--- Уровни изоляции транзакций
-- @class table
-- @name transaction_level
-- @field 1 "TRANSACTION_NONE"
-- @field 2 "TRANSACTION_READ_UNCOMMITTED"
-- @field 3 "TRANSACTION_READ_COMMITTED"
-- @field 4 "TRANSACTION_REPEATABLE_READ"
-- @field 5 "TRANSACTION_SERIALIZABLE"
-- @see odbc.Connection.Connection:supports_transaction
-- @see odbc.Connection.Connection:set_transaction_level
-- @see odbc.Connection.Connection:get_transaction_level
