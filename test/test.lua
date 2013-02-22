require "pprint"
local dbassert = require "odbc".assert

local DBNAME = [[:memory:]]
local odbc_params = {
  Driver   = "SQLite3 ODBC Driver";
  Database = DBNAME;
}

local odbc = require "dba.odbc"
local lsql = require "dba.luasql".load('sqlite3')

function init_db(cnn)
  cnn:exec[[create table Agent(
      ID INTEGER PRIMARY KEY,
      Name char(32)
  )]]
  for i = 1, 10 do
    cnn:exec(
      string.format("insert into Agent(ID,NAME)values(%d, 'Agent#%d')", i, i)
    )
  end
end

local sql = "select NULL, ID, Name from Agent where ID>:ID"
local par = {ID=1}

ocnn = dbassert(odbc.Connect(odbc_params))
lcnn = assert(lsql.Connect(DBNAME))

init_db(ocnn)
init_db(lcnn)

ocnn:tables(print)

ocnn:each(sql, par, print)
lcnn:each(sql, par, print)

oqry = ocnn:query(sql)
lqry = lcnn:query(sql)

oqry:each(par, print)
lqry:each(par, print)

oqry:destroy()
lqry:destroy()

oqry = ocnn:query(sql, par)
lqry = lcnn:query(sql, par)

oqry:each(print)
lqry:each(print)

oqry:destroy()
lqry:destroy()

pprint{ocnn:fetch_all('an', sql, par)}
pprint{lcnn:fetch_all('an', sql, par)}

ocnn:destroy()
lcnn:destroy()