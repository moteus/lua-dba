Usage
-----

```lua
  ---[[ use lua-odbc
  local dba = require "dba.odbc"
  local cnn = dba.Connect('emptydb')
  --]]

  --[[ use luasql.sqlite3
  -- emulation of parameters and prepare
  local dba = require "dba.luasql".load('sqlite3')
  local cnn = dba.Connect('./test.db')
  --]]

  local sql = "select ID, Name from Agent where ID = :ID"
  local par = {ID=1}

  cnn:each(sql, par, print)
  for ID,Name in cnn:rows(sql, par) do
    print(ID, Name)
  end

  local qry = cnn:query(sql)
  qry:each(par, print)

  print(qry:first_value('select count(*) from Agent'))

  qry:prepare(sql)
  for i = 1, 3 do
    print(i, qry:first_row{ID=i})
  end

  qry:destroy()
  cnn:destroy()
```