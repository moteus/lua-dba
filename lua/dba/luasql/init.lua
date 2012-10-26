local dba = require "dba"
return {
  load = function (driver_name)
    local luasql = require ("luasql." .. driver_name)
    local ctor = assert(luasql[driver_name])
    return dba.load(ctor)
  end
}