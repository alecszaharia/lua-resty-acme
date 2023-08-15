local ok, lfs = pcall(require, 'lfs_ffi')
local util = require "resty.acme.util"
local basexx = require( "basexx" )
local log = util.log

local ngx_ERR = ngx.ERR
local ngx_WARN = ngx.WARN
local ngx_INFO = ngx.INFO
local ngx_DEBUG = ngx.DEBUG

local DIRSEP = package.config:sub(1,1) -- handle Windows or Unix

if not ok then
  local _
  _, lfs = pcall(require, 'lfs')
end

local _M = {}
local mt = {__index = _M}

local TTL_SEPERATOR = '::'
local TTL_PATTERN = "(%d+)" .. TTL_SEPERATOR .. "(.+)"

function _M.new(conf)
  local dir = conf and conf.dir
  dir = dir or os.getenv("TMPDIR") or '/tmp'

  local self =
    setmetatable(
    {
      dir = dir
    },
    mt
  )
  return self
end

local function generate_path_from_key(s)
  local path = DIRSEP
  local parts = {}

  for key in string.gmatch(s, "(.[^:]+):?") do
    table.insert(parts,key);
  end

  local l = table.getn(parts)

  for key,value in pairs(parts) do
    if l>key then
      path=path..value..DIRSEP
    end
    if l==key then
      local lp = string.sub(basexx.to_base32(value),1,3)
      local last_part = lp;
      path=path..last_part
    end
  end
  return path
end

local function generate_path_from_key_prefix(s)
  local path = DIRSEP
  local parts = {}

  for key in string.gmatch(s, "(.[^:]+):?") do
    table.insert(parts,key);
  end

  local l = table.getn(parts)

  for key,value in pairs(parts) do
      path=path..value..DIRSEP
  end
  return path
end

local function regulate_filename(dir, s)
  local path = generate_path_from_key(s)
  local abs_path = dir..path
  return abs_path ..DIRSEP .. basexx.to_base32(s), abs_path
end

local function exists(f)
  -- TODO: check for existence, not just able to open or not
  local f, err = io.open(f, "rb")
  if f then
    f:close()
  end
  return err == nil
end

local function split_ttl(s)
  local _, _, ttl, value = string.find(s, TTL_PATTERN)

  return tonumber(ttl), value
end

local function check_expiration(f)
  if not exists(f) then
    return
  end

  local file, err = io.open(f, "rb")
  if err then
    return nil, err
  end

  local output, err = file:read("*a")
  file:close()

  if err then
    return nil, err
  end

  local ttl, value = split_ttl(output)

  -- ttl is nil meaning the file is corrupted or in legacy format
  -- ttl = 0 means the key never expires
  if not ttl or (ttl > 0 and ngx.time() - ttl >= 0) then
    os.remove(f)
  else
    return value
  end
end

function _M:add(k, v, ttl)
  local f = regulate_filename(self.dir, k)

  local check = check_expiration(f)

  if check then
    return "exists"
  end

  return self:set(k, v, ttl)
end

function _M:set(k, v, ttl)
  local f,p = regulate_filename(self.dir, k)

  -- remove old keys if it's expired
  check_expiration(f)

  if ttl then
    ttl = math.floor(ttl + ngx.time())
  else
    ttl = 0
  end

  os.execute('mkdir -p '..p)

  local file, err = io.open(f, "wb")

  if err then
    return err
  end
  local _, err = file:write(ttl .. TTL_SEPERATOR .. v)

  if err then
    return err
  end
  file:close()
end

function _M:delete(k)
  local f = regulate_filename(self.dir, k)
  if not exists(f) then
    return nil, nil
  end
  local _, err = os.remove(f)
  if err then
    return err
  end
end

function _M:get(k)
  local f = regulate_filename(self.dir, k)
  local value, err = check_expiration(f)
  if err then
    return nil, err
  elseif value then
    return value, nil
  else
    return nil
  end
end

function _M:list(prefix)
  if not lfs then
    return {}, "lfs_ffi needed for file:list"
  end

  local files = {}

  local prefix_len = prefix and #prefix or 0
  local path=self.dir..generate_path_from_key_prefix(prefix)

  for _,file in ipairs(listfiles(path)) do
    if prefix_len == 0 or string.sub(file, 1, prefix_len) == prefix then
      table.insert(files, file)
    end
  end
  return files
end

function listfiles(dir,list)
  list = list or {}	-- use provided list or create a new one

  if not dir_exists_v1(dir) then
    return list
  end

  for entry in lfs.dir(dir) do
    if entry ~= "." and entry ~= ".." then
      local next_dir = dir..entry;
      -- log(ngx_DEBUG,next_dir)
      if lfs.attributes(next_dir).mode == 'directory' then
        listfiles(next_dir.."/",list)
      else
        local file = basexx.from_base32(entry)
        if not file then
          goto nextfile
        end
        table.insert(list,file)
      end
    end
    ::nextfile::
  end

  return list
end

function dir_exists_v1(path)
  if (lfs.attributes(path, "mode") == "directory") then
    return true
  end
  return false
end


return _M
