-- fibbenchstorage.lua
--
-- FibBench STORAGE node.
--
-- Role: holds numeric limb-chunks for the master's growing Fibonacci
-- values on disk, so no single machine ever has to keep a whole huge
-- number in RAM. On startup it lets you pick which mounted disk(s)
-- to use. Chunks are placed across your chosen disks by a deterministic
-- hash of their filename, so the same file always resolves to the same
-- disk without needing a persisted index.
--
-- Run with: fibbenchstorage

local component = require("component")
local event     = require("event")
local filesystem = require("filesystem")
local serialization = require("serialization")
local term = require("term")
local computer = require("computer")

local scriptDir = (...) and (...):match("(.*/)") or ""
if scriptDir == "" then
  local ok, shell = pcall(require, "shell")
  if ok then
    local resolved = shell.resolve("fibbenchstorage.lua")
    if resolved then scriptDir = resolved:match("(.*/)") or "" end
  end
end
local common = dofile(scriptDir .. "fibbenchcommon.lua")
local ui, net, util = common.ui, common.net, common.util
local keys = net.keyboard.keys

------------------------------------------------------------------
-- Disk selection wizard
------------------------------------------------------------------

local function discoverMounts()
  local list = {}
  for proxy, path in filesystem.mounts() do
    local label = "(no label)"
    local ok, l = pcall(proxy.getLabel)
    if ok and l and l ~= "" then label = l end
    local isTmp = (label:lower():find("tmpfs") ~= nil)
    local total, used = 0, 0
    pcall(function() total = proxy.spaceTotal() or 0 end)
    pcall(function() used = proxy.spaceUsed() or 0 end)
    if not (total ~= total) then -- guard NaN
      list[#list + 1] = {
        proxy = proxy, path = path, label = label,
        total = total, used = used, tmpfs = isTmp,
        isBoot = (path == "/"),
      }
    end
  end
  table.sort(list, function(a, b) return a.path < b.path end)
  return list
end

local function runDiskWizard()
  term.clear()
  local gpu, w, h = ui.init("FibBench Storage Node - Disk Selection")
  ui.footer("Type disk numbers separated by commas, then press Enter (e.g. 1,3)")

  local mounts = discoverMounts()
  ui.box(2, 3, w - 2, h - 6, "Available mounted filesystems")
  local y = 5
  ui.text(4, y, "#   Path                 Label                 Free / Total    Notes", ui.palette.dim)
  y = y + 1
  for i, m in ipairs(mounts) do
    local free = m.total - m.used
    local notes = {}
    if m.isBoot then notes[#notes+1] = "boot/OS disk" end
    if m.tmpfs then notes[#notes+1] = "tmpfs (NOT persistent!)" end
    local line = string.format("%-3d %-20s %-20s %10s/%-9s %s",
      i, m.path:sub(1, 20), m.label:sub(1, 20),
      util.formatBytes(free), util.formatBytes(m.total),
      table.concat(notes, ", "))
    local color = ui.palette.text
    if m.tmpfs then color = ui.palette.warn end
    if m.isBoot then color = ui.palette.dim end
    ui.text(4, y, line, color)
    y = y + 1
  end

  if #mounts == 0 then
    ui.text(4, y + 1, "No filesystems found! Attach a disk and restart.", ui.palette.bad)
    error("no filesystems available")
  end

  term.setCursor(4, h - 2)
  gpu.setForeground(ui.palette.accent)
  io.write("Select disk(s) for FibBench chunk storage: ")
  gpu.setForeground(ui.palette.text)
  local input
  while true do
    input = term.read()
    if input then input = input:gsub("%s", "") end
    if input and input ~= "" then
      local chosen = {}
      local valid = true
      for numStr in input:gmatch("[^,]+") do
        local idx = tonumber(numStr)
        if not idx or not mounts[idx] then
          valid = false
        else
          chosen[#chosen + 1] = mounts[idx]
        end
      end
      if valid and #chosen > 0 then
        return chosen
      end
    end
    io.write("Invalid selection, try again (e.g. 1 or 1,2): ")
  end
end

------------------------------------------------------------------
-- Chunk storage backend
------------------------------------------------------------------

local CHUNK_DIR = "fibbench_chunks"

local Storage = {}
Storage.__index = Storage

function Storage.new(disks)
  local self = setmetatable({}, Storage)
  self.disks = disks
  self.reads, self.writes, self.deletes = 0, 0, 0
  for _, d in ipairs(disks) do
    local dirPath = filesystem.concat(d.path, CHUNK_DIR)
    if not filesystem.exists(dirPath) then
      filesystem.makeDirectory(dirPath)
    end
  end
  return self
end

local function hashFilename(name, n)
  local h = 0
  for i = 1, #name do h = (h * 31 + name:byte(i)) % 2147483647 end
  return (h % n) + 1
end

function Storage:diskFor(filename)
  return self.disks[hashFilename(filename, #self.disks)]
end

function Storage:fullPath(filename)
  local d = self:diskFor(filename)
  return filesystem.concat(d.path, CHUNK_DIR, filename), d
end

function Storage:store(filename, data)
  local path = self:fullPath(filename)
  local f, err = io.open(path, "w")
  if not f then return false, err end
  f:write(serialization.serialize(data))
  f:close()
  self.writes = self.writes + 1
  return true
end

function Storage:fetch(filename)
  local path = self:fullPath(filename)
  local f = io.open(path, "r")
  if not f then return nil, "missing chunk: " .. filename end
  local raw = f:read("*a")
  f:close()
  local ok, data = pcall(serialization.unserialize, raw)
  if not ok then return nil, "corrupt chunk: " .. filename end
  self.reads = self.reads + 1
  return data
end

function Storage:delete(filename)
  local path = self:fullPath(filename)
  if filesystem.exists(path) then
    filesystem.remove(path)
  end
  self.deletes = self.deletes + 1
  return true
end

-- Inventory scan (for master resume support): returns list of filenames
-- currently present across all selected disks.
function Storage:inventory()
  local files = {}
  for _, d in ipairs(self.disks) do
    local dirPath = filesystem.concat(d.path, CHUNK_DIR)
    if filesystem.exists(dirPath) then
      for name in filesystem.list(dirPath) do
        if not name:match("/$") then
          files[#files + 1] = name
        end
      end
    end
  end
  return files
end

function Storage:usageSnapshot()
  local snap = {}
  for i, d in ipairs(self.disks) do
    local total, used = d.total, 0
    pcall(function() used = d.proxy.spaceUsed() or 0 end)
    pcall(function() total = d.proxy.spaceTotal() or total end)
    snap[i] = { path = d.path, label = d.label, used = used, total = total }
  end
  return snap
end

------------------------------------------------------------------
-- Main
------------------------------------------------------------------

local disks = runDiskWizard()
local storage = Storage.new(disks)

local gpu, W, H = ui.init("FibBench Storage Node")
local logPanel = ui.newLog(3, 16, W - 4, H - 18)
ui.box(2, 15, W - 2, H - 16, "Log")
ui.box(2, 2, W - 2, 5, "Connection")
ui.box(2, 8, W - 2, 6, "Disks")

local modems = net.openModems()
local myId = net.myAddress(modems)
if #modems == 0 then
  logPanel:push("WARNING: no modem found - cannot join a network.", ui.palette.bad)
end

local state = {
  masterAddr = nil,
  masterName = nil,
  seriesId = nil,
  connected = false,
  lastMasterSeen = 0,
}

local function drawConnection()
  ui.clearArea(4, 3, W - 6, 3)
  if state.connected then
    ui.text(4, 3, "Status:  CONNECTED", ui.palette.good)
    ui.text(4, 4, "Master:  " .. util.shortId(state.masterAddr) .. "  (" .. (state.masterName or "?") .. ")", ui.palette.text)
    ui.text(4, 5, "Series:  " .. (state.seriesId or "-"), ui.palette.dim)
  else
    ui.text(4, 3, "Status:  SEARCHING FOR MASTER...", ui.palette.warn)
    ui.text(4, 4, "My address: " .. util.shortId(myId), ui.palette.dim)
  end
end

local function drawDisks()
  ui.clearArea(4, 9, W - 6, 4)
  local snap = storage:usageSnapshot()
  local y = 9
  for i, d in ipairs(snap) do
    if y > 12 then break end
    local frac = d.total > 0 and (d.used / d.total) or 0
    local label = string.format("%d) %s [%s]", i, d.path, d.label)
    ui.text(4, y, label:sub(1, 40), ui.palette.text)
    ui.progressBar(46, y, W - 50, frac, frac > 0.85 and ui.palette.bad or ui.palette.accent)
    y = y + 1
  end
end

local function drawFooter()
  ui.footer(string.format(
    "reads:%d  writes:%d  deletes:%d   |   q = quit and deregister",
    storage.reads, storage.writes, storage.deletes))
end

drawConnection()
drawDisks()
drawFooter()
logPanel:push("Storage node ready. Disks: " .. #disks .. ". Address: " .. util.shortId(myId), ui.palette.accent)

local function sendHello()
  local inv = storage:inventory()
  net.broadcast(modems, {
    type = "hello_storage",
    role = "storage",
    id = myId,
    disks = (function()
      local out = {}
      for _, d in ipairs(disks) do out[#out+1] = { path = d.path, label = d.label } end
      return out
    end)(),
    inventoryCount = #inv,
    inventory = inv,
  })
end

local lastHelloOrHeartbeat = 0

local function handleMessage(msg, remoteAddr)
  if msg.type == "welcome" and msg.to == myId then
    state.connected = true
    state.masterAddr = remoteAddr
    state.masterName = msg.name
    state.seriesId = msg.seriesId
    state.lastMasterSeen = computer.uptime()
    logPanel:push("Registered with master " .. util.shortId(remoteAddr) .. " as " .. tostring(msg.name), ui.palette.good)
    drawConnection()

  elseif msg.type == "master_shutdown" then
    if state.connected and remoteAddr == state.masterAddr then
      logPanel:push("Master shut down. Returning to search mode.", ui.palette.warn)
      state.connected = false
      state.masterAddr = nil
      drawConnection()
    end

  elseif msg.type == "fetch_chunk" and msg.path then
    state.lastMasterSeen = computer.uptime()
    local data, err = storage:fetch(msg.path)
    net.send(modems, msg.from or remoteAddr, {
      type = "chunk_data", replyId = msg.replyId, ok = (data ~= nil), data = data, err = err,
    })
    logPanel:push("fetch " .. msg.path .. (data and " OK" or (" FAIL: " .. tostring(err))),
      data and ui.palette.text or ui.palette.bad)
    drawFooter()

  elseif msg.type == "store_chunk" and msg.path then
    state.lastMasterSeen = computer.uptime()
    local ok, err = storage:store(msg.path, msg.data)
    net.send(modems, msg.from or remoteAddr, {
      type = "store_ack", replyId = msg.replyId, ok = ok, err = err,
    })
    logPanel:push("store " .. msg.path .. (ok and " OK" or (" FAIL: " .. tostring(err))),
      ok and ui.palette.text or ui.palette.bad)
    drawFooter()
    drawDisks()

  elseif msg.type == "delete_chunk" and msg.path then
    state.lastMasterSeen = computer.uptime()
    storage:delete(msg.path)
    if msg.replyId then
      net.send(modems, msg.from or remoteAddr, { type = "delete_ack", replyId = msg.replyId, ok = true })
    end
    drawFooter()
    drawDisks()
  end
end

sendHello()

local running = true
while running do
  local timeout = 2
  local msg, remoteAddr = net.pull(timeout)
  if msg then
    local ok, err = pcall(handleMessage, msg, remoteAddr)
    if not ok then
      logPanel:push("handler error: " .. tostring(err), ui.palette.bad)
    end
  end

  local now = computer.uptime()
  if not state.connected and (now - lastHelloOrHeartbeat) >= 2 then
    sendHello()
    lastHelloOrHeartbeat = now
  elseif state.connected then
    if (now - lastHelloOrHeartbeat) >= common.HEARTBEAT_INTERVAL then
      net.send(modems, state.masterAddr, {
        type = "heartbeat", id = myId, role = "storage",
        stats = { reads = storage.reads, writes = storage.writes, deletes = storage.deletes },
      })
      lastHelloOrHeartbeat = now
    end
    if state.lastMasterSeen > 0 and (now - state.lastMasterSeen) > common.HEARTBEAT_TIMEOUT then
      logPanel:push("Lost contact with master. Returning to search mode.", ui.palette.warn)
      state.connected = false
      state.masterAddr = nil
      drawConnection()
    end
  end

  local ev, _, _char, code = event.pull(0, "key_down")
  if ev and code == keys.q then
    running = false
  end
end

if state.connected then
  net.send(modems, state.masterAddr, { type = "bye", id = myId, role = "storage" })
end
term.clear()
print("FibBench storage node stopped.")