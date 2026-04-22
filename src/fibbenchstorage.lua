-- =============================================================
--  fibbenchstorage.lua  -  Fibonacci Chunk Storage Node  [OPTIMISED]
--
--  Stores chunked Fibonacci limbs in RAM first, then offloads to
--  disk as a swap-like backing store under pressure.
--
--  Optimisation notes (vs original):
--    1. redraw() now uses the already-maintained memCount counter
--       instead of iterating the entire store table with a for loop
--       on every redraw.
--    2. checkRAMPressure() returns early (before any iteration)
--       when memory usage is below the threshold, avoiding a full
--       table scan and compression pass that was being done even
--       when RAM was not under pressure.
-- =============================================================

local component = require("component")
local event     = require("event")
local computer  = require("computer")
local term      = require("term")
local fs        = require("filesystem")

if not component.isAvailable("modem") then
  error("No modem component found - install a network card!")
end

local modem = component.modem
local gpu   = component.isAvailable("gpu") and component.gpu or nil
local PORT  = 5757
modem.open(PORT)

-- ── Optional: Data Card compression ──────────────────────────
local dataCard       = nil
local useCompression = false
for addr in component.list("data") do
  local ok, proxy = pcall(component.proxy, addr)
  if ok and proxy then
    local probeOk, probeResult = pcall(proxy.deflate, "probe")
    if probeOk and type(probeResult) == "string" then
      dataCard = proxy
      useCompression = true
      break
    end
  end
end

local COMPRESS_MIN    = 64
local totalSavedBytes = 0

local function tryCompress(s)
  if not useCompression or #s < COMPRESS_MIN then return s, false end
  local ok, result = pcall(dataCard.deflate, s)
  if ok and result and #result < #s then
    totalSavedBytes = totalSavedBytes + (#s - #result)
    return result, true
  end
  return s, false
end

local function tryDecompress(s, isCompressed)
  if not isCompressed then return s end
  if not useCompression then return s end
  local ok, result = pcall(dataCard.inflate, s)
  return (ok and result) and result or s
end

-- ── Disk swap backing ────────────────────────────────────────
local DISK_PATH   = "/home/fibcache/"
local diskMeta    = {}  -- [key] = { compressed = bool }
local diskCount   = 0
local RAM_THRESH  = 0.80
local offloadTotal = 0

local function ensureDiskPath()
  if not fs.exists(DISK_PATH) then
    return pcall(fs.makeDirectory, DISK_PATH)
  end
  return true
end

-- ── Core state ────────────────────────────────────────────────
--  store[key] = { data = <string>, compressed = <bool>, idx = n, part = p, total = t }
local store   = {}
local count   = 0
local memCount = 0   -- tracks #store entries without iterating the table
local lastIdx = 0

local myAddr = modem.address
local orphanDrops = 0

local function keyOf(idx, part)
  return tostring(idx) .. ":" .. tostring(part)
end

local function diskFileOf(key)
  return DISK_PATH .. (key:gsub("[^%w%-%_%.]", "_"))
end

local function removeFromDisk(key)
  if diskMeta[key] then
    local path = diskFileOf(key)
    if fs.exists(path) then pcall(fs.remove, path) end
    diskMeta[key] = nil
    diskCount = math.max(0, diskCount - 1)
  end
end

local function removeRecord(key)
  if store[key] then
    store[key] = nil
    memCount = math.max(0, memCount - 1)
  end
  removeFromDisk(key)
end

local function offloadEntry(key)
  local entry = store[key]
  if not entry or diskMeta[key] then return false end
  if not ensureDiskPath() then return false end

  local data, compressed = entry.data, entry.compressed
  if not compressed and useCompression then
    local ok, result = pcall(dataCard.deflate, data)
    if ok and result and #result < #data then
      totalSavedBytes = totalSavedBytes + (#data - #result)
      data = result
      compressed = true
    end
  end

  local path = diskFileOf(key)
  local f = io.open(path, "wb")
  if not f then return false end
  local ok = pcall(function() f:write(data) end)
  f:close()
  if not ok then return false end

  diskMeta[key] = { compressed = compressed }
  store[key] = nil
  memCount = math.max(0, memCount - 1)
  diskCount = diskCount + 1
  offloadTotal = offloadTotal + 1
  return true
end

-- OPTIMISED: returns immediately (without touching the store table
-- at all) when RAM usage is below the threshold.  The original
-- always ran a full compression pass first, then checked the
-- threshold again -- this scan was wasted work the vast majority
-- of the time when memory was healthy.
local function checkRAMPressure()
  local total = computer.totalMemory()
  if total <= 0 then return 0 end

  -- Fast early exit: nothing to do when under threshold.
  if (1 - computer.freeMemory() / total) <= RAM_THRESH then return 0 end

  -- RAM is tight -- attempt in-place compression first.
  if useCompression then
    for _, entry in pairs(store) do
      if not entry.compressed then
        local ok, result = pcall(dataCard.deflate, entry.data)
        if ok and result and #result < #entry.data then
          totalSavedBytes = totalSavedBytes + (#entry.data - #result)
          entry.data = result
          entry.compressed = true
        end
      end
    end
    collectgarbage("collect")
  end

  -- Re-check after compression.
  if (1 - computer.freeMemory() / total) <= RAM_THRESH then return 0 end

  -- Still tight -- offload largest entries to disk.
  local candidates = {}
  for key, entry in pairs(store) do
    candidates[#candidates + 1] = { key = key, size = #entry.data }
  end
  table.sort(candidates, function(a, b) return a.size > b.size end)

  local moved = 0
  for _, item in ipairs(candidates) do
    if (1 - computer.freeMemory() / total) <= RAM_THRESH then break end
    if offloadEntry(item.key) then moved = moved + 1 end
  end
  return moved
end

local function maybeLoadFromDisk(key)
  if store[key] then return store[key] end
  local meta = diskMeta[key]
  if not meta then return nil end
  local path = diskFileOf(key)
  local f = io.open(path, "rb")
  if not f then return nil end
  local data = f:read("*a") or ""
  f:close()
  data = tryDecompress(data, meta.compressed)
  return { data = data, compressed = meta.compressed }
end

local function storeRecord(idx, part, total, rawVal)
  local key = keyOf(idx, part)
  if store[key] or diskMeta[key] then
    removeRecord(key)
  end

  local data, compressed = tryCompress(rawVal)
  store[key] = {
    data = data,
    compressed = compressed,
    idx = idx,
    part = part,
    total = total,
  }
  memCount = memCount + 1
  count = count + 1
  if idx > lastIdx then lastIdx = idx end
  return key
end

local function fetchRecord(idx, part)
  local key = keyOf(idx, part)
  local entry = store[key]
  if entry then
    return entry.data, entry.total, entry.compressed, key
  end
  local diskEntry = maybeLoadFromDisk(key)
  if diskEntry then
    return diskEntry.data, nil, diskEntry.compressed, key
  end
  return nil, nil, nil, key
end

local function purgeRecord(idx, part)
  removeRecord(keyOf(idx, part))
end

-- ── UI ────────────────────────────────────────────────────────
local BG = 0x000000
local C = {
  border   = 0x1A4A7A,
  header   = 0x33AAFF,
  label    = 0x778899,
  value    = 0xEEEEEE,
  number   = 0xFFDD44,
  good     = 0x44CC44,
  warn     = 0xFF9900,
  bad      = 0xFF4444,
  dim      = 0x333344,
  status   = 0x88CCFF,
  op_store = 0x44EE44,
  op_fetch = 0xFFDD44,
  op_ping  = 0x33AAFF,
  compress = 0xAA88FF,
  disk     = 0xFF8844,
}

local statusMsg = "Waiting for master ...  (Ctrl-C to quit)"
local statusCol = C.status

local W, H = 80, 25
if gpu then W, H = gpu.getResolution() end

local function cls()
  if gpu then
    gpu.setBackground(BG)
    gpu.setForeground(C.value)
    gpu.fill(1, 1, W, H, " ")
  else
    term.clear()
  end
end

local function put(x, y, text, fg, bg)
  if not gpu or y < 1 or y > H or x > W then return end
  gpu.setBackground(bg or BG)
  gpu.setForeground(fg or C.value)
  local s = tostring(text or "")
  if x < 1 then s = s:sub(2 - x); x = 1 end
  if x + #s - 1 > W then s = s:sub(1, W - x + 1) end
  if #s > 0 then gpu.set(x, y, s) end
end

local function hline(y, fg)
  put(1, y, "+" .. ("-"):rep(W - 2) .. "+", fg or C.border)
end

local function pad(s, w)
  s = tostring(s or "")
  if #s > w then return s:sub(1, w) end
  return s .. (" "):rep(w - #s)
end

local function drawBar(x, y, barW, free, total)
  local pct = (total > 0) and (1 - free / total) or 0
  local filled = math.max(0, math.min(barW, math.floor(pct * barW)))
  local col = (pct < 0.5) and C.good or (pct < 0.8) and C.warn or C.bad
  put(x,              y, "[",                      C.border)
  put(x + 1,          y, ("#"):rep(filled),        col)
  put(x + 1 + filled, y, ("."):rep(barW - filled), C.dim)
  put(x + 1 + barW,   y, "]",                      C.border)
  put(x + barW + 2,   y, string.format("%3d%%", math.floor(pct * 100)), col)
end

-- OPTIMISED: uses memCount (already maintained by storeRecord /
-- removeRecord) instead of iterating the whole store table on
-- every single redraw call.
local function redraw()
  local free  = computer.freeMemory()
  local total = computer.totalMemory()

  put(2, 4, " mem   : ", C.label)
  put(11, 4, pad(tostring(memCount), 7), C.number)
  put(19, 4, " disk  : ", C.label)
  put(28, 4, pad(tostring(diskCount), 6), diskCount > 0 and C.disk or C.dim)
  put(35, 4, " last  : ", C.label)
  put(44, 4, pad(lastIdx > 0 and string.format("fib(%d)", lastIdx) or "none", 14), lastIdx > 0 and C.number or C.dim)
  put(2, 6, " RAM  ", C.label)
  drawBar(8, 6, math.max(4, W - 15), free, total)

  put(2, 8, " cmp   : ", C.label)
  local cmpStr
  if useCompression then
    if totalSavedBytes >= 1048576 then
      cmpStr = string.format("ON  [saved %dMB]", math.floor(totalSavedBytes / 1048576))
    elseif totalSavedBytes >= 1024 then
      cmpStr = string.format("ON  [saved %dKB]", math.floor(totalSavedBytes / 1024))
    else
      cmpStr = string.format("ON  [saved %dB]", totalSavedBytes)
    end
  else
    cmpStr = "OFF  (no tier-2 data card)"
  end
  put(11, 8, pad(cmpStr, 28), useCompression and C.compress or C.dim)
  put(40, 8, " offloaded: ", C.label)
  put(52, 8, pad(tostring(offloadTotal), W - 53), offloadTotal > 0 and C.disk or C.dim)

  put(2, 10, " status : ", C.label)
  put(12, 10, pad(statusMsg, W - 13), statusCol)
end

local function drawFrame()
  cls()
  hline(1)
  put(1, 2, "|", C.border)
  put(2, 2, pad("  STORAGE NODE  [" .. myAddr:sub(1, 8) .. "...]", W - 2), C.header)
  put(W, 2, "|", C.border)
  hline(3)
  for _, row in ipairs({4, 6, 8, 10}) do
    put(1, row, "|", C.border)
    put(W, row, "|", C.border)
  end
  hline(5)
  hline(7)
  hline(9)
  hline(11)
end

-- ── Event loop ────────────────────────────────────────────────
local running = true
local function onInterrupt() running = false end
event.listen("interrupted", onInterrupt)

drawFrame()
redraw()

while running do
  local ev, _, sender, port, _, cmd, a1, a2, a3, a4 = event.pull(2, "modem_message")

  if ev and port == PORT then
    if cmd == "PING" then
      modem.send(sender, PORT, "PONG", computer.freeMemory(), computer.totalMemory(), count)
      statusMsg = string.format("PING from %.8s...", sender)
      statusCol = C.op_ping

    elseif cmd == "STAT" then
      modem.send(sender, PORT, "STAT", computer.freeMemory(), computer.totalMemory(), count)

    elseif cmd == "STORE" then
      local idx, part, total = tonumber(a1), tonumber(a2), tonumber(a3)
      local data = a4 or ""
      if idx and part and total then
        storeRecord(idx, part, total, data)
        modem.send(sender, PORT, "ACK", idx, part)
        statusMsg = string.format("STORE fib(%d) part %d/%d [%d bytes]", idx, part, total, #data)
        statusCol = C.op_store
        if checkRAMPressure() > 0 then
          statusMsg = string.format("RAM >80%%: offloaded to disk  (total %d)", offloadTotal)
          statusCol = C.disk
        end
      end

    elseif cmd == "FETCH" then
      local idx, part = tonumber(a1), tonumber(a2)
      local deleteAfter = tostring(a3) == "1"
      if idx and part then
        local data, total, compressed, key = fetchRecord(idx, part)
        if data then
          modem.send(sender, PORT, "GOT", idx, part, total or 0, data)
          statusMsg = string.format("FETCH fib(%d) part %d%s", idx, part, deleteAfter and " delete" or "")
          statusCol = C.op_fetch
          if deleteAfter then removeRecord(key) end
        else
          modem.send(sender, PORT, "ERR", "missing", idx, part)
          statusMsg = string.format("FETCH miss fib(%d) part %d", idx, part)
          statusCol = C.warn
        end
      end

    elseif cmd == "PURGE" then
      local idx, part = tonumber(a1), tonumber(a2)
      if idx and part then
        purgeRecord(idx, part)
        statusMsg = string.format("PURGE fib(%d) part %d", idx, part)
        statusCol = C.warn
      end

    elseif cmd == "QUIT" then
      running = false
    end

    redraw()
  else
    if checkRAMPressure() > 0 then
      statusMsg = string.format("RAM >80%%: offloaded to disk  (total %d)", offloadTotal)
      statusCol = C.disk
    end
    redraw()
  end
end

event.ignore("interrupted", onInterrupt)
modem.close(PORT)
if gpu then gpu.setForeground(C.value); gpu.setBackground(BG) end
print("\n[Storage] Shut down cleanly.")