-- =============================================================
--  fibbenchstorage.lua  -  Fibonacci RAM Storage Node
--  Run this on every non-master server in the rack.
--
--  Enhancements over v1:
--    • Auto-detects a Data Card (tier 2+) and uses deflate/inflate
--      to compress values before storing them in RAM.  Only applied
--      when the compressed form is actually smaller, and only for
--      values longer than COMPRESS_MIN bytes.
--    • Monitors RAM usage every PRESSURE_INTERVAL seconds and after
--      every STORE/CHUNK_END.  When usage exceeds RAM_THRESH (80 %)
--      entries are serialised to disk (largest first) until usage
--      drops back below the threshold.  Disk entries are tracked in
--      diskMeta so protocol counts (PING / STAT) remain accurate.
--
--  Protocol (all traffic on PORT 5757):
--    PING                           -> PONG  <free> <total> <count>
--    STORE   <idx> <value>          -> ACK   <idx>
--    CHUNK   <idx> <part> <total> <data>
--    CHUNK_END <idx>                -> ACK   <idx>
--    STAT                           -> STAT  <free> <total> <count>
--    QUIT                           (graceful shutdown)
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

-- ── Optional: Data Card (tier 2+ compression) ────────────────
--  Tier-1 data cards only have hash functions; deflate/inflate are
--  tier-2+ features.
--
--  Why not component.isAvailable / component.data?
--    In OC's proxy system, methods are wrapped callables whose type()
--    does not reliably return "function" — so a type() check can
--    silently fail even on a perfectly good tier-3 card.  Instead we
--    iterate component.list() (which works regardless of tier or slot)
--    and probe deflate with an actual pcall.
local dataCard       = nil
local useCompression = false
do
  for addr in component.list("data") do
    local ok, proxy = pcall(component.proxy, addr)
    if ok and proxy then
      -- Actually invoke deflate; if it succeeds the card is tier 2+.
      local probeOk, probeResult = pcall(proxy.deflate, "probe")
      if probeOk and type(probeResult) == "string" then
        dataCard     = proxy
        useCompression = true
        break
      end
    end
  end
end

local COMPRESS_MIN    = 64     -- bytes; don't bother below this size
local totalSavedBytes = 0      -- running tally of bytes saved

local function tryCompress(s)
  if not useCompression or #s < COMPRESS_MIN then return s, false end
  local ok, result = pcall(dataCard.deflate, s)
  if ok and result and #result < #s then
    totalSavedBytes = totalSavedBytes + (#s - #result)
    return result, true
  end
  return s, false           -- compression made it larger; store raw
end

local function tryDecompress(s, isCompressed)
  if not isCompressed then return s end
  local ok, result = pcall(dataCard.inflate, s)
  return (ok and result) and result or s
end

-- ── Disk offload ──────────────────────────────────────────────
--  When RAM exceeds RAM_THRESH we serialise in-memory entries to
--  DISK_PATH/<idx>.  Compressed entries are written as-is (the
--  compression flag is remembered in diskMeta so a future inflate
--  is possible if a retrieval command is ever added to the protocol).
local DISK_PATH  = "/home/fibcache/"
local diskMeta   = {}   -- [idx] = { compressed = bool }
local diskCount  = 0
local RAM_THRESH = 0.80
local offloadTotal = 0  -- cumulative entries ever offloaded to disk

local function ensureDiskPath()
  if not fs.exists(DISK_PATH) then
    local ok = pcall(fs.makeDirectory, DISK_PATH)
    return ok
  end
  return true
end

-- ── Core state ────────────────────────────────────────────────
--  store[idx] = { data = <string>, compressed = <bool> }
local store    = {}
local chunks   = {}    -- [idx] = { parts = {}, born = uptime }
local memCount = 0     -- entries currently in RAM
local count    = 0     -- total entries (RAM + disk); used in PING/STAT
local lastIdx  = 0     -- highest fib index received

local CHUNK_TTL      = 10.0
local lastSweep      = computer.uptime()
local SWEEP_INTERVAL = 5.0

local lastPressureCheck = computer.uptime()
local PRESSURE_INTERVAL = 3.0

local orphanDrops = 0
local myAddr      = modem.address

-- ── Color palette ─────────────────────────────────────────────
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
  op_chunk = 0xFFDD44,
  op_ping  = 0x33AAFF,
  compress = 0xAA88FF,   -- purple  - compression info
  disk     = 0xFF8844,   -- amber   - disk activity / counts
}

local statusMsg = "Waiting for master ...  (Ctrl-C to quit)"
local statusCol = C.status

local W, H = 80, 25
if gpu then W, H = gpu.getResolution() end

-- ── Display primitives ────────────────────────────────────────
local function cls()
  if gpu then
    gpu.setBackground(BG); gpu.setForeground(C.value)
    gpu.fill(1, 1, W, H, " ")
  else term.clear() end
end

local function put(x, y, text, fg, bg)
  if not gpu or y < 1 or y > H or x > W then return end
  gpu.setBackground(bg or BG); gpu.setForeground(fg or C.value)
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
  local pct    = (total > 0) and (1 - free / total) or 0
  local filled = math.max(0, math.min(barW, math.floor(pct * barW)))
  local col    = (pct < 0.5) and C.good or (pct < 0.8) and C.warn or C.bad
  put(x,              y, "[",                      C.border)
  put(x + 1,          y, ("#"):rep(filled),        col)
  put(x + 1 + filled, y, ("."):rep(barW - filled), C.dim)
  put(x + 1 + barW,   y, "]",                      C.border)
  put(x + barW + 2,   y, string.format("%3d%%", math.floor(pct * 100)), col)
end

-- ── Layout ────────────────────────────────────────────────────
--  Row  1   top border
--  Row  2   title / node address
--  Row  3   separator
--  Row  4   mem entries | disk entries | pending chunks | orphans
--  Row  5   last fib index received
--  Row  6   separator
--  Row  7   RAM bar
--  Row  8   compression status | total disk offloads
--  Row  9   separator
--  Row 10   status / last operation
--  Row 11   bottom border
local BAR_W = math.max(4, W - 15)

local function drawFrame()
  cls()
  hline(1)
  put(1, 2, "|", C.border)
  put(2, 2, pad("  STORAGE NODE  [" .. myAddr:sub(1, 8) .. "...]", W - 2), C.header)
  put(W, 2, "|", C.border)
  hline(3)
  for _, row in ipairs({4, 5, 7, 8, 10}) do
    put(1, row, "|", C.border); put(W, row, "|", C.border)
  end
  hline(6); hline(9); hline(11)
end

-- ── Disk offload helpers ──────────────────────────────────────
local function offloadEntry(idx)
  local entry = store[idx]
  if not entry or diskMeta[idx] then return false end
  if not ensureDiskPath() then return false end
  local path = DISK_PATH .. tostring(idx)
  local f = io.open(path, "wb")
  if not f then return false end
  local ok = pcall(function() f:write(entry.data) end)
  f:close()
  if not ok then return false end
  diskMeta[idx] = { compressed = entry.compressed }
  store[idx]    = nil
  memCount      = memCount - 1
  diskCount     = diskCount + 1
  offloadTotal  = offloadTotal + 1
  return true
end

-- Offload largest in-memory entries until RAM usage <= RAM_THRESH.
-- Returns the number of entries moved to disk this call.
local function checkRAMPressure()
  local free  = computer.freeMemory()
  local total = computer.totalMemory()
  if (1 - free / total) <= RAM_THRESH then return 0 end

  -- Gather candidates sorted by stored-byte size, largest first.
  -- Evicting big entries reclaims the most RAM per iteration.
  local candidates = {}
  for idx, entry in pairs(store) do
    candidates[#candidates + 1] = { idx = idx, size = #entry.data }
  end
  table.sort(candidates, function(a, b) return a.size > b.size end)

  local moved = 0
  for _, item in ipairs(candidates) do
    -- Re-sample freeMemory each loop; it rises as entries are cleared.
    if (1 - computer.freeMemory() / total) <= RAM_THRESH then break end
    if offloadEntry(item.idx) then moved = moved + 1 end
  end
  return moved
end

-- ── Chunk sweep ───────────────────────────────────────────────
local function sweepChunks()
  local now, expired = computer.uptime(), {}
  for idx, c in pairs(chunks) do
    if now - c.born > CHUNK_TTL then expired[#expired + 1] = idx end
  end
  for _, idx in ipairs(expired) do chunks[idx] = nil end
  return #expired
end

-- ── redraw ────────────────────────────────────────────────────
local function redraw()
  local free    = computer.freeMemory()
  local total   = computer.totalMemory()
  local pending = 0
  for _ in pairs(chunks) do pending = pending + 1 end

  -- Row 4: mem / disk / pending / orphans
  put(2,  4, " mem   : ", C.label)
  put(11, 4, pad(tostring(memCount), 7), C.number)
  put(19, 4, " disk  : ", C.label)
  put(28, 4, pad(tostring(diskCount), 6),
             diskCount > 0 and C.disk or C.dim)
  put(35, 4, " pend  : ", C.label)
  put(44, 4, pad(tostring(pending), 4),
             pending > 0 and C.warn or C.dim)
  put(49, 4, " orphan: ", C.label)
  put(58, 4, pad(tostring(orphanDrops), W - 59),
             orphanDrops > 0 and C.warn or C.dim)

  -- Row 5: last fib index
  put(2,  5, " last fib : ", C.label)
  local idxStr = lastIdx > 0
    and string.format("fib(%d)", lastIdx) or "none yet"
  put(14, 5, pad(idxStr, W - 15),
             lastIdx > 0 and C.number or C.dim)

  -- Row 7: RAM bar
  put(2, 7, " RAM  ", C.label)
  drawBar(8, 7, BAR_W, free, total)

  -- Row 8: compression info | disk offload tally
  put(2, 8, " cmp   : ", C.label)
  local cmpStr
  if useCompression then
    if totalSavedBytes >= 1048576 then
      cmpStr = string.format("ON  [saved %dMB]",
        math.floor(totalSavedBytes / 1048576))
    elseif totalSavedBytes >= 1024 then
      cmpStr = string.format("ON  [saved %dKB]",
        math.floor(totalSavedBytes / 1024))
    else
      cmpStr = string.format("ON  [saved %dB]", totalSavedBytes)
    end
  else
    cmpStr = "OFF  (no tier-2 data card)"
  end
  put(11, 8, pad(cmpStr, 28), useCompression and C.compress or C.dim)
  put(40, 8, " offloaded: ", C.label)
  put(52, 8, pad(tostring(offloadTotal), W - 53),
             offloadTotal > 0 and C.disk or C.dim)

  -- Row 10: status
  put(2,  10, " status : ", C.label)
  put(12, 10, pad(statusMsg, W - 13), statusCol)
end

-- ── Helpers ───────────────────────────────────────────────────
-- Store a (possibly compressed) value and update counters.
local function storeValue(idx, rawVal)
  local data, compressed = tryCompress(rawVal)
  store[idx]  = { data = data, compressed = compressed }
  memCount    = memCount + 1
  count       = count   + 1
  if idx > lastIdx then lastIdx = idx end
end

-- Run a pressure check and update statusMsg / statusCol if entries
-- were moved to disk.  Returns the number of entries offloaded.
local function pressureCheck()
  local n = checkRAMPressure()
  if n > 0 then
    statusMsg = string.format(
      "RAM >80%%: offloaded %d entr%s to disk  (total %d)",
      n, n == 1 and "y" or "ies", offloadTotal)
    statusCol = C.disk
  end
  return n
end

-- ── Initial draw ──────────────────────────────────────────────
drawFrame()
redraw()

-- ── Event loop ────────────────────────────────────────────────
local running = true
local function onInterrupt() running = false end
event.listen("interrupted", onInterrupt)

while running do
  local ev, _, sender, port, _, cmd, a1, a2, a3, a4 =
    event.pull(2, "modem_message")

  if ev and port == PORT then

    -- ── Discovery ────────────────────────────────────────────
    if cmd == "PING" then
      modem.send(sender, PORT, "PONG",
        computer.freeMemory(), computer.totalMemory(), count)
      statusMsg = string.format("PING from %.8s...", sender)
      statusCol = C.op_ping

    -- ── Small value store ────────────────────────────────────
    elseif cmd == "STORE" then
      local idx = tonumber(a1)
      if idx then
        storeValue(idx, a2 or "")
        modem.send(sender, PORT, "ACK", idx)
        local cmpFlag = store[idx] and store[idx].compressed
        statusMsg = string.format(
          "STORE  fib(%d)  [%d digits%s]",
          idx, #(a2 or ""), cmpFlag and "  cmp" or "")
        statusCol = C.op_store
        pressureCheck()
      end

    -- ── Chunked store (accumulate parts) ─────────────────────
    elseif cmd == "CHUNK" then
      local idx, part = tonumber(a1), tonumber(a2)
      if idx and part then
        if not chunks[idx] then
          chunks[idx] = { parts = {}, born = computer.uptime() }
        end
        chunks[idx].parts[part] = a4
        statusMsg = string.format(
          "CHUNK  fib(%d)  part %d/%s", idx, part, tostring(a3))
        statusCol = C.op_chunk
      end

    -- ── Finalise chunked store ───────────────────────────────
    elseif cmd == "CHUNK_END" then
      local idx = tonumber(a1)
      if idx and chunks[idx] then
        local parts     = chunks[idx].parts
        local assembled = {}
        for i = 1, #parts do assembled[i] = parts[i] or "" end
        local raw    = table.concat(assembled)
        chunks[idx]  = nil
        storeValue(idx, raw)
        -- Capture compression flag before a potential offload evicts it.
        local cmpFlag = store[idx] and store[idx].compressed
        modem.send(sender, PORT, "ACK", idx)
        statusMsg = string.format(
          "CHUNK_END  fib(%d)  %d parts  [%d digits%s]",
          idx, #parts, #raw, cmpFlag and "  cmp" or "")
        statusCol = C.op_store
        pressureCheck()
      end

    -- ── Stat request ─────────────────────────────────────────
    elseif cmd == "STAT" then
      modem.send(sender, PORT, "STAT",
        computer.freeMemory(), computer.totalMemory(), count)

    -- ── Graceful shutdown ────────────────────────────────────
    elseif cmd == "QUIT" then
      running = false
    end

    redraw()

  else
    -- Idle tick: run orphan sweep and periodic pressure check.
    local now = computer.uptime()

    if now - lastSweep >= SWEEP_INTERVAL then
      drawFrame()
      lastSweep = now
      local dropped = sweepChunks()
      if dropped > 0 then
        orphanDrops = orphanDrops + dropped
        statusMsg = string.format(
          "swept %d orphaned chunk set(s)  (total dropped: %d)",
          dropped, orphanDrops)
        statusCol = C.warn
      end
    end

    if now - lastPressureCheck >= PRESSURE_INTERVAL then
      lastPressureCheck = now
      pressureCheck()
    end

    redraw()
  end
end

event.ignore("interrupted", onInterrupt)
modem.close(PORT)
if gpu then gpu.setForeground(C.value); gpu.setBackground(BG) end
print("\n[Storage] Shut down cleanly.")