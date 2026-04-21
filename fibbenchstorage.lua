-- =============================================================
--  fibbenchstorage.lua  –  Fibonacci RAM Storage Node
--  Run this on every non-master server in the rack.
--
--  Protocol (all traffic on PORT 5757):
--    PING                           → PONG  <free> <total> <count>
--    STORE   <idx> <value>          → ACK   <idx>          (small values)
--    CHUNK   <idx> <part> <total> <data>                   (large values,
--    CHUNK   <idx> <part> <total> <data>  ...               no ACK per chunk)
--    CHUNK_END <idx>                → ACK   <idx>          (last chunk)
--    STAT                           → STAT  <free> <total> <count>
--    QUIT                           (graceful shutdown)
-- =============================================================

local component = require("component")
local event     = require("event")
local computer  = require("computer")
local term      = require("term")

-- ── Sanity check ─────────────────────────────────────────────
if not component.isAvailable("modem") then
  error("No modem component found – install a network card!")
end

local modem  = component.modem
local PORT   = 5757

modem.open(PORT)

-- ── State ─────────────────────────────────────────────────────
local store      = {}   -- [fibIndex (number)] = valueString
local chunks     = {}   -- [fibIndex] = { parts={}, born=uptime }
local count      = 0    -- total complete entries stored
local myAddr     = modem.address

-- Incomplete chunk sets older than this are considered orphaned
-- (e.g. master restarted mid-transmission) and will be discarded.
local CHUNK_TTL     = 10.0   -- seconds
local lastSweep     = computer.uptime()
local SWEEP_INTERVAL = 5.0   -- how often to run the sweep

local function sweepChunks()
  local now     = computer.uptime()
  local expired = {}
  for idx, c in pairs(chunks) do
    if now - c.born > CHUNK_TTL then
      expired[#expired + 1] = idx
    end
  end
  for _, idx in ipairs(expired) do
    chunks[idx] = nil
  end
  return #expired   -- returns how many were dropped (for display)
end

-- ── Display ───────────────────────────────────────────────────
local W = 40
if component.isAvailable("gpu") then
  W = component.gpu.getResolution()
end

local function bar(free, total)
  local pct   = 1 - free / total
  local bW    = W - 16
  local fill  = math.floor(pct * bW)
  return string.format("[%s%s] %3d%%",
    ("#"):rep(fill), ("."):rep(bW - fill),
    math.floor(pct * 100))
end

local orphanDrops = 0   -- cumulative orphaned chunk sets discarded

local function redraw()
  local free    = computer.freeMemory()
  local total   = computer.totalMemory()
  local pending = 0
  for _ in pairs(chunks) do pending = pending + 1 end

  term.setCursor(1, 1)
  -- Use io.write + manual padding so each line is exactly W chars
  -- and never wraps or leaves stale characters from a previous draw.
  local function ln(s)
    s = tostring(s or "")
    if #s < W then s = s .. (" "):rep(W - #s) end
    io.write(s:sub(1, W) .. "\n")
  end
  ln(("-"):rep(W))
  ln(string.format(" Storage Node  %.8s...", myAddr))
  ln(("-"):rep(W))
  ln(string.format(" Stored entries  : %-8d", count))
  ln(string.format(" Pending chunks  : %-4d  (orphans dropped: %d)",
    pending, orphanDrops))
  ln(string.format(" RAM free / total: %.0f / %.0f KiB",
    free / 1024, total / 1024))
  ln(" " .. bar(free, total))
  ln(("-"):rep(W))
  io.write((" Waiting for master... (Ctrl-C to quit)"):sub(1, W))
end

term.clear()
redraw()

-- ── Event Loop ────────────────────────────────────────────────
local running = true

-- Allow clean shutdown via keyboard interrupt
local function onInterrupt()
  running = false
end
event.listen("interrupted", onInterrupt)

while running do
  -- Short timeout so the display can refresh occasionally
  local ev, _, sender, port, _, cmd, a1, a2, a3, a4 =
    event.pull(2, "modem_message")

  if ev and port == PORT then

    -- ── Discovery ──────────────────────────────────────────────
    if cmd == "PING" then
      modem.send(sender, PORT, "PONG",
        computer.freeMemory(), computer.totalMemory(), count)

    -- ── Store a small value (fits in one packet) ───────────────
    elseif cmd == "STORE" then
      -- a1 = fibIndex (string→number), a2 = value string
      local idx = tonumber(a1)
      if idx then
        store[idx] = a2
        count = count + 1
        modem.send(sender, PORT, "ACK", idx)
      end

    -- ── Chunked store (large values) ──────────────────────────
    elseif cmd == "CHUNK" then
      -- a1=idx  a2=partNum  a3=totalParts  a4=data
      local idx  = tonumber(a1)
      local part = tonumber(a2)
      if idx and part then
        if not chunks[idx] then
          -- Record when this chunk set started arriving so the
          -- sweep can expire it if CHUNK_END never comes.
          chunks[idx] = { parts = {}, born = computer.uptime() }
        end
        chunks[idx].parts[part] = a4
      end

    elseif cmd == "CHUNK_END" then
      -- a1 = idx  (signals all chunks have been sent)
      local idx = tonumber(a1)
      if idx and chunks[idx] then
        local parts    = chunks[idx].parts
        local assembled = {}
        for i = 1, #parts do
          assembled[i] = parts[i] or ""
        end
        store[idx] = table.concat(assembled)
        chunks[idx] = nil   -- free chunk buffer
        count = count + 1
        modem.send(sender, PORT, "ACK", idx)
      end

    -- ── Stat request (for master's display refresh) ───────────
    elseif cmd == "STAT" then
      modem.send(sender, PORT, "STAT",
        computer.freeMemory(), computer.totalMemory(), count)

    -- ── Graceful shutdown ─────────────────────────────────────
    elseif cmd == "QUIT" then
      running = false
    end

    redraw()

  else
    -- No message this tick – run sweep and refresh display
    local now = computer.uptime()
    if now - lastSweep >= SWEEP_INTERVAL then
      lastSweep = now
      orphanDrops = orphanDrops + sweepChunks()
    end
    redraw()
  end
end

event.ignore("interrupted", onInterrupt)
modem.close(PORT)
print("\n[Storage] Shut down cleanly.")