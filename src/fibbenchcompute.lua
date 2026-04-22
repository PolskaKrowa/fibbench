-- =============================================================
--  fibbenchcompute.lua  -  Fibonacci Compute Worker
--
--  Standalone worker for the Fibonacci master.
--  Handles PING, STAT, ADDJOB, and CHUNK broadcasts.
--
--  UI notes:
--    - Shows only this node's own information.
--    - Uses a compact dashboard similar to the master styling.
--    - No storage node lists, no master-wide status.
-- =============================================================

local component = require("component")
local computer  = require("computer")
local event     = require("event")
local term      = require("term")

if not component.isAvailable("modem") then
  error("No modem found - install a network card!")
end

local modem = component.modem
local gpu   = component.isAvailable("gpu") and component.gpu or nil
local PORT  = 5757
modem.open(PORT)

local LIMB_BASE = 10000000
local LIMB_WIDTH = 7
local CHUNK_LIMBS = 64

-- ── Palette ───────────────────────────────────────────────────
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
  preview  = 0x88CCFF,
  ckpt_ok  = 0x44DD88,
  node_id  = 0xAABBCC,
}

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

local function trunc(s, max)
  s = tostring(s or "")
  if #s <= max then return s end
  local h = math.floor((max - 3) / 2)
  return s:sub(1, h) .. "..." .. s:sub(-(max - h - 3))
end

local function drawBar(x, y, barW, free, total)
  local pct = (total > 0) and (1 - free / total) or 0
  local filled = math.max(0, math.min(barW, math.floor(pct * barW)))
  local col = (pct < 0.5) and C.good or (pct < 0.8) and C.warn or C.bad
  put(x,              y, "[",                      C.border)
  put(x + 1,          y, ("#"):rep(filled),        col)
  put(x + 1 + filled, y, ("."):rep(barW - filled), C.dim)
  put(x + barW + 1,   y, "]",                      C.border)
  put(x + barW + 3,   y, string.format("%3d%%", math.floor(pct * 100)), col)
end

local function cprint(text, fg)
  if gpu then gpu.setForeground(fg or C.value); gpu.setBackground(BG) end
  print(tostring(text))
end

local function decodeChunk(payload)
  payload = tostring(payload or "")
  if payload == "" then return { 0 } end
  local chunk = {}
  for token in payload:gmatch("[^,]+") do
    chunk[#chunk + 1] = tonumber(token) or 0
  end
  if #chunk == 0 then chunk[1] = 0 end
  return chunk
end

local function encodeChunk(chunk)
  local out = {}
  for i = 1, #chunk do out[i] = tostring(chunk[i] or 0) end
  return table.concat(out, ",")
end

local function addChunkWithCarry(a, b, carryIn)
  local result = {}
  local carry = carryIn or 0
  local n = math.max(#a, #b)
  for i = 1, n do
    local sum = (a[i] or 0) + (b[i] or 0) + carry
    if sum >= LIMB_BASE then
      result[i] = sum - LIMB_BASE
      carry = 1
    else
      result[i] = sum
      carry = 0
    end
  end
  return result, carry
end

local function addChunkVariants(a, b)
  local sum0, carry0 = addChunkWithCarry(a, b, 0)
  local sum1, carry1 = addChunkWithCarry(a, b, 1)
  return sum0, carry0, sum1, carry1
end

-- ── Local node state ──────────────────────────────────────────
local bootTime = computer.uptime()
local startFree = computer.freeMemory()
local startTotal = computer.totalMemory()

local lastFree = startFree
local lastTotal = startTotal
local jobsSeen = 0
local jobsDone = 0
local chunksSeen = 0
local lastCmd = "idle"
local lastJobId = "-"
local lastPart = 0
local lastTotalParts = 0
local lastCarryOut = 0
local lastResultLimbs = 0
local lastPeer = "-"
local lastActivity = bootTime

local function refreshStats()
  lastFree = computer.freeMemory()
  lastTotal = computer.totalMemory()
end

-- ── UI ────────────────────────────────────────────────────────
local BAR_W = math.max(6, W - 20)

local function drawFrame()
  cls()
  hline(1)
  put(1, 2, "|", C.border)
  put(2, 2, pad("  FIBONACCI COMPUTE NODE  [" .. modem.address:sub(1, 8) .. "...]", W - 2), C.header)
  put(W, 2, "|", C.border)
  hline(3)

  for row = 4, 6 do
    put(1, row, "|", C.border)
    put(W, row, "|", C.border)
  end
  hline(7)

  put(1, 8, "|", C.border); put(W, 8, "|", C.border)
  hline(9)

  put(1, 10, "|", C.border); put(W, 10, "|", C.border)
  put(1, 11, "|", C.border); put(W, 11, "|", C.border)
  hline(12)

  put(2, 13, pad("  THIS NODE", W - 2), C.header)
  hline(14)
end

local function statRow(y, lLbl, lVal, lCol, rLbl, rVal, rCol)
  local mid = math.floor(W / 2)
  put(2,        y, " " .. pad(lLbl, 8) .. " : ",      C.label)
  put(13,       y, pad(tostring(lVal), mid - 14),        lCol or C.number)
  put(mid + 1,  y, " " .. pad(rLbl, 8) .. " : ",      C.label)
  put(mid + 12, y, pad(tostring(rVal), W - mid - 13),   rCol or C.number)
end

local function drawStatus()
  local uptime = computer.uptime() - bootTime
  local memUsed = lastTotal - lastFree

  statRow(4,
    "role", "compute", C.good,
    "port", PORT, C.number)
  statRow(5,
    "uptime", string.format("%.1f s", uptime), C.value,
    "jobs", string.format("%d/%d", jobsDone, jobsSeen), C.good)
  statRow(6,
    "chunks", tostring(chunksSeen), C.number,
    "last cmd", lastCmd, C.preview)

  put(2,  8, " node id : ", C.label)
  put(12, 8, pad(modem.address, W - 13), C.node_id)

  put(2, 10, " memory  ", C.label)
  drawBar(13, 10, BAR_W, lastFree, lastTotal)
  put(W - 18, 10, pad(string.format("%d KiB", math.floor(memUsed / 1024)), 17), C.value)

  put(2, 11, " peer    : ", C.label)
  put(13, 11, pad(trunc(lastPeer, W - 14), W - 14), C.preview)

  put(2, 15, " last job : ", C.label)
  put(13, 15, pad(string.format("%s  part %d/%d", lastJobId, lastPart, lastTotalParts), W - 14), C.value)
  put(2, 16, " result   : ", C.label)
  put(13, 16, pad(string.format("%d limb(s), carry %d", lastResultLimbs, lastCarryOut), W - 14), C.value)

  put(2, 18, " activity : ", C.label)
  put(13, 18, pad(string.format("%.1f s ago", computer.uptime() - lastActivity), W - 14), C.preview)
end

-- ── Startup UI ────────────────────────────────────────────────
drawFrame()
refreshStats()
drawStatus()

cprint("+" .. ("-"):rep(W - 2) .. "+", C.border)
cprint("|" .. pad("  FIBONACCI COMPUTE NODE  [" .. modem.address:sub(1, 8) .. "...]  --  online", W - 2) .. "|", C.header)
cprint("+" .. ("-"):rep(W - 2) .. "+", C.border)
cprint("|" .. pad("  Waiting for master jobs on port " .. PORT .. " ...", W - 2) .. "|", C.label)
cprint("|" .. pad("  This panel shows only this node's own state.", W - 2) .. "|", C.dim)
cprint("+" .. ("-"):rep(W - 2) .. "+", C.border)

-- ── Main loop ────────────────────────────────────────────────
local running = true
event.listen("interrupted", function() running = false end)

local lastDraw = 0
local DRAW_INTERVAL = 0.25

while running do
  local ev, _, sender, port, _, cmd, a1, a2, a3, a4, a5, a6 =
    event.pull(DRAW_INTERVAL, "modem_message")

  if ev and port == PORT then
    lastPeer = tostring(sender or "-")
    lastCmd = tostring(cmd or "unknown")
    lastActivity = computer.uptime()

    if cmd == "PING" then
      refreshStats()
      modem.send(sender, PORT, "PONG", lastFree, lastTotal, 0, "compute")
      cprint(string.format("[PING] from %.8s...", sender), C.dim)

    elseif cmd == "STAT" then
      refreshStats()
      modem.send(sender, PORT, "STAT", lastFree, lastTotal, 0, "compute")
      cprint(string.format("[STAT] from %.8s...", sender), C.dim)

    elseif cmd == "ADDJOB" then
      jobsSeen = jobsSeen + 1
      local jobId  = tostring(a1 or "")
      local part   = tonumber(a2) or 0
      local total  = tonumber(a3) or 0
      local chunkA = decodeChunk(a4)
      local chunkB = decodeChunk(a5)
      local sum0, carry0, sum1, carry1 = addChunkVariants(chunkA, chunkB)
      jobsDone = jobsDone + 1
      lastJobId = jobId
      lastPart = part
      lastTotalParts = total
      lastCarryOut = carry1
      lastResultLimbs = math.max(#sum0, #sum1)
      refreshStats()
      modem.send(sender, PORT, "ADDRES",
        jobId, tostring(part), encodeChunk(sum0), tostring(carry0),
        encodeChunk(sum1), tostring(carry1))

    elseif cmd == "CHUNK" then
      chunksSeen = chunksSeen + 1
      -- Broadcast only. No persistence required on workers.
    end
  end

  if computer.uptime() - lastDraw >= DRAW_INTERVAL then
    lastDraw = computer.uptime()
    refreshStats()
    drawStatus()
  end
end

modem.close(PORT)
if gpu then gpu.setForeground(C.value); gpu.setBackground(BG) end
term.setCursor(1, H)
print("\nStopped compute node at " .. modem.address .. ".")
