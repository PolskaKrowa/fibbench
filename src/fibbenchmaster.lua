-- =============================================================
--  fibbenchmaster.lua  -  Fibonacci Compute Node  (with checkpointing)
--
--  Computes fib(n) as fast as possible.  Keeps only prev/curr
--  locally (O(1) RAM).  Every computed value is queued and
--  flushed to storage nodes during the mandatory OS yield so
--  network I/O never stalls the compute loop.
--
--  Checkpoints are written atomically to disk at a configurable
--  interval so computation can resume after a restart.
--
--  Run fib_storage.lua on every other server in the rack first,
--  then run this.  Optionally run fib_monitor.lua on a spare
--  screen for a rack-wide view.
-- =============================================================

local component = require("component")
local computer  = require("computer")
local event     = require("event")
local term      = require("term")
local fs        = require("filesystem")

if not component.isAvailable("modem") then
  error("No modem found - install a network card!")
end

local modem = component.modem
local gpu   = component.isAvailable("gpu") and component.gpu or nil
local PORT  = 5757
modem.open(PORT)

-- ── Color palette ─────────────────────────────────────────────
local BG = 0x000000
local C = {
  border   = 0x1A4A7A,   -- steel blue  - box lines
  header   = 0x33AAFF,   -- bright blue - titles / section labels
  label    = 0x778899,   -- slate gray  - field labels
  value    = 0xEEEEEE,   -- near-white  - plain values
  number   = 0xFFDD44,   -- yellow      - numeric values
  good     = 0x44CC44,   -- green       - positive status
  warn     = 0xFF9900,   -- orange      - warning / degraded
  bad      = 0xFF4444,   -- red         - error / overload
  dim      = 0x333344,   -- very dark   - empty bar segments, quiet text
  preview  = 0x88CCFF,   -- light blue  - value preview text
  ckpt_ok  = 0x44DD88,   -- mint green  - checkpoint success
  node_id  = 0xAABBCC,   -- blue-gray   - storage node addresses
}

local W, H = 80, 25
if gpu then W, H = gpu.getResolution() end

-- ── Display primitives ────────────────────────────────────────

local function cls()
  if gpu then
    gpu.setBackground(BG)
    gpu.setForeground(C.value)
    gpu.fill(1, 1, W, H, " ")
  else
    term.clear()
  end
end

-- Write text at (x, y) with given fg/bg; clamps to screen edges.
local function put(x, y, text, fg, bg)
  if not gpu or y < 1 or y > H or x > W then return end
  gpu.setBackground(bg or BG)
  gpu.setForeground(fg or C.value)
  local s = tostring(text or "")
  if x < 1 then s = s:sub(2 - x); x = 1 end
  if x + #s - 1 > W then s = s:sub(1, W - x + 1) end
  if #s > 0 then gpu.set(x, y, s) end
end

-- Full-width horizontal rule with + corners.
local function hline(y, fg)
  put(1, y, "+" .. ("-"):rep(W - 2) .. "+", fg or C.border)
end

-- Pad / truncate to exactly w chars.
local function pad(s, w)
  s = tostring(s or "")
  if #s > w then return s:sub(1, w) end
  return s .. (" "):rep(w - #s)
end

-- Middle-truncate a string.
local function trunc(s, max)
  s = tostring(s or "")
  if #s <= max then return s end
  local h = math.floor((max - 3) / 2)
  return s:sub(1, h) .. "..." .. s:sub(-(max - h - 3))
end

-- Draw a colored RAM/usage bar starting at (x, y).
-- Total columns consumed = barW + 7  ("[" fill dots "]" " NNN%")
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

-- ── Big Integer Addition ──────────────────────────────────────
local function bigadd(a, b)
  local result, carry = {}, 0
  local ia, ib = #a, #b
  while ia > 0 or ib > 0 or carry > 0 do
    local da  = ia > 0 and a:byte(ia) - 48 or 0
    local db  = ib > 0 and b:byte(ib) - 48 or 0
    local sum = da + db + carry
    carry = math.floor(sum / 10)
    result[#result + 1] = string.char((sum % 10) + 48)
    ia = ia - 1; ib = ib - 1
  end
  local lo, hi = 1, #result
  while lo < hi do
    result[lo], result[hi] = result[hi], result[lo]
    lo = lo + 1; hi = hi - 1
  end
  return table.concat(result)
end

-- ── Constants ─────────────────────────────────────────────────
local CHUNK_SIZE          = 7000
local DISCOVERY_TIME      = 2.5
local STAT_INTERVAL       = 2.0
local DRAW_INTERVAL       = 0.25
local YIELD_EVERY         = 50
local CHECKPOINT_INTERVAL = 30.0
local CHECKPOINT_FILE     = "/fib_checkpoint.dat"
local CHECKPOINT_TMP      = "/fib_checkpoint.tmp"

-- ── Checkpoint I/O ────────────────────────────────────────────
-- Format: 3 lines  <n> / <prev> / <curr>
-- Written to .tmp then renamed so a mid-write crash never corrupts
-- the last good checkpoint.

local function saveCheckpoint(n, prev, curr)
  local f, err = io.open(CHECKPOINT_TMP, "w")
  if not f then return false, "open failed: " .. tostring(err) end
  f:write(tostring(n) .. "\n" .. prev .. "\n" .. curr .. "\n")
  f:close()
  if fs.exists(CHECKPOINT_FILE) then fs.remove(CHECKPOINT_FILE) end
  local ok = fs.rename(CHECKPOINT_TMP, CHECKPOINT_FILE)
  if not ok then return false, "rename failed" end
  return true
end

local function loadCheckpoint()
  local path = fs.exists(CHECKPOINT_FILE) and CHECKPOINT_FILE
            or fs.exists(CHECKPOINT_TMP)  and CHECKPOINT_TMP
            or nil
  if not path then return nil end
  local f = io.open(path, "r")
  if not f then return nil end
  local ns, ps, cs = f:read("*l"), f:read("*l"), f:read("*l")
  f:close()
  local n = tonumber(ns)
  if not n or not ps or not cs then return nil end
  if not ps:match("^%d+$") or not cs:match("^%d+$") then return nil end
  return n, ps, cs
end

-- ── Node registry ─────────────────────────────────────────────
local nodes    = {}
local nodeList = {}
local robin    = 0

-- ── Discovery phase ───────────────────────────────────────────
-- Styled line printer: sets GPU foreground then calls print().
local function cprint(text, fg)
  if gpu then gpu.setForeground(fg or C.value); gpu.setBackground(BG) end
  print(tostring(text))
end

cls()
cprint("+" .. ("-"):rep(W - 2) .. "+", C.border)
cprint("|" .. pad("  FIBONACCI COMPUTE NODE  [" .. modem.address:sub(1, 8)
       .. "...]  --  discovery", W - 2) .. "|", C.header)
cprint("+" .. ("-"):rep(W - 2) .. "+", C.border)
cprint("|" .. pad("", W - 2) .. "|", C.dim)
cprint("|" .. pad("  Scanning port " .. PORT .. " for storage nodes ...", W - 2) .. "|", C.label)

modem.broadcast(PORT, "PING")
local deadline = computer.uptime() + DISCOVERY_TIME
while computer.uptime() < deadline do
  local ev, _, sender, port, _, cmd, free, total, cnt =
    event.pull(deadline - computer.uptime(), "modem_message")
  if ev and port == PORT and cmd == "PONG" then
    if not nodes[sender] then nodeList[#nodeList + 1] = sender end
    nodes[sender] = {
      free    = tonumber(free)  or 0,
      total   = tonumber(total) or 0,
      stored  = tonumber(cnt)   or 0,
      shortId = sender:sub(1, 8),
    }
    cprint("|" .. pad(string.format(
      "  [+] %.8s...  found   %.0f KiB free",
      sender, (tonumber(free) or 0) / 1024), W - 2) .. "|", C.good)
  end
end

cprint("|" .. pad("", W - 2) .. "|", C.dim)
if #nodeList == 0 then
  cprint("|" .. pad("  [!] No storage nodes found -- running compute-only.", W - 2) .. "|", C.warn)
else
  cprint("|" .. pad(string.format("  [*] %d storage node(s) ready.", #nodeList), W - 2) .. "|", C.good)
end
cprint("|" .. pad("", W - 2) .. "|", C.dim)

-- ── Checkpoint resume prompt ──────────────────────────────────
local prev, curr, n
local resumedFrom = nil
local cpN, cpPrev, cpCurr = loadCheckpoint()

if cpN then
  cprint("|" .. pad(string.format(
    "  [C] Checkpoint found: fib(%d)  (%d digits)", cpN, #cpCurr), W - 2) .. "|", C.ckpt_ok)
  cprint("|" .. pad("", W - 2) .. "|", C.dim)
  if gpu then gpu.setForeground(C.value); gpu.setBackground(BG) end
  io.write("|  Resume from checkpoint? [Y/n]  ")
  local answer = (io.read() or ""):lower()
  if answer ~= "n" then
    n = cpN; prev = cpPrev; curr = cpCurr; resumedFrom = cpN
    cprint("|" .. pad(string.format("  Resuming from fib(%d).", n), W - 2) .. "|", C.good)
  else
    cprint("|" .. pad("  Starting fresh from fib(0).", W - 2) .. "|", C.label)
  end
else
  cprint("|" .. pad("  No checkpoint -- starting from fib(0).", W - 2) .. "|", C.label)
end

if not n then prev = "0"; curr = "1"; n = 1 end

cprint("|" .. pad("", W - 2) .. "|", C.dim)
cprint("|" .. pad("  Starting in 1 s ...", W - 2) .. "|", C.dim)
cprint("+" .. ("-"):rep(W - 2) .. "+", C.border)
os.sleep(1)

-- ── Stat helpers ──────────────────────────────────────────────
local lastStat = 0

local function pollStats()
  for _, addr in ipairs(nodeList) do modem.send(addr, PORT, "STAT") end
end

local function drainStats()
  while true do
    local ev, _, sender, port, _, cmd, free, total, cnt =
      event.pull(0, "modem_message")
    if not ev then break end
    if port == PORT and cmd == "STAT" and nodes[sender] then
      nodes[sender].free   = tonumber(free)  or nodes[sender].free
      nodes[sender].total  = tonumber(total) or nodes[sender].total
      nodes[sender].stored = tonumber(cnt)   or nodes[sender].stored
    end
  end
end

-- ── Send queue ────────────────────────────────────────────────
local sendQueue  = {}
local queueDrops = 0

local function flushQueue()
  if #nodeList == 0 then
    queueDrops = queueDrops + #sendQueue
    sendQueue  = {}
    return
  end
  for _, item in ipairs(sendQueue) do
    local idx, value = item[1], item[2]
    robin = (robin % #nodeList) + 1
    local addr = nodeList[robin]
    if #value <= CHUNK_SIZE then
      modem.send(addr, PORT, "STORE", tostring(idx), value)
    else
      local part, pos = 1, 1
      while pos <= #value do
        modem.send(addr, PORT, "CHUNK",
          tostring(idx), tostring(part),
          tostring(math.ceil(#value / CHUNK_SIZE)),
          value:sub(pos, pos + CHUNK_SIZE - 1))
        pos = pos + CHUNK_SIZE; part = part + 1
      end
      modem.send(addr, PORT, "CHUNK_END", tostring(idx))
    end
  end
  sendQueue = {}
end

-- ── Layout ────────────────────────────────────────────────────
--
--  Row  1   top border
--  Row  2   title bar
--  Row  3   separator
--  Row  4   fib(n)   |  rate
--  Row  5   digits   |  queue
--  Row  6   elapsed  |  nodes
--  Row  7   separator
--  Row  8   value preview
--  Row  9   separator
--  Row 10   local RAM bar
--  Row 11   checkpoint status
--  Row 12   separator
--  Row 13   STORAGE NODES header
--  Row 14   separator
--  Row 15+  one row per node  (or "none" notice)
--  last     bottom border
--
-- Two-column split: left content x=2..(MID-1), divider at MID,
--                   right content x=(MID+1)..(W-1), right border at W.

local MID = math.floor(W / 2)

-- Local RAM bar width: "| local RAM  [bar] NNN% |"
--   label "| local RAM  " ends at x=13, bar at x=14,
--   bar total cols = barW+7, last col = 14+barW+6 <= W-1
--   => barW = W - 21
local BAR_W_L = math.max(4, W - 21)

-- Node bar width: "| [xxxxxxxx...]  [bar] NNN%   entries: NNNNNNN |"
--   id prefix 14 chars ends at x=15, bar at x=17 (1 gap),
--   reserve 20 cols on right for "  entries: NNNNNNN " before "|"
--   17 + barW + 6 + 20 = W  => barW = W - 43
local BAR_W_N = math.max(4, W - 43)

-- Checkpoint display state
local lastCkptStatus = resumedFrom
  and string.format("resumed from fib(%d)", resumedFrom)
  or  "none yet"
local lastCkptColor  = resumedFrom and C.ckpt_ok or C.dim
local lastCkptTime   = 0

-- ── Static frame (drawn once on startup) ─────────────────────
local function drawFrame()
  cls()

  -- Top title box
  hline(1)
  put(1, 2, "|", C.border)
  put(2, 2, pad("  FIBONACCI COMPUTE NODE  ["
    .. modem.address:sub(1, 8) .. "...]", W - 2), C.header)
  put(W, 2, "|", C.border)
  hline(3)

  -- Stat rows 4-6: borders + vertical divider at MID
  for row = 4, 6 do
    put(1,   row, "|", C.border)
    put(MID, row, "|", C.border)
    put(W,   row, "|", C.border)
  end
  hline(7)

  -- Value preview row
  put(1, 8, "|", C.border); put(W, 8, "|", C.border)
  hline(9)

  -- Local RAM + checkpoint rows
  put(1, 10, "|", C.border); put(W, 10, "|", C.border)
  put(1, 11, "|", C.border); put(W, 11, "|", C.border)
  hline(12)

  -- Storage section header
  put(1,   13, "|", C.border)
  put(2,   13, pad("  STORAGE NODES", W - 2), C.header)
  put(W,   13, "|", C.border)
  hline(14)

  -- Node rows (or empty notice if no nodes found)
  if #nodeList == 0 then
    put(1, 15, "|", C.border)
    put(2, 15, pad("  (no nodes -- running in compute-only mode)", W - 2), C.dim)
    put(W, 15, "|", C.border)
    hline(16)
  else
    for i = 1, #nodeList do
      local row = 14 + i
      if row < H then
        put(1, row, "|", C.border)
        put(W, row, "|", C.border)
      end
    end
    hline(math.min(H, 15 + #nodeList))
  end
end

-- ── Two-column stat row ───────────────────────────────────────
-- Left:  label at x=2 (11 chars: " lbl      : "), value at x=13
-- Right: label at x=MID+1 (11 chars), value at x=MID+12
local function statRow(y, lLbl, lVal, lCol, rLbl, rVal, rCol)
  put(2,        y, " " .. pad(lLbl, 8) .. " : ",       C.label)
  put(13,       y, pad(tostring(lVal), MID - 14),       lCol or C.number)
  put(MID + 1,  y, " " .. pad(rLbl, 8) .. " : ",       C.label)
  put(MID + 12, y, pad(tostring(rVal), W - MID - 13),  rCol or C.number)
end

-- ── drawStatus  (called every DRAW_INTERVAL) ──────────────────
local function drawStatus(n, digits, elapsed, rate, value)
  local lFree  = computer.freeMemory()
  local lTotal = computer.totalMemory()

  -- Two-column stat rows
  statRow(4,
    "fib(n)",  tostring(n),                           C.number,
    "rate",    string.format("%.1f /s", rate),         rate > 0 and C.good or C.dim)
  statRow(5,
    "digits",  tostring(digits),                      C.number,
    "queue",   string.format("%d buffered", #sendQueue),
               #sendQueue > 200 and C.warn or C.value)
  statRow(6,
    "elapsed", string.format("%.1f s", elapsed),      C.value,
    "nodes",   string.format("%d storage", #nodeList),
               #nodeList > 0 and C.good or C.warn)

  -- Row 8: value preview
  -- " value  : <text>" starting at x=2; text fills to x=W-1
  put(2,  8, " value  : ",                             C.label)
  put(11, 8, pad(trunc(value, W - 12), W - 12),        C.preview)

  -- Row 10: local RAM bar at x=14
  put(2,  10, " local RAM  ",                           C.label)
  drawBar(14, 10, BAR_W_L, lFree, lTotal)

  -- Row 11: checkpoint status at x=16
  put(2,  11, " checkpoint : ",                         C.label)
  put(16, 11, pad(lastCkptStatus, W - 17),              lastCkptColor)

  -- Storage node rows
  for i, addr in ipairs(nodeList) do
    local nd  = nodes[addr]
    local row = 14 + i
    if nd and row < H then
      -- Node ID tag "[xxxxxxxx...]  " at x=2 (14 chars)
      put(2,  row, "[" .. nd.shortId .. "...]  ",       C.node_id)
      -- RAM bar at x=17
      drawBar(17, row, BAR_W_N, nd.free, nd.total)
      -- Entry count, right-aligned before closing "|"
      local eStr = string.format("entries: %d", nd.stored)
      put(W - #eStr - 1, row, eStr,                     C.value)
    end
  end
end

-- ── Seed the queue with initial values ───────────────────────
if not resumedFrom then
  sendQueue[#sendQueue + 1] = {0, "0"}
  sendQueue[#sendQueue + 1] = {1, "1"}
end

-- ── Tracking vars ─────────────────────────────────────────────
local startTime = computer.uptime()
local lastDraw  = 0
local snapN     = n
local snapTime  = startTime
local rate      = 0

-- Draw initial frame
drawFrame()

-- ── Main compute loop ─────────────────────────────────────────
local running = true
event.listen("interrupted", function() running = false end)

while running do
  -- Hot path: pure bignum arithmetic, no I/O
  local next = bigadd(prev, curr)
  n    = n + 1
  prev = curr
  curr = next
  sendQueue[#sendQueue + 1] = {n, next}

  -- Yield + all I/O every YIELD_EVERY iterations
  if n % YIELD_EVERY == 0 then
    local now = computer.uptime()

    -- Rate calculation
    local dt = now - snapTime
    if dt >= STAT_INTERVAL then
      rate = (n - snapN) / dt
      snapN = n; snapTime = now
    end

    flushQueue()

    -- Checkpoint save
    if now - lastCkptTime >= CHECKPOINT_INTERVAL then
      local ok, err = saveCheckpoint(n, prev, curr)
      lastCkptTime = now
      if ok then
        lastCkptStatus = string.format(
          "fib(%d)  %d digits  @ %.0f s", n, #curr, now - startTime)
        lastCkptColor  = C.ckpt_ok
      else
        lastCkptStatus = "FAILED: " .. tostring(err)
        lastCkptColor  = C.bad
      end
    end

    -- Stats poll + monitor broadcast
    if now - lastStat >= STAT_INTERVAL then
      lastStat = now
      pollStats()
      modem.broadcast(PORT, "STAT",
        n, #curr,
        math.floor(rate * 10) / 10,
        math.floor((now - startTime) * 10) / 10,
        computer.freeMemory(), computer.totalMemory())
    end

    drainStats()

    -- Display refresh
    if now - lastDraw >= DRAW_INTERVAL then
      lastDraw = now
      drawFrame()
      drawStatus(n, #curr, now - startTime, rate, curr)
    end

    os.sleep(0)
  end
end

modem.close(PORT)
if gpu then gpu.setForeground(C.value); gpu.setBackground(BG) end
term.setCursor(1, H)
print("\nStopped at fib(" .. n .. ").  Checkpoint: " .. CHECKPOINT_FILE)