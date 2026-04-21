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
local PORT  = 5757
modem.open(PORT)

-- ── Big Integer Addition ──────────────────────────────────────
local function bigadd(a, b)
  local result = {}
  local carry  = 0
  local ia, ib = #a, #b
  while ia > 0 or ib > 0 or carry > 0 do
    local da  = ia > 0 and a:byte(ia) - 48 or 0
    local db  = ib > 0 and b:byte(ib) - 48 or 0
    local sum = da + db + carry
    carry = math.floor(sum / 10)
    result[#result + 1] = string.char((sum % 10) + 48)
    ia = ia - 1
    ib = ib - 1
  end
  local lo, hi = 1, #result
  while lo < hi do
    result[lo], result[hi] = result[hi], result[lo]
    lo = lo + 1; hi = hi - 1
  end
  return table.concat(result)
end

-- ── Terminal geometry ─────────────────────────────────────────
local W, H = 80, 25
if component.isAvailable("gpu") then
  W, H = component.gpu.getResolution()
end

local function put(row, text)
  if row < 1 or row > H then return end
  term.setCursor(1, row)
  local s = tostring(text or "")
  if #s < W then s = s .. (" "):rep(W - #s)
  elseif #s > W then s = s:sub(1, W) end
  io.write(s)
end

local function truncate(s, max)
  if #s <= max then return s end
  local h = math.floor((max - 3) / 2)
  return s:sub(1, h) .. "..." .. s:sub(-h)
end

local function membar(free, total, bW)
  local pct    = (total > 0) and (1 - free / total) or 0
  local filled = math.max(0, math.min(bW, math.floor(pct * bW)))
  return string.format("[%s%s] %3d%%",
    ("#"):rep(filled), ("."):rep(bW - filled), math.floor(pct * 100))
end

-- ── Constants ─────────────────────────────────────────────────
local CHUNK_SIZE          = 7000   -- chars per modem packet (< 8 KiB OC limit)
local DISCOVERY_TIME      = 2.5    -- seconds to collect PONG replies
local STAT_INTERVAL       = 2.0    -- seconds between stats broadcast / stat polls
local DRAW_INTERVAL       = 0.25   -- seconds between display refresh
local YIELD_EVERY         = 50     -- iterations between OS yields / I/O flushes
local CHECKPOINT_INTERVAL = 30.0   -- seconds between checkpoint writes
local CHECKPOINT_FILE     = "/fib_checkpoint.dat"
local CHECKPOINT_TMP      = "/fib_checkpoint.tmp"

-- ── Checkpoint I/O ───────────────────────────────────────────
-- Format (plain text, 3 lines):
--   <n>
--   <fib(n-1)>   ← prev
--   <fib(n)>     ← curr
--
-- Written to a .tmp file first then renamed so a crash mid-write
-- never corrupts the last good checkpoint.

local function saveCheckpoint(n, prev, curr)
  local f, err = io.open(CHECKPOINT_TMP, "w")
  if not f then return false, "open failed: " .. tostring(err) end
  f:write(tostring(n) .. "\n")
  f:write(prev        .. "\n")
  f:write(curr        .. "\n")
  f:close()
  -- Atomic replace: remove old checkpoint then rename tmp into place.
  -- If the rename step crashes, the .tmp is still valid and loadCheckpoint
  -- falls back to it automatically.
  if fs.exists(CHECKPOINT_FILE) then fs.remove(CHECKPOINT_FILE) end
  local ok = fs.rename(CHECKPOINT_TMP, CHECKPOINT_FILE)
  if not ok then return false, "rename failed" end
  return true
end

local function loadCheckpoint()
  -- Try the canonical file first, fall back to the .tmp in case a
  -- previous run crashed between the remove and the rename.
  local path = fs.exists(CHECKPOINT_FILE) and CHECKPOINT_FILE
            or fs.exists(CHECKPOINT_TMP)  and CHECKPOINT_TMP
            or nil
  if not path then return nil end

  local f = io.open(path, "r")
  if not f then return nil end
  local nStr   = f:read("*l")
  local prevStr = f:read("*l")
  local currStr = f:read("*l")
  f:close()

  local n = tonumber(nStr)
  if not n or not prevStr or not currStr then return nil end
  if not prevStr:match("^%d+$") or not currStr:match("^%d+$") then
    return nil   -- corrupted
  end
  return n, prevStr, currStr
end

-- ── Node registry ─────────────────────────────────────────────
local nodes    = {}
local nodeList = {}
local robin    = 0

-- ── Discovery ─────────────────────────────────────────────────
-- (uses scrolling print – happens before term.clear)
term.clear()
print(("─"):rep(W))
print("  Fibonacci Compute Node  [" .. modem.address:sub(1,8) .. "...]")
print(("─"):rep(W))
print()
print("  Discovering storage nodes on port " .. PORT .. " ...")
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
    print(string.format("  Found storage node %.8s...  %.0f KiB free",
      sender, (tonumber(free) or 0) / 1024))
  end
end
print()
if #nodeList == 0 then
  print("  No storage nodes found - running compute-only.")
  print("  (Run fib_storage.lua on other rack servers to enable storage.)")
else
  print(string.format("  %d storage node(s) ready.", #nodeList))
end

-- ── Checkpoint resume prompt ──────────────────────────────────
local prev, curr, n
local resumedFrom = nil

local cpN, cpPrev, cpCurr = loadCheckpoint()
if cpN then
  print()
  print(string.format("  Checkpoint found: fib(%d)  (%d digits)",
    cpN, #cpCurr))
  io.write("  Resume from checkpoint? [Y/n]  ")
  local answer = (io.read() or ""):lower()
  if answer ~= "n" then
    n    = cpN
    prev = cpPrev
    curr = cpCurr
    resumedFrom = cpN
    print(string.format("  Resuming from fib(%d).", n))
  else
    print("  Starting fresh from fib(0).")
  end
else
  print("  No checkpoint found - starting from fib(0).")
end

if not n then
  prev = "0"
  curr = "1"
  n    = 1
end

print()
print("  Starting in 1 s ...")
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
      local part    = 1
      local pos     = 1
      local nchunks = math.ceil(#value / CHUNK_SIZE)
      while pos <= #value do
        modem.send(addr, PORT, "CHUNK",
          tostring(idx), tostring(part), tostring(nchunks),
          value:sub(pos, pos + CHUNK_SIZE - 1))
        pos  = pos + CHUNK_SIZE
        part = part + 1
      end
      modem.send(addr, PORT, "CHUNK_END", tostring(idx))
    end
  end
  sendQueue = {}
end

-- ── Display layout ────────────────────────────────────────────
-- Row  1   separator          (static)
-- Row  2   title              (static)
-- Row  3   separator          (static)
-- Row  4   fib(n) / digits / elapsed
-- Row  5   rate / queue / nodes
-- Row  6   value preview
-- Row  7   blank
-- Row  8   local RAM bar
-- Row  9   checkpoint status
-- Row 10   separator          (static)
-- Row 11   storage node header (static)
-- Row 12+  one row per storage node

local BAR_LOCAL = W - 40
local BAR_NODE  = math.max(4, W - 46)

-- checkpoint display state
local lastCheckpointTime   = 0
local lastCheckpointStatus = resumedFrom
  and string.format("resumed from fib(%d)", resumedFrom)
  or  "none yet"

-- Draw static skeleton
term.clear()
put(1,  ("-"):rep(W))
put(2,  "  Fibonacci Compute Node  [" .. modem.address:sub(1,8) .. "...]")
put(3,  ("-"):rep(W))
put(7,  "")
put(10, ("-"):rep(W))
local header = "  Storage node  " ..
               ("RAM fill"):sub(1, BAR_NODE) ..
               (" "):rep(math.max(0, BAR_NODE - #"RAM fill")) ..
               "  Stored"

put(11, header)

local function drawStatus(n, digits, elapsed, rate, value)
  local localFree  = computer.freeMemory()
  local localTotal = computer.totalMemory()

  put(4, string.format("  fib(%-10d)  digits: %-10d  elapsed: %.1f s",
    n, digits, elapsed))
  put(5, string.format("  rate: %.1f fibs/s   queue: %-6d  nodes: %d",
    rate, #sendQueue, #nodeList))
  put(6, "  value : " .. truncate(value, W - 10))
  put(8, "  local  " .. membar(localFree, localTotal, BAR_LOCAL))
  put(9, "  ckpt  : " .. lastCheckpointStatus)

  for i, addr in ipairs(nodeList) do
    local nd  = nodes[addr]
    local row = 11 + i
    if nd and row <= H then
      put(row, string.format("  %.8s...   %s  %d",
        nd.shortId, membar(nd.free, nd.total, BAR_NODE), nd.stored))
    end
  end
end

-- ── Seed the send queue with starting values ──────────────────
-- (only if we're starting fresh - a resumed run already has
--  these on the storage nodes from before the restart)
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

-- ── Main compute loop ─────────────────────────────────────────
local running = true
event.listen("interrupted", function() running = false end)

while running do
  -- Hot path: pure computation, no I/O
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
      rate     = (n - snapN) / dt
      snapN    = n
      snapTime = now
    end

    -- Flush values to storage nodes
    flushQueue()

    -- Checkpoint save
    if now - lastCheckpointTime >= CHECKPOINT_INTERVAL then
      local ok, err = saveCheckpoint(n, prev, curr)
      lastCheckpointTime = now
      if ok then
        lastCheckpointStatus = string.format(
          "fib(%d)  %d digits  @ %.0fs", n, #curr, now - startTime)
      else
        lastCheckpointStatus = "WRITE FAILED: " .. tostring(err)
      end
    end

    -- Stat poll + monitor broadcast
    if now - lastStat >= STAT_INTERVAL then
      lastStat = now
      pollStats()
      modem.broadcast(PORT, "STAT",
        n, #curr,
        math.floor(rate * 10) / 10,
        math.floor((now - startTime) * 10) / 10,
        computer.freeMemory(),
        computer.totalMemory())
    end

    drainStats()

    -- Display refresh
    if now - lastDraw >= DRAW_INTERVAL then
      lastDraw = now
      drawStatus(n, #curr, now - startTime, rate, curr)
    end

    os.sleep(0)
  end
end

modem.close(PORT)
term.setCursor(1, H)
print("\nStopped at fib(" .. n .. ").  Checkpoint is at: " .. CHECKPOINT_FILE)