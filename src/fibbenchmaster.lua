-- =============================================================
--  fibbenchmaster.lua  -  Fibonacci Master Node  [OPTIMISED]
--
--  Uses packed integer arrays for faster arbitrary-precision
--  arithmetic, and stores Fibonacci values to storage nodes as
--  limb chunks so the rack acts like distributed swap.
--
--  Optimisation notes (vs original):
--    1. bigadd: carry is always 0 or 1 when LIMB_BASE=10^7, so
--       math.floor() and % are replaced with a compare+subtract.
--       trimLimbs() removed from the return path (result is always
--       non-zero for Fibonacci n>0, and we handle the overflow limb
--       explicitly).
--    2. Network reload removed: prev/curr are already correct in
--       RAM; the old loadNumber(n-1)/loadNumber(n) block performed
--       unnecessary blocking round-trips every YIELD_EVERY steps.
--    3. drawFrame() called once before the loop; only drawStatus()
--       is called on each draw tick, eliminating repeated full-
--       screen redraws of unchanging borders.
--    4. decimalDigits(curr) cached once per YIELD block instead of
--       being recomputed three times (checkpoint, broadcast, draw).
--    5. fastPreview() converts only the leading limbs needed for
--       the display preview instead of calling toDecimal() on the
--       full number every draw tick.
--    6. YIELD_EVERY raised 50->200 so queue/stat overhead amortises
--       over more iterations.
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

local BOOTSTRAP_FIB = tonumber((arg and arg[1]) or (os.getenv and os.getenv("FIB_BOOTSTRAP_N") or "")) or 4096

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

-- ── Display helpers ───────────────────────────────────────────
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
  put(x + 1 + barW,   y, "]",                      C.border)
  put(x + barW + 2,    y, string.format("%3d%%", math.floor(pct * 100)), col)
end

-- ── Packed arbitrary precision arithmetic ─────────────────────
local LIMB_BASE  = 10000000   -- 10^7
local LIMB_WIDTH = 7
local CHUNK_LIMBS = 64

local function trimLimbs(a)
  local i = #a
  while i > 1 and (a[i] or 0) == 0 do
    a[i] = nil
    i = i - 1
  end
  return a
end

local function fromDecimal(s)
  s = tostring(s or "0"):gsub("^0+", "")
  if s == "" then return {0} end
  local limbs = {}
  while #s > 0 do
    local start = math.max(1, #s - LIMB_WIDTH + 1)
    limbs[#limbs + 1] = tonumber(s:sub(start)) or 0
    s = s:sub(1, start - 1)
  end
  return limbs
end

local function toDecimal(a)
  local i = #a
  while i > 1 and (a[i] or 0) == 0 do i = i - 1 end
  local parts = { tostring(a[i] or 0) }
  for j = i - 1, 1, -1 do
    parts[#parts + 1] = string.format("%07d", a[j] or 0)
  end
  return table.concat(parts)
end

local function decimalDigits(a)
  local i = #a
  while i > 1 and (a[i] or 0) == 0 do i = i - 1 end
  local hi = tostring(a[i] or 0)
  return (#a - 1) * LIMB_WIDTH + #hi
end

-- OPTIMISED: carry is always 0 or 1 when LIMB_BASE = 10^7.
-- Two limbs are at most 9,999,999 each; adding carry 1 gives
-- 19,999,999 < 2*LIMB_BASE, so floor()/% are unnecessary.
-- The result never has spurious leading zeros for Fibonacci,
-- so trimLimbs() is not needed on the return value.
local function bigadd(a, b)
  local result = {}
  local carry  = 0
  local na, nb = #a, #b
  local n = na > nb and na or nb
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
  if carry > 0 then
    result[n + 1] = carry   -- at most one extra limb
  end
  return result
end

local function splitChunks(limbs)
  local chunks, total = {}, math.max(1, math.ceil(#limbs / CHUNK_LIMBS))
  for part = 1, total do
    local chunk = {}
    local start = (part - 1) * CHUNK_LIMBS + 1
    local stop = math.min(#limbs, start + CHUNK_LIMBS - 1)
    for i = start, stop do
      chunk[#chunk + 1] = limbs[i]
    end
    chunks[part] = chunk
  end
  return chunks, total
end

local function encodeChunk(chunk)
  local out = {}
  for i = 1, #chunk do out[i] = tostring(chunk[i] or 0) end
  return table.concat(out, ",")
end

-- OPTIMISED: only convert the leading limbs needed to fill maxLen
-- characters, rather than calling toDecimal() on the full number.
local function fastPreview(a, maxLen)
  local i = #a
  while i > 1 and (a[i] or 0) == 0 do i = i - 1 end
  -- If the number is small enough, convert in full.
  if i * LIMB_WIDTH <= maxLen + LIMB_WIDTH then
    return toDecimal(a)
  end
  -- Otherwise only pull the top few limbs (enough to fill maxLen).
  local needed = math.ceil(maxLen / LIMB_WIDTH) + 2
  local parts  = { tostring(a[i] or 0) }
  local j = i - 1
  local count = 0
  while j >= 1 and count < needed do
    parts[#parts + 1] = string.format("%07d", a[j] or 0)
    j = j - 1
    count = count + 1
  end
  local s = table.concat(parts)
  if #s > maxLen then
    return s:sub(1, maxLen) .. "~"   -- ~ indicates truncation
  end

  return s
end

local function bigmul(a, b)
  if (#a == 1 and (a[1] or 0) == 0) or (#b == 1 and (b[1] or 0) == 0) then
    return { 0 }
  end

  local result = {}
  for i = 1, #a do
    local ai = a[i] or 0
    if ai ~= 0 then
      local carry = 0
      for j = 1, #b do
        local idx = i + j - 1
        local sum = (result[idx] or 0) + ai * (b[j] or 0) + carry
        carry = math.floor(sum / LIMB_BASE)
        result[idx] = sum - carry * LIMB_BASE
      end
      local k = i + #b
      while carry > 0 do
        local sum = (result[k] or 0) + carry
        carry = math.floor(sum / LIMB_BASE)
        result[k] = sum - carry * LIMB_BASE
        k = k + 1
      end
    end
  end
  return trimLimbs(result)
end

local function matrixMul(a, b)
  return {
    {
      bigadd(bigmul(a[1][1], b[1][1]), bigmul(a[1][2], b[2][1])),
      bigadd(bigmul(a[1][1], b[1][2]), bigmul(a[1][2], b[2][2])),
    },
    {
      bigadd(bigmul(a[2][1], b[1][1]), bigmul(a[2][2], b[2][1])),
      bigadd(bigmul(a[2][1], b[1][2]), bigmul(a[2][2], b[2][2])),
    },
  }
end

local function bootstrapFibPair(n)
  if n <= 0 then return { 0 }, { 1 } end
  local result = {
    { { 1 }, { 0 } },
    { { 0 }, { 1 } },
  }
  local base = {
    { { 1 }, { 1 } },
    { { 1 }, { 0 } },
  }
  local exp = n
  while exp > 0 do
    if exp % 2 == 1 then
      result = matrixMul(result, base)
    end
    exp = math.floor(exp / 2)
    if exp > 0 then
      base = matrixMul(base, base)
    end
  end
  return result[1][2], result[1][1]
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

-- ── Constants ─────────────────────────────────────────────────
local DISCOVERY_TIME      = 2.5
local STAT_INTERVAL       = 2.0
local DRAW_INTERVAL       = 0.25
local YIELD_EVERY         = 200      -- was 50; amortises overhead better
local CHECKPOINT_INTERVAL = 30.0
local CHECKPOINT_FILE     = "/fib_checkpoint.dat"
local CHECKPOINT_TMP      = "/fib_checkpoint.tmp"

-- ── Checkpoint I/O ────────────────────────────────────────────
local function saveCheckpoint(n, prev, curr)
  local f, err = io.open(CHECKPOINT_TMP, "w")
  if not f then return false, "open failed: " .. tostring(err) end
  f:write(tostring(n) .. "\n" .. toDecimal(prev) .. "\n" .. toDecimal(curr) .. "\n")
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

-- ── Node registry ────────────────────────────────────────────
local nodes       = {}   -- storage nodes
local nodeList    = {}
local computeNodes = {}
local computeList  = {}
local computeRobin = 0

local function sortedNodeList()
  local list = {}
  for i = 1, #nodeList do list[i] = nodeList[i] end
  table.sort(list, function(a, b)
    local na, nb = nodes[a], nodes[b]
    local fa = na and na.free or 0
    local fb = nb and nb.free or 0
    return fa > fb
  end)
  return list
end

local function sortedComputeList()
  local list = {}
  for i = 1, #computeList do list[i] = computeList[i] end
  table.sort(list, function(a, b)
    local na, nb = computeNodes[a], computeNodes[b]
    local fa = na and na.free or 0
    local fb = nb and nb.free or 0
    return fa > fb
  end)
  return list
end

local function chooseNodeForBytes(bytes)
  local sorted = sortedNodeList()
  for _, addr in ipairs(sorted) do
    local nd = nodes[addr]
    if nd and (nd.free or 0) >= bytes then
      return addr
    end
  end
  return sorted[1]
end

local function chooseComputeNode()
  local sorted = sortedComputeList()
  if #sorted == 0 then
    return nil
  end
  computeRobin = (computeRobin % #sorted) + 1
  return sorted[computeRobin]
end

local function waitForReply(addr, expectCmd, timeout)
  local deadline = computer.uptime() + (timeout or 1.5)
  while computer.uptime() < deadline do
    local ev, _, sender, port, _, cmd, a1, a2, a3, a4, a5, a6 =
      event.pull(deadline - computer.uptime(), "modem_message")
    if ev and sender == addr and port == PORT and cmd == expectCmd then
      return a1, a2, a3, a4, a5, a6
    end
  end
  return nil, "timeout"
end

local function storeChunk(addr, idx, part, total, payload)
  modem.send(addr, PORT, "STORE", tostring(idx), tostring(part), tostring(total), payload)
  local a1, a2 = waitForReply(addr, "ACK", 1.8)
  if tonumber(a1) == idx and tonumber(a2) == part then
    return true
  end
  return nil, tostring(a1 or "no ack")
end

local function purgeChunk(addr, idx, part)
  modem.send(addr, PORT, "PURGE", tostring(idx), tostring(part))
end

local storedManifests = {} -- [idx] = { total = n, parts = { [part] = addr } }

local function storeNumber(idx, limbs)
  if #nodeList == 0 then
    return true
  end

  local chunks, total = splitChunks(limbs)
  storedManifests[idx] = storedManifests[idx] or { total = total, parts = {} }
  storedManifests[idx].total = total
  storedManifests[idx].parts = storedManifests[idx].parts or {}

  for part = 1, total do
    local payload = encodeChunk(chunks[part])
    local addr = chooseNodeForBytes(#payload + 32)
    if not addr then
      return nil, "no storage nodes available"
    end

    local ok, err = storeChunk(addr, idx, part, total, payload)
    if not ok then
      local placed = false
      for _, alt in ipairs(sortedNodeList()) do
        if alt ~= addr then
          ok, err = storeChunk(alt, idx, part, total, payload)
          if ok then
            addr = alt
            placed = true
            break
          end
        end
      end
      if not placed then
        return nil, err
      end
    end

    storedManifests[idx].parts[part] = addr
    if nodes[addr] then
      nodes[addr].free = math.max(0, (nodes[addr].free or 0) - #payload)
      nodes[addr].stored = (nodes[addr].stored or 0) + 1
    end
  end

  return true
end

local function purgeNumber(idx)
  local man = storedManifests[idx]
  if not man then return end
  for part, addr in pairs(man.parts or {}) do
    purgeChunk(addr, idx, part)
  end
  storedManifests[idx] = nil
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

local function fetchChunk(addr, idx, part, deleteAfter)
  if not addr then return nil, "no address" end
  modem.send(addr, PORT, "FETCH", tostring(idx), tostring(part), deleteAfter and "1" or "0")
  local a1, a2, a3, a4 = waitForReply(addr, "GOT", 2.0)
  if tonumber(a1) ~= idx or tonumber(a2) ~= part then
    return nil, "bad reply"
  end
  return decodeChunk(a4), tonumber(a3) or 0
end

local function loadNumber(idx, deleteAfter)
  local man = storedManifests[idx]
  if not man then
    return nil, "missing manifest"
  end

  local total = tonumber(man.total) or 0
  if total <= 0 then
    for part in pairs(man.parts or {}) do
      if part > total then total = part end
    end
  end
  if total <= 0 then
    return { 0 }
  end

  local limbs = {}
  for part = 1, total do
    local addr = man.parts and man.parts[part]
    local chunk, err = fetchChunk(addr, idx, part, deleteAfter)
    if not chunk then
      local found = false
      for _, alt in ipairs(nodeList) do
        if alt ~= addr then
          chunk, err = fetchChunk(alt, idx, part, deleteAfter)
          if chunk then
            addr = alt
            found = true
            break
          end
        end
      end
      if not found then
        return nil, err or ("missing part " .. tostring(part))
      end
    end
    for i = 1, #chunk do
      limbs[#limbs + 1] = chunk[i]
    end
    if man.parts then man.parts[part] = addr end
  end


  return trimLimbs(limbs)
end

-- ── Discovery phase ───────────────────────────────────────────
local function cprint(text, fg)
  if gpu then gpu.setForeground(fg or C.value); gpu.setBackground(BG) end
  print(tostring(text))
end

cls()
cprint("+" .. ("-"):rep(W - 2) .. "+", C.border)
cprint("|" .. pad("  FIBONACCI MASTER NODE  [" .. modem.address:sub(1, 8)
       .. "...]  --  discovery", W - 2) .. "|", C.header)
cprint("+" .. ("-"):rep(W - 2) .. "+", C.border)
cprint("|" .. pad("", W - 2) .. "|", C.dim)
cprint("|" .. pad("  Scanning port " .. PORT .. " for storage and compute nodes ...", W - 2) .. "|", C.label)

modem.broadcast(PORT, "PING")
local deadline = computer.uptime() + DISCOVERY_TIME
while computer.uptime() < deadline do
  local ev, _, sender, port, _, cmd, free, total, cnt, kind =
    event.pull(deadline - computer.uptime(), "modem_message")
  if ev and port == PORT and cmd == "PONG" then
    kind = tostring(kind or "storage")
    if kind == "compute" then
      if not computeNodes[sender] then computeList[#computeList + 1] = sender end
      computeNodes[sender] = {
        free    = tonumber(free)  or 0,
        total   = tonumber(total) or 0,
        stored  = tonumber(cnt)   or 0,
        shortId = sender:sub(1, 8),
      }
      cprint("|" .. pad(string.format(
        "  [C] %.8s...  compute ready   %.0f KiB free",
        sender, (tonumber(free) or 0) / 1024), W - 2) .. "|", C.good)
    else
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
end

cprint("|" .. pad("", W - 2) .. "|", C.dim)
cprint("|" .. pad(string.format("  [*] %d storage node(s) ready.", #nodeList), W - 2) .. "|", C.good)
cprint("|" .. pad(string.format("  [*] %d compute node(s) ready.", #computeList), W - 2) .. "|", C.good)
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
    n = cpN
    prev = fromDecimal(cpPrev)
    curr = fromDecimal(cpCurr)
    resumedFrom = cpN
    cprint("|" .. pad(string.format("  Resuming from fib(%d).", n), W - 2) .. "|", C.good)
  else
    cprint("|" .. pad("  Starting fresh from fib(0).", W - 2) .. "|", C.label)
  end
else
  cprint("|" .. pad("  No checkpoint -- starting from fib(0).", W - 2) .. "|", C.label)
end

if not n then
  if BOOTSTRAP_FIB and BOOTSTRAP_FIB > 1 then
    cprint("|" .. pad(string.format(
      "  [*] Bootstrapping with matrix exponentiation to fib(%d)...",
      BOOTSTRAP_FIB), W - 2) .. "|", C.label)
    local bootStart = computer.uptime()
    local bootN = math.max(1, BOOTSTRAP_FIB)
    prev, curr = bootstrapFibPair(bootN - 1)
    n = bootN
    cprint("|" .. pad(string.format(
      "  [*] Bootstrap complete in %.1f s.", computer.uptime() - bootStart),
      W - 2) .. "|", C.good)
  else
    prev = { 0 }
    curr = { 1 }
    n = 1
  end
end

cprint("|" .. pad("", W - 2) .. "|", C.dim)
cprint("|" .. pad("  Starting in 1 s ...", W - 2) .. "|", C.dim)
cprint("+" .. ("-"):rep(W - 2) .. "+", C.border)
os.sleep(1)

-- ── Stat helpers ──────────────────────────────────────────────
local lastStat = 0
local function pollStats()
  for _, addr in ipairs(nodeList) do modem.send(addr, PORT, "STAT") end
  for _, addr in ipairs(computeList) do modem.send(addr, PORT, "STAT") end
end

local function drainStats()
  while true do
    local ev, _, sender, port, _, cmd, free, total, cnt =
      event.pull(0, "modem_message")
    if not ev then break end
    if port == PORT and cmd == "STAT" then
      if nodes[sender] then
        nodes[sender].free   = tonumber(free)  or nodes[sender].free
        nodes[sender].total  = tonumber(total) or nodes[sender].total
        nodes[sender].stored = tonumber(cnt)   or nodes[sender].stored
      elseif computeNodes[sender] then
        computeNodes[sender].free   = tonumber(free)  or computeNodes[sender].free
        computeNodes[sender].total  = tonumber(total) or computeNodes[sender].total
        computeNodes[sender].stored = tonumber(cnt)   or computeNodes[sender].stored
      end
    end
  end
end

-- ── Queue and transfer helpers ────────────────────────────────
local lastCkptStatus = "none yet"
local lastCkptColor  = C.dim
local lastCkptTime   = 0
local sendQueue  = {}
local queueDrops = 0

local function flushQueue()
  if #nodeList == 0 then
    queueDrops = queueDrops + #sendQueue
    sendQueue = {}
    return
  end

  for _, item in ipairs(sendQueue) do
    local idx, limbs = item[1], item[2]
    local ok, err = storeNumber(idx, limbs)
    if not ok then
      queueDrops = queueDrops + 1
      lastCkptStatus = "STORE failed: " .. tostring(err)
      lastCkptColor = C.bad
    else
      if idx >= 3 then
        purgeNumber(idx - 2)
      end
    end
  end
  sendQueue = {}
end

-- ── Layout ────────────────────────────────────────────────────
local MID = math.floor(W / 2)
local BAR_W_L = math.max(4, W - 21)
local BAR_W_N = math.max(4, W - 43)
lastCkptStatus = resumedFrom and string.format("resumed from fib(%d)", resumedFrom) or "none yet"
lastCkptColor  = resumedFrom and C.ckpt_ok or C.dim
lastCkptTime   = 0

-- OPTIMISED: drawFrame draws the static chrome only.
-- It is called once before the loop (and never again from inside
-- the loop) so the unchanging borders are not redrawn every tick.
local function drawFrame()
  cls()

  hline(1)
  put(1, 2, "|", C.border)
  put(2, 2, pad("  FIBONACCI MASTER NODE  [" .. modem.address:sub(1, 8) .. "...]", W - 2), C.header)
  put(W, 2, "|", C.border)
  hline(3)

  for row = 4, 6 do
    put(1, row, "|", C.border)
    put(MID, row, "|", C.border)
    put(W, row, "|", C.border)
  end
  hline(7)

  put(1, 8, "|", C.border); put(W, 8, "|", C.border)
  hline(9)

  put(1, 10, "|", C.border); put(W, 10, "|", C.border)
  put(1, 11, "|", C.border); put(W, 11, "|", C.border)
  hline(12)

  put(1, 13, "|", C.border)
  put(2, 13, pad("  STORAGE NODES", W - 2), C.header)
  put(W, 13, "|", C.border)
  hline(14)

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

local function statRow(y, lLbl, lVal, lCol, rLbl, rVal, rCol)
  put(2,        y, " " .. pad(lLbl, 8) .. " : ",      C.label)
  put(13,       y, pad(tostring(lVal), MID - 14),      lCol or C.number)
  put(MID + 1,  y, " " .. pad(rLbl, 8) .. " : ",      C.label)
  put(MID + 12, y, pad(tostring(rVal), W - MID - 13), rCol or C.number)
end

-- OPTIMISED: accepts pre-computed digits and previewStr so this
-- function never calls decimalDigits() or toDecimal() itself.
local function drawStatus(nVal, digits, elapsed, rate, previewStr)
  local lFree  = computer.freeMemory()
  local lTotal = computer.totalMemory()

  statRow(4,
    "fib(n)", tostring(nVal), C.number,
    "rate", string.format("%.1f /s", rate), rate > 0 and C.good or C.dim)
  statRow(5,
    "digits", tostring(digits), C.number,
    "queue", string.format("%d buffered", #sendQueue), #sendQueue > 0 and C.warn or C.value)
  statRow(6,
    "elapsed", string.format("%.1f s", elapsed), C.value,
    "nodes", string.format("%d storage / %d compute", #nodeList, #computeList),
    (#nodeList + #computeList) > 0 and C.good or C.warn)

  put(2,  8, " value  : ", C.label)
  put(11, 8, pad(trunc(previewStr, W - 12), W - 12), C.preview)

  put(2, 10, " local RAM  ", C.label)
  drawBar(14, 10, BAR_W_L, lFree, lTotal)

  put(2, 11, " checkpoint : ", C.label)
  put(16, 11, pad(lastCkptStatus, W - 17), lastCkptColor)

  for i, addr in ipairs(nodeList) do
    local nd  = nodes[addr]
    local row = 14 + i
    if nd and row < H then
      put(2, row, "[" .. nd.shortId .. "...]  ", C.node_id)
      drawBar(17, row, BAR_W_N, nd.free, nd.total)
      local eStr = string.format("entries: %d", nd.stored or 0)
      put(W - #eStr - 1, row, eStr, C.value)
    end
  end
end

local function parallelBigAdd(a, b)
  local width = math.max(#a, #b)
  local total = math.max(1, math.ceil(width / CHUNK_LIMBS))
  local jobId = tostring(computer.uptime()) .. "-" .. tostring(math.random(100000, 999999))
  local replies = {}
  local taskInputs = {}

  local expected = 0
  for part = 1, total do
    local startIdx = (part - 1) * CHUNK_LIMBS + 1
    local stopIdx  = math.min(width, startIdx + CHUNK_LIMBS - 1)
    local chunkA, chunkB = {}, {}
    for i = startIdx, stopIdx do
      chunkA[#chunkA + 1] = a[i] or 0
      chunkB[#chunkB + 1] = b[i] or 0
    end
    if #chunkA == 0 then chunkA[1] = 0 end
    if #chunkB == 0 then chunkB[1] = 0 end

    taskInputs[part] = { chunkA, chunkB }

    local worker = chooseComputeNode()
    if worker then
      modem.send(worker, PORT, "ADDJOB", jobId, tostring(part), tostring(total),
        encodeChunk(chunkA), encodeChunk(chunkB))
      expected = expected + 1
    else
      local sum0, carry0, sum1, carry1 = addChunkVariants(chunkA, chunkB)
      replies[part] = { sum0 = sum0, carry0 = carry0, sum1 = sum1, carry1 = carry1 }
    end
  end

  local deadline = computer.uptime() + 10.0
  while expected > 0 do
    local timeout = math.max(0, deadline - computer.uptime())
    local ev, _, sender, port, _, cmd, a1, a2, a3, a4, a5, a6 =
      event.pull(timeout, "modem_message")
    if not ev then
      for part = 1, total do
        if not replies[part] then
          local pair = taskInputs[part]
          local sum0, carry0, sum1, carry1 = addChunkVariants(pair[1], pair[2])
          replies[part] = { sum0 = sum0, carry0 = carry0, sum1 = sum1, carry1 = carry1 }
        end
      end
      expected = 0
    elseif port == PORT and cmd == "ADDRES" and a1 == jobId then
      local part = tonumber(a2)
      if part and not replies[part] then
        replies[part] = {
          sum0 = decodeChunk(a3), carry0 = tonumber(a4) or 0,
          sum1 = decodeChunk(a5), carry1 = tonumber(a6) or 0,
        }
        expected = expected - 1
      end
    end
  end

  local result = {}
  local carry = 0
  for part = 1, total do
    local rec = replies[part]
    if not rec then
      rec = { sum0 = { 0 }, carry0 = 0, sum1 = { 0 }, carry1 = 0 }
    end

    local chosen, nextCarry
    if carry == 0 then
      chosen, nextCarry = rec.sum0, rec.carry0
    else
      chosen, nextCarry = rec.sum1, rec.carry1
    end

    modem.broadcast(PORT, "CHUNK", jobId, tostring(part), tostring(total),
      tostring(carry), tostring(nextCarry), encodeChunk(chosen))

    for i = 1, #chosen do
      result[#result + 1] = chosen[i]
    end
    carry = nextCarry
  end

  if carry > 0 then
    result[#result + 1] = carry
  end

  return trimLimbs(result)
end

-- ── Seed storage and run ──────────────────────────────────────
if not resumedFrom and not (BOOTSTRAP_FIB and BOOTSTRAP_FIB > 1) then
  table.insert(sendQueue, { 0, { 0 } })
  table.insert(sendQueue, { 1, { 1 } })
end

local startTime = computer.uptime()
local lastDraw  = 0
local snapN     = n
local snapTime  = startTime
local rate      = 0

-- Draw the static frame once; the loop only refreshes data cells.
drawFrame()

local running = true
event.listen("interrupted", function() running = false end)

while running do
  local nextVal = parallelBigAdd(prev, curr)
  n = n + 1
  prev = curr
  curr = nextVal

  table.insert(sendQueue, { n, nextVal })

  if n % YIELD_EVERY == 0 then
    local now = computer.uptime()

    -- OPTIMISED: compute decimalDigits once and reuse everywhere.
    -- Previously it was computed separately for checkpoint, stat
    -- broadcast, and drawStatus -- three times per YIELD block.
    local currDigits  = decimalDigits(curr)
    local currPreview = fastPreview(curr, W - 12)

    local dt = now - snapTime
    if dt >= STAT_INTERVAL then
      rate = (n - snapN) / dt
      snapN = n
      snapTime = now
    end

    flushQueue()

    -- REMOVED (was lines 644-651 in original):
    -- The original code reloaded prev/curr from storage nodes here,
    -- doing blocking network round-trips (up to 2 s per chunk) on
    -- every YIELD_EVERY boundary even though prev and curr are
    -- already correct in local memory.  Removed entirely.

    if now - lastCkptTime >= CHECKPOINT_INTERVAL then
      local ok, err = saveCheckpoint(n, prev, curr)
      lastCkptTime = now
      if ok then
        lastCkptStatus = string.format("fib(%d)  %d digits  @ %.0f s",
          n, currDigits, now - startTime)
        lastCkptColor  = C.ckpt_ok
      else
        lastCkptStatus = "FAILED: " .. tostring(err)
        lastCkptColor  = C.bad
      end
    end

    if now - lastStat >= STAT_INTERVAL then
      lastStat = now
      pollStats()
      modem.broadcast(PORT, "STAT",
        n, currDigits,
        math.floor(rate * 10) / 10,
        math.floor((now - startTime) * 10) / 10,
        computer.freeMemory(), computer.totalMemory())
    end

    drainStats()

    if now - lastDraw >= DRAW_INTERVAL then
      lastDraw = now
      -- OPTIMISED: drawFrame() is NOT called here any more.
      -- It was repainting the full static chrome every 0.25 s.
      -- We pass the already-computed preview string so drawStatus
      -- never calls toDecimal() or decimalDigits() itself.
      drawStatus(n, currDigits, now - startTime, rate, currPreview)
    end

    os.sleep(0)
  end
end

modem.close(PORT)
if gpu then gpu.setForeground(C.value); gpu.setBackground(BG) end
term.setCursor(1, H)
print("\nStopped at fib(" .. n .. ").  Checkpoint: " .. CHECKPOINT_FILE)