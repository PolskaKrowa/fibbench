-- fibbenchcommon.lua
--
-- Shared library for the FibBench distributed Fibonacci network.
-- Loaded via dofile() by fibbenchmaster.lua, fibbenchcompute.lua and
-- fibbenchstorage.lua. All four files must live in the same directory.
--
-- Provides:
--   common.bigint   - arbitrary precision integer math (custom, no deps)
--   common.net      - modem discovery + send/receive helpers
--   common.ui       - coloured TUI primitives (gpu/screen based)
--   common.util     - formatting/misc helpers

local component = require("component")
local event     = require("event")
local serialization = require("serialization")
local computer  = require("computer")
local keyboard  = require("keyboard")

local common = {}

------------------------------------------------------------------
-- Protocol constants
------------------------------------------------------------------

common.PORT = 4790
common.PROTOCOL_VERSION = 1
common.HEARTBEAT_INTERVAL = 4      -- seconds between worker heartbeats
common.HEARTBEAT_TIMEOUT  = 13     -- master drops a silent worker after this
common.TASK_TIMEOUT       = 20     -- master reassigns a task after this
common.DEFAULT_CHUNK_LIMBS = 500   -- ~3,500 decimal digits per chunk (small on purpose:
                                    -- real OC drives/RAM are tiny, so chunking should
                                    -- kick in well before a single node's limits, not
                                    -- only after implausibly long uptimes)

------------------------------------------------------------------
-- BigInt
--
-- Representation: { sign = 1|-1, n = limbCount, [1]=leastSigLimb, ... }
-- Base 1e7 (DIGITS_PER_LIMB = 7) so that limb*limb (< 1e14) plus carry
-- accumulation stays comfortably inside Lua double integer precision
-- (2^53 ~= 9.007e15).
------------------------------------------------------------------

local bigint = {}
common.bigint = bigint

local BASE = 10000000
local DIGITS_PER_LIMB = 7
bigint.BASE = BASE
bigint.DIGITS_PER_LIMB = DIGITS_PER_LIMB
bigint.LOG10_PHI = 0.20898764024997873 -- log10((1+sqrt(5))/2), digits(F(n)) ~= n*this

local function yield()
  -- Cooperative yield so long-running loops don't trip OpenComputers'
  -- "too long without yielding" watchdog (default ~5s slice).
  os.sleep(0)
end
common.yield = yield

local function newBig(sign)
  return { sign = sign or 1, n = 0 }
end

local function trim(x)
  while x.n > 1 and x[x.n] == 0 do
    x[x.n] = nil
    x.n = x.n - 1
  end
  if x.n == 0 then x.n = 1; x[1] = 0 end
  if x.n == 1 and x[1] == 0 then x.sign = 1 end
  return x
end

function bigint.fromInt(v)
  v = math.floor(v)
  local x = newBig(v < 0 and -1 or 1)
  v = math.abs(v)
  if v == 0 then
    x.n = 1; x[1] = 0
    return x
  end
  local i = 0
  while v > 0 do
    i = i + 1
    x[i] = v % BASE
    v = math.floor(v / BASE)
  end
  x.n = i
  return x
end

function bigint.fromString(s)
  s = tostring(s)
  local sign = 1
  if s:sub(1, 1) == "-" then sign = -1; s = s:sub(2) end
  s = s:gsub("^0+(%d)", "%1")
  if s == "" then s = "0" end
  local x = newBig(sign)
  local i, pos = 0, #s
  while pos > 0 do
    local startPos = math.max(1, pos - DIGITS_PER_LIMB + 1)
    i = i + 1
    x[i] = tonumber(s:sub(startPos, pos))
    pos = startPos - 1
  end
  x.n = math.max(i, 1)
  return trim(x)
end

function bigint.toString(x)
  local parts = {}
  for i = x.n, 1, -1 do
    if i == x.n then
      parts[#parts + 1] = tostring(x[i])
    else
      parts[#parts + 1] = string.format("%07d", x[i])
    end
  end
  local s = table.concat(parts)
  if x.sign < 0 and not (x.n == 1 and x[1] == 0) then s = "-" .. s end
  return s
end

-- Exact digit count without stringifying the whole number.
function bigint.digitCount(x)
  if x.n == 1 and x[1] == 0 then return 1 end
  return (x.n - 1) * DIGITS_PER_LIMB + #tostring(x[x.n])
end

local function cmpMag(a, b)
  if a.n ~= b.n then return a.n < b.n and -1 or 1 end
  for i = a.n, 1, -1 do
    if a[i] ~= b[i] then return a[i] < b[i] and -1 or 1 end
  end
  return 0
end
bigint.cmpMag = cmpMag

function bigint.compare(a, b)
  if a.sign ~= b.sign then return a.sign < b.sign and -1 or 1 end
  local c = cmpMag(a, b)
  return a.sign > 0 and c or -c
end

local function addMag(a, b)
  local r = newBig(1)
  local n = math.max(a.n, b.n)
  local carry = 0
  for i = 1, n do
    local s = (a[i] or 0) + (b[i] or 0) + carry
    if s >= BASE then s = s - BASE; carry = 1 else carry = 0 end
    r[i] = s
    if i % 20000 == 0 then yield() end
  end
  if carry > 0 then n = n + 1; r[n] = carry end
  r.n = n
  return trim(r)
end

-- Requires |a| >= |b|
local function subMag(a, b)
  local r = newBig(1)
  local borrow = 0
  for i = 1, a.n do
    local s = a[i] - (b[i] or 0) - borrow
    if s < 0 then s = s + BASE; borrow = 1 else borrow = 0 end
    r[i] = s
    if i % 20000 == 0 then yield() end
  end
  r.n = a.n
  return trim(r)
end

function bigint.add(a, b)
  if a.sign == b.sign then
    local r = addMag(a, b); r.sign = a.sign; return trim(r)
  end
  local c = cmpMag(a, b)
  if c == 0 then return bigint.fromInt(0) end
  if c > 0 then
    local r = subMag(a, b); r.sign = a.sign; return trim(r)
  else
    local r = subMag(b, a); r.sign = b.sign; return trim(r)
  end
end

function bigint.neg(a)
  local r = { sign = -a.sign, n = a.n }
  for i = 1, a.n do r[i] = a[i] end
  return trim(r)
end

function bigint.sub(a, b)
  return bigint.add(a, bigint.neg(b))
end

function bigint.isZero(a)
  return a.n == 1 and a[1] == 0
end

-- Schoolbook multiply; also the Karatsuba base case.
local function mulSchool(a, b)
  local r = newBig(1)
  local rn = a.n + b.n
  for i = 1, rn do r[i] = 0 end
  for i = 1, a.n do
    local ai = a[i]
    if ai ~= 0 then
      local carry = 0
      for j = 1, b.n do
        local idx = i + j - 1
        local cur = r[idx] + ai * b[j] + carry
        r[idx] = cur % BASE
        carry = math.floor(cur / BASE)
      end
      local idx = i + b.n
      while carry > 0 do
        local cur = r[idx] + carry
        r[idx] = cur % BASE
        carry = math.floor(cur / BASE)
        idx = idx + 1
      end
    end
    if i % 400 == 0 then yield() end
  end
  r.n = rn
  return trim(r)
end

local KARATSUBA_THRESHOLD = 48 -- limbs; below this, schoolbook wins

local function shiftLimbs(a, k)
  if a.n == 1 and a[1] == 0 then return a end
  local r = newBig(a.sign)
  for i = 1, k do r[i] = 0 end
  for i = 1, a.n do r[k + i] = a[i] end
  r.n = a.n + k
  return r
end
bigint.shiftLimbs = shiftLimbs

local function splitAt(a, k)
  local lo = newBig(1)
  local ln = math.min(k, a.n)
  for i = 1, ln do lo[i] = a[i] end
  lo.n = math.max(ln, 1)
  trim(lo)
  local hi = newBig(1)
  local hn = 0
  for i = k + 1, a.n do hn = hn + 1; hi[hn] = a[i] end
  hi.n = math.max(hn, 1)
  trim(hi)
  return lo, hi
end

local mulMag -- forward declaration

local function karatsuba(a, b)
  if a.n <= KARATSUBA_THRESHOLD or b.n <= KARATSUBA_THRESHOLD then
    return mulSchool(a, b)
  end
  local m = math.floor(math.max(a.n, b.n) / 2)
  local a0, a1 = splitAt(a, m)
  local b0, b1 = splitAt(b, m)
  local z0 = mulMag(a0, b0)
  local z2 = mulMag(a1, b1)
  local sa = addMag(a0, a1)
  local sb = addMag(b0, b1)
  local z1 = mulMag(sa, sb)
  z1 = subMag(z1, addMag(z0, z2))
  local result = addMag(addMag(z0, shiftLimbs(z2, 2 * m)), shiftLimbs(z1, m))
  yield()
  return result
end

mulMag = function(a, b)
  if (a.n == 1 and a[1] == 0) or (b.n == 1 and b[1] == 0) then
    return bigint.fromInt(0)
  end
  return karatsuba(a, b)
end

function bigint.mul(a, b)
  local r = mulMag(a, b)
  r.sign = a.sign * b.sign
  return trim(r)
end

------------------------------------------------------------------
-- Fast doubling Fibonacci
--   F(2k)   = F(k) * (2*F(k+1) - F(k))
--   F(2k+1) = F(k)^2 + F(k+1)^2
-- Returns F(n), F(n+1) in O(log n) bigint multiplications - both
-- values fall out "for the price of one" recursive descent.
------------------------------------------------------------------

function bigint.fibFastDoubling(n)
  if n == 0 then
    return bigint.fromInt(0), bigint.fromInt(1)
  end
  local a, b = bigint.fibFastDoubling(math.floor(n / 2))
  local twoBMinusA = bigint.sub(bigint.add(b, b), a)
  local c = bigint.mul(a, twoBMinusA)                        -- F(2k)
  local d = bigint.add(bigint.mul(a, a), bigint.mul(b, b))   -- F(2k+1)
  yield()
  if n % 2 == 0 then
    return c, d
  else
    return d, bigint.add(c, d)
  end
end

------------------------------------------------------------------
-- Chunking: split/reassemble a bigint's limb array into fixed-size
-- pages, so a number far larger than any one node's RAM can be
-- spread across storage nodes and worked on piece by piece.
------------------------------------------------------------------

function bigint.toChunks(x, limbsPerChunk)
  local chunks, c, idx = {}, 0, 1
  while idx <= x.n do
    c = c + 1
    local chunk = {}
    for i = 1, limbsPerChunk do
      chunk[i] = x[idx] or 0
      idx = idx + 1
    end
    chunks[c] = chunk
  end
  if c == 0 then c = 1; chunks[1] = { 0 } end
  return chunks, c
end

function bigint.fromChunks(chunks, chunkCount, limbsPerChunk, sign)
  local x = newBig(sign or 1)
  local idx = 0
  for c = 1, chunkCount do
    local chunk = chunks[c]
    for i = 1, limbsPerChunk do
      idx = idx + 1
      x[idx] = chunk[i] or 0
    end
  end
  x.n = math.max(idx, 1)
  return trim(x)
end

-- Add two equal-length limb chunks (plain arrays, unsigned) plus an
-- incoming carry bit. Returns the result chunk and outgoing carry (0/1).
function bigint.chunkAdd(chunkA, chunkB, carryIn, limbsPerChunk)
  local out = {}
  local carry = carryIn or 0
  for i = 1, limbsPerChunk do
    local s = (chunkA and chunkA[i] or 0) + (chunkB and chunkB[i] or 0) + carry
    if s >= BASE then s = s - BASE; carry = 1 else carry = 0 end
    out[i] = s
  end
  return out, carry
end

function bigint.zeroChunk(limbsPerChunk)
  local z = {}
  for i = 1, limbsPerChunk do z[i] = 0 end
  return z
end

------------------------------------------------------------------
-- Memory calibration + bootstrap search
--
-- Empirically measures bytes-per-limb on THIS machine (rather than
-- guessing at Lua table/GC overhead), then uses the fact that
-- digits(F(n)) ~= n * log10(phi) to jump straight to an estimate of
-- the largest n whose F(n) fits in half of the machine's memory, and
-- refines it with a couple of measured correction passes.
------------------------------------------------------------------

local function gc()
  -- Some OpenOS builds/BIOS variants don't expose the standard Lua
  -- `collectgarbage` global. Fall back to no-op if it's missing rather
  -- than crashing calibration.
  if collectgarbage then collectgarbage() end
end

local function calibrateBytesPerLimb()
  gc()
  local free1 = computer.freeMemory()
  local probe = {}
  local N = 20000
  for i = 1, N do probe[i] = (i * 9973) % BASE end
  local free2 = computer.freeMemory()
  local delta = free1 - free2
  probe = nil
  gc()
  if delta <= 0 then delta = N * 16 end -- sane fallback if GC raced us
  return delta / N
end
common.calibrateBytesPerLimb = calibrateBytesPerLimb

-- progressCb(text) is called with human-readable status updates.
-- Returns: n, F(n), F(n+1), budgetBytes, bytesPerLimb
function common.bootstrapFindMaxFib(progressCb)
  local function report(s) if progressCb then progressCb(s) end end

  local totalMem = computer.totalMemory()
  local budgetBytes = totalMem * 0.1
  local bytesPerLimb = calibrateBytesPerLimb()
  local overhead = 1.2 -- fudge factor for bigint table/GC bookkeeping

  report(string.format("Total memory: %d bytes | budget (50%%): %d bytes",
    totalMem, math.floor(budgetBytes)))
  report(string.format("Calibrated ~%.1f bytes/limb", bytesPerLimb))

  local n = math.floor(((budgetBytes / (bytesPerLimb * overhead)) * DIGITS_PER_LIMB) / bigint.LOG10_PHI)
  n = math.max(n, 10)

  local best = nil
  for attempt = 1, 6 do
    report(string.format("Trial %d: computing F(%d) via fast doubling...", attempt, n))
    local a, b = bigint.fibFastDoubling(n)
    local actualBytes = b.n * bytesPerLimb * overhead
    report(string.format("  F(%d) uses %d limbs (~%d digits, ~%.0f bytes)",
      n, b.n, bigint.digitCount(b), actualBytes))
    if actualBytes <= budgetBytes then
      best = { n = n, a = a, b = b, bytes = actualBytes }
      local ratio = budgetBytes / math.max(actualBytes, 1)
      if ratio < 1.02 then break end
      local nextN = math.floor(n * math.min(ratio, 1.5))
      if nextN <= n then break end
      n = nextN
    else
      local ratio = budgetBytes / actualBytes
      local nextN = math.floor(n * ratio * 0.98)
      if best and nextN <= best.n then break end
      n = math.max(nextN, 1)
    end
  end

  if not best then
    n = math.max(n - 1, 1)
    local a, b = bigint.fibFastDoubling(n)
    best = { n = n, a = a, b = b, bytes = b.n * bytesPerLimb * overhead }
  end

  return best.n, best.a, best.b, budgetBytes, bytesPerLimb
end

------------------------------------------------------------------
-- Networking
------------------------------------------------------------------

local net = {}
common.net = net

function net.openModems()
  local modems = {}
  for address in component.list("modem") do
    local m = component.proxy(address)
    pcall(m.open, common.PORT)
    modems[#modems + 1] = m
  end
  return modems
end

function net.myAddress(modems)
  return modems[1] and modems[1].address or computer.address()
end

local seenIds = {}
local seenOrder = {}
local function alreadySeen(id)
  if seenIds[id] then return true end
  seenIds[id] = true
  seenOrder[#seenOrder + 1] = id
  if #seenOrder > 400 then
    local old = table.remove(seenOrder, 1)
    seenIds[old] = nil
  end
  return false
end

local msgCounter = 0
function net.send(modems, target, msg)
  msgCounter = msgCounter + 1
  msg.__id = net.myAddress(modems) .. ":" .. msgCounter
  local data = serialization.serialize(msg)
  for _, m in ipairs(modems) do
    pcall(m.send, target, common.PORT, data)
  end
end

function net.broadcast(modems, msg)
  msgCounter = msgCounter + 1
  msg.__id = net.myAddress(modems) .. ":b:" .. msgCounter
  local data = serialization.serialize(msg)
  for _, m in ipairs(modems) do
    pcall(m.broadcast, common.PORT, data)
  end
end

-- Blocks up to `timeout` seconds for the next *new* (deduped) FibBench
-- message. Returns msg, remoteAddress  OR  nil on timeout / non-matching event.
function net.pull(timeout)
  local e, _, remoteAddr, port, _, data = event.pull(timeout, "modem_message")
  if e == nil then return nil end
  if port ~= common.PORT or type(data) ~= "string" then return nil end
  local ok, msg = pcall(serialization.unserialize, data)
  if not ok or type(msg) ~= "table" then return nil end
  if msg.__id and alreadySeen(msg.__id) then return nil end
  return msg, remoteAddr
end

-- Send `req` to `target` and wait for a reply matching expectType +
-- req.replyId, retrying the send a few times if nothing comes back.
-- Returns reply, remoteAddr  OR  nil, "timeout"
function net.request(modems, target, req, expectType, opts)
  opts = opts or {}
  local attempts = opts.attempts or 4
  local perWait = opts.perWait or 3
  for _ = 1, attempts do
    net.send(modems, target, req)
    for _ = 1, 50 do
      local msg, remoteAddr = net.pull(perWait)
      if msg == nil then break end
      if msg.type == expectType and msg.replyId == req.replyId then
        return msg, remoteAddr
      end
    end
  end
  return nil, "timeout"
end

net.keyboard = keyboard

------------------------------------------------------------------
-- Misc utilities
------------------------------------------------------------------

local util = {}
common.util = util

function util.commas(n)
  n = math.floor(n)
  local s = tostring(n)
  local sign = ""
  if s:sub(1,1) == "-" then sign = "-"; s = s:sub(2) end
  local out = s:reverse():gsub("(%d%d%d)", "%1,"):reverse()
  out = out:gsub("^,", "")
  return sign .. out
end

function util.formatBytes(b)
  if b >= 1024 * 1024 * 1024 then
    return string.format("%.2f GB", b / (1024*1024*1024))
  elseif b >= 1024 * 1024 then
    return string.format("%.2f MB", b / (1024*1024))
  elseif b >= 1024 then
    return string.format("%.2f KB", b / 1024)
  end
  return string.format("%d B", b)
end

function util.formatDuration(s)
  s = math.floor(s)
  local h = math.floor(s / 3600)
  local m = math.floor((s % 3600) / 60)
  local sec = s % 60
  if h > 0 then return string.format("%dh %dm %ds", h, m, sec) end
  if m > 0 then return string.format("%dm %ds", m, sec) end
  return string.format("%ds", sec)
end

function util.shortId(addr)
  if not addr then return "??????" end
  return addr:sub(1, 6)
end

local seriesCounter = 0
function util.newSeriesId()
  seriesCounter = seriesCounter + 1
  return string.format("fb%d%04x", os.time(), math.random(0, 0xFFFF))
end

function util.tableCount(t)
  local c = 0
  for _ in pairs(t) do c = c + 1 end
  return c
end

------------------------------------------------------------------
-- TUI primitives (gpu/screen based, coloured)
------------------------------------------------------------------

local ui = {}
common.ui = ui

ui.palette = {
  bg        = 0x0B0E14,
  panel     = 0x141925,
  header    = 0x1F2A44,
  border    = 0x2B3A55,
  accent    = 0x4FD1C5,
  accent2   = 0x8E7CFF,
  text      = 0xE6E8EE,
  dim       = 0x7C8494,
  good      = 0x3FCB6A,
  warn      = 0xF2C744,
  bad       = 0xE85C5C,
}

function ui.init(title)
  local gpuAddr = component.list("gpu")()
  local screenAddr = component.list("screen")()
  if not gpuAddr or not screenAddr then
    error("This program needs a GPU and a screen attached.")
  end
  local gpu = component.proxy(gpuAddr)
  gpu.bind(screenAddr)
  local w, h = gpu.maxResolution()
  gpu.setResolution(w, h)
  gpu.setBackground(ui.palette.bg)
  gpu.setForeground(ui.palette.text)
  gpu.fill(1, 1, w, h, " ")
  ui.gpu, ui.width, ui.height = gpu, w, h
  if title then ui.drawHeader(title) end
  return gpu, w, h
end

function ui.drawHeader(title, subtitle)
  local gpu, w = ui.gpu, ui.width
  gpu.setBackground(ui.palette.header)
  gpu.fill(1, 1, w, 1, " ")
  gpu.setForeground(ui.palette.accent)
  gpu.set(2, 1, title)
  if subtitle then
    gpu.setForeground(ui.palette.dim)
    gpu.set(math.max(2, w - #subtitle - 1), 1, subtitle)
  end
  gpu.setBackground(ui.palette.bg)
  gpu.setForeground(ui.palette.text)
end

function ui.footer(text)
  local gpu, w, h = ui.gpu, ui.width, ui.height
  gpu.setBackground(ui.palette.header)
  gpu.fill(1, h, w, 1, " ")
  gpu.setForeground(ui.palette.dim)
  gpu.set(2, h, text:sub(1, w - 2))
  gpu.setBackground(ui.palette.bg)
  gpu.setForeground(ui.palette.text)
end

function ui.box(x, y, w, h, label)
  local gpu = ui.gpu
  gpu.setForeground(ui.palette.border)
  gpu.set(x, y, "+" .. string.rep("-", w - 2) .. "+")
  for i = 1, h - 2 do
    gpu.set(x, y + i, "|")
    gpu.set(x + w - 1, y + i, "|")
  end
  gpu.set(x, y + h - 1, "+" .. string.rep("-", w - 2) .. "+")
  if label then
    gpu.setForeground(ui.palette.accent)
    gpu.set(x + 2, y, " " .. label .. " ")
  end
  gpu.setForeground(ui.palette.text)
end

function ui.clearArea(x, y, w, h)
  local gpu = ui.gpu
  gpu.setBackground(ui.palette.bg)
  gpu.fill(x, y, w, h, " ")
end

function ui.text(x, y, s, color, w)
  local gpu = ui.gpu
  gpu.setForeground(color or ui.palette.text)
  if w then
    ui.clearArea(x, y, w, 1)
    gpu.setBackground(ui.palette.bg)
  end
  gpu.set(x, y, s)
  gpu.setForeground(ui.palette.text)
end

function ui.progressBar(x, y, w, frac, color)
  local gpu = ui.gpu
  frac = math.max(0, math.min(1, frac))
  local filled = math.floor(w * frac)
  gpu.setForeground(color or ui.palette.accent)
  gpu.set(x, y, string.rep("=", filled))
  gpu.setForeground(ui.palette.border)
  gpu.set(x + filled, y, string.rep(".", w - filled))
  gpu.setForeground(ui.palette.text)
end

-- Scrolling log panel object.
function ui.newLog(x, y, w, h)
  local log = { x = x, y = y, w = w, h = h, lines = {} }
  function log:push(msg, color)
    table.insert(self.lines, { text = msg, color = color or ui.palette.text })
    while #self.lines > self.h do table.remove(self.lines, 1) end
    self:draw()
  end
  function log:draw()
    local gpu = ui.gpu
    for i = 1, self.h do
      gpu.setBackground(ui.palette.bg)
      gpu.fill(self.x, self.y + i - 1, self.w, 1, " ")
      local line = self.lines[#self.lines - self.h + i]
      if line then
        gpu.setForeground(line.color)
        gpu.set(self.x, self.y + i - 1, line.text:sub(1, self.w))
      end
    end
    gpu.setForeground(ui.palette.text)
  end
  return log
end

------------------------------------------------------------------
-- Checkpoint persistence (plain files via OpenOS io)
------------------------------------------------------------------

function common.saveTable(path, tbl)
  local f, err = io.open(path, "w")
  if not f then return false, err end
  f:write(serialization.serialize(tbl))
  f:close()
  return true
end

function common.loadTable(path)
  local f = io.open(path, "r")
  if not f then return nil, "not found" end
  local data = f:read("*a")
  f:close()
  local ok, tbl = pcall(serialization.unserialize, data)
  if not ok then return nil, "corrupt checkpoint" end
  return tbl
end

return common