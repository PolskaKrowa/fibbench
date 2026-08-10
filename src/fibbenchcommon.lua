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
common.PROTOCOL_VERSION = 2       -- v2: adds task_chunk_gp / task_chunk_final
common.HEARTBEAT_INTERVAL = 4      -- seconds between worker heartbeats
common.HEARTBEAT_TIMEOUT  = 13     -- master drops a silent worker after this
common.TASK_TIMEOUT       = 20     -- master reassigns a task after this
common.DEFAULT_CHUNK_LIMBS = 500   -- ~3,500 decimal digits per chunk (small on purpose:
                                    -- real OC drives/RAM are tiny, so chunking should
                                    -- kick in well before a single node's limits, not
                                    -- only after implausibly long uptimes)

-- Task / reply message types understood by compute nodes.
-- (Listed here so the master and compute node agree on string literals.)
common.MSG = {
  -- Master -> compute
  TASK_CHUNK_ADD    = "task_chunk_add",     -- legacy: fetch A,B + sequential carry-chain add
  TASK_CHUNK_GP     = "task_chunk_gp",      -- Kogge-Stone phase 1: per-chunk (gOut, pOut)
  TASK_CHUNK_FINAL  = "task_chunk_final",   -- Kogge-Stone phase 3: final sum w/ known carryIn
  WELCOME           = "welcome",
  MASTER_SHUTDOWN   = "master_shutdown",

  -- Compute -> master
  HELLO_COMPUTE     = "hello_compute",
  TASK_DONE         = "task_done",          -- reply to TASK_CHUNK_ADD / TASK_CHUNK_FINAL
  TASK_GP_DONE      = "task_gp_done",       -- reply to TASK_CHUNK_GP
  HEARTBEAT         = "heartbeat",
  BYE               = "bye",

  -- Storage ops (master or compute -> storage)
  FETCH_CHUNK       = "fetch_chunk",
  STORE_CHUNK       = "store_chunk",
  CHUNK_DATA        = "chunk_data",
  STORE_ACK         = "store_ack",
}

------------------------------------------------------------------
-- BigInt
--
-- Representation: { sign = 1|-1, n = limbCount, [1]=leastSigLimb, ... }
-- Base 1e7 (DIGITS_PER_LIMB = 7) so that limb*limb (< 1e14) plus carry
-- accumulation stays comfortably inside Lua double integer precision
-- (2^53 ~= 9.007e15).
--
-- Algorithm notes
--   * Addition uses a Kogge-Stone parallel-prefix carry network. The
--     carry into every limb is computed in O(log n) "rounds" of pairwise
--     (g, p) combination, after which the final limb sums are produced
--     in a single forward pass. In single-threaded Lua this is still
--     serial at the instruction level, but the algorithmic structure
--     is what powers the distributed chunk pipeline (chunkGenProp +
--     combineGenProp + chunkFinalAdd): when a number is spread across
--     N compute nodes, the master can run an O(log N)-rounds reduction
--     instead of the previous O(N) sequential carry chain.
--   * Multiplication dispatches schoolbook -> Karatsuba -> Toom-Cook 3
--     as operands grow. Toom-Cook 3 is O(n^log_3(5)) ~= O(n^1.465),
--     which beats Karatsuba's O(n^1.585) and noticeably shortens the
--     fast-doubling Fibonacci bootstrap.
--   * Squaring has its own dedicated path (schoolbook, KaratsubaSqr,
--     Toom-Cook 3 squaring) for the same reason.
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

------------------------------------------------------------------
-- Kogge-Stone parallel-prefix addition
--
-- For each limb position i, we classify the (a[i], b[i]) pair:
--   g[i] = 1  if a[i] + b[i] >= BASE         (this position GENERATES a carry-out)
--   p[i] = 1  if a[i] + b[i] == BASE - 1       (this position PROPAGATES a carry-in)
-- The carry INTO position i is the OR over all j<i of (g[j] AND (AND_{k=j+1..i-1} p[k])).
-- The parallel-prefix operator combines two adjacent blocks:
--   (g2,p2) ∘ (g1,p1) = (g2 OR (p2 AND g1),  p2 AND p1)
-- which is *associative*, so a Kogge-Stone tree of log2(n) levels gives
-- every position its prefix carry in O(n log n) work.
--
-- In single-threaded Lua this is more work than the linear carry chain,
-- but the same primitive powers the distributed chunk pipeline (see
-- chunkGenProp / combineGenProp / chunkFinalAdd below), where the log n
-- structure cuts master-side rounds from O(N) to O(log N).
--
-- A `bigint.USE_KOGGE_STONE_ADD` flag (default true) lets callers fall
-- back to the sequential chain for very small operands if desired.
------------------------------------------------------------------

bigint.USE_KOGGE_STONE_ADD = true

-- Sequential carry-chain adder (kept as the fallback / reference).
local function addMagSeq(a, b)
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

-- Kogge-Stone parallel-prefix adder (the new default).
local function addMagKS(a, b)
  local n = math.max(a.n, b.n)
  if n == 0 then return bigint.fromInt(0) end

  -- Tiny operands: the prefix tree overhead is not worth it; defer to
  -- the linear chain. The crossover was tuned for the BASE-1e7 layout
  -- (each limb is ~23 bits, so a 4-limb number is ~92 bits).
  if n < 8 then
    return addMagSeq(a, b)
  end

  -- Pass 1: compute partial sums + generate/propagate bits.
  -- We pack g and p into Lua arrays of 0/1 numbers for cache friendliness.
  local g  = {}     -- g[i] = 1 if (a[i]+b[i]) generates a carry out
  local p  = {}     -- p[i] = 1 if (a[i]+b[i]) propagates a carry in
  local s  = {}     -- s[i] = (a[i]+b[i]) mod BASE  (partial sum, no carry-in yet)
  for i = 1, n do
    local ai = a[i] or 0
    local bi = b[i] or 0
    local sum = ai + bi
    if sum >= BASE then
      g[i] = 1; p[i] = 0; s[i] = sum - BASE
    elseif sum == BASE - 1 then
      g[i] = 0; p[i] = 1; s[i] = sum
    else
      g[i] = 0; p[i] = 0; s[i] = sum
    end
    if i % 20000 == 0 then yield() end
  end

  -- Pass 2: Kogge-Stone parallel-prefix scan.
  -- Invariant at the start of each "round" with offset d:
  --   g[i] = the carry-out generated by the block [i-d+1 .. i]
  --   p[i] = whether that block propagates a carry end-to-end
  -- After log2(n) rounds, g[i] is the carry-out of the entire prefix [1..i].
  local d = 1
  while d < n do
    for i = n, d + 1, -1 do     -- sweep right-to-left so we don't clobber in-flight inputs
      local pi = p[i]
      local gi = g[i]
      local gL = g[i - d]
      -- new g = g[i] OR (p[i] AND g[i-d])
      if pi == 1 and gL == 1 then
        g[i] = 1
      end
      -- new p = p[i] AND p[i-d]   (only changes if p[i] is currently 1)
      if pi == 1 and p[i - d] == 0 then
        p[i] = 0
      end
    end
    d = d * 2
    if d <= n and math.floor(d / 2) % 65536 == 0 then yield() end
  end

  -- Pass 3: final sums. carry into position 1 is 0; carry into position i>1
  -- is g[i-1]. The "+carry" may push a partial sum from BASE-1 to BASE,
  -- in which case the limb becomes 0 and the outgoing carry is captured
  -- by g[i] (which is already 1 because p[i] was 1).
  local r = newBig(1)
  local carry = 0
  for i = 1, n do
    local si = s[i] + carry
    if si >= BASE then si = si - BASE; carry = 1 else carry = g[i] end
    r[i] = si
    if i % 20000 == 0 then yield() end
  end
  if carry > 0 then n = n + 1; r[n] = carry end
  r.n = n
  return trim(r)
end

local function addMag(a, b)
  if bigint.USE_KOGGE_STONE_ADD then
    return addMagKS(a, b)
  end
  return addMagSeq(a, b)
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

------------------------------------------------------------------
-- Toom-Cook 3-way multiplication
--
-- Splits each operand into 3 chunks (a = a0 + a1*B^m + a2*B^2m),
-- evaluates both polynomials at 5 points {0, 1, -1, 2, infinity},
-- multiplies pointwise (5 recursive calls of size ~n/3), and interpolates
-- back via the standard Bodrato sequence (one div-by-2, one div-by-2,
-- one div-by-3). Asymptotic cost O(n^log_3(5)) ~= O(n^1.465), beating
-- Karatsuba's O(n^1.585). For the fast-doubling Fibonacci bootstrap,
-- where the operands routinely reach 10^4 - 10^6 limbs, this is the
-- single biggest algorithmic win available without going to an FFT.
--
-- Threshold: set generously above Karatsuba's crossover, because the
-- evaluation/interpolation overhead (5 adds, 2 subs, 3 shifts, 3 small
-- divisions per call) only pays off at reasonably large sizes.
------------------------------------------------------------------

local TOOM3_THRESHOLD = 240 -- limbs; above this, Toom-Cook 3 wins

-- Small-integer division helper. Divides a signed bigint by a small
-- positive integer d. Toom-Cook 3 guarantees exact divisibility, so we
-- don't need to handle remainder semantics; we just truncate to zero
-- remainder when we encounter it (which is the mathematically correct
-- result for an exactly-divisible input).
local function divSmall(x, d)
  if x.n == 1 and x[1] == 0 then return bigint.fromInt(0) end
  local sign = x.sign
  local r = newBig(1)
  local carry = 0
  for i = x.n, 1, -1 do
    local cur = carry * BASE + x[i]
    r[i] = math.floor(cur / d)
    carry = cur % d
  end
  r.n = x.n
  r.sign = sign
  return trim(r)
end
bigint.divSmall = divSmall

-- Split a bigint into 3 magnitude pieces at split point m:
--   x = x0 + x1*B^m + x2*B^(2m)   (x2 may be smaller than m limbs)
local function split3(x, m)
  local x0 = newBig(1)
  local n0 = math.min(m, x.n)
  for i = 1, n0 do x0[i] = x[i] end
  x0.n = math.max(n0, 1); trim(x0)

  local x1 = newBig(1)
  local n1 = 0
  for i = m + 1, math.min(2 * m, x.n) do n1 = n1 + 1; x1[n1] = x[i] end
  x1.n = math.max(n1, 1); trim(x1)

  local x2 = newBig(1)
  local n2 = 0
  for i = 2 * m + 1, x.n do n2 = n2 + 1; x2[n2] = x[i] end
  x2.n = math.max(n2, 1); trim(x2)

  return x0, x1, x2
end

local toom3 -- forward declaration

local function toom3(a, b)
  -- Both operands are magnitudes (signs are handled by bigint.mul, which
  -- sets the result sign based on a.sign * b.sign *after* mulMag returns).
  local m = math.ceil(math.max(a.n, b.n) / 3)
  if m < 2 then
    -- Operands too small to split sensibly - fall back to Karatsuba.
    return karatsuba(a, b)
  end

  local a0, a1, a2 = split3(a, m)
  local b0, b1, b2 = split3(b, m)

  -- Evaluate a(x) at the 5 evaluation points. We use bigint.add/sub
  -- throughout because intermediate values can be negative (e.g.
  -- a(-1) = a0 - a1 + a2 can be negative).
  local aP1  = bigint.add(bigint.add(a0, a1), a2)            -- a(1)  = a0 + a1 + a2
  local aM1  = bigint.add(bigint.sub(a0, a1), a2)            -- a(-1) = a0 - a1 + a2
  local twoA1 = bigint.add(a1, a1)
  local twoA2 = bigint.add(a2, a2)
  local fourA2 = bigint.add(twoA2, twoA2)
  local aP2  = bigint.add(bigint.add(a0, twoA1), fourA2)    -- a(2)  = a0 + 2*a1 + 4*a2

  local bP1  = bigint.add(bigint.add(b0, b1), b2)
  local bM1  = bigint.add(bigint.sub(b0, b1), b2)
  local twoB1 = bigint.add(b1, b1)
  local twoB2 = bigint.add(b2, b2)
  local fourB2 = bigint.add(twoB2, twoB2)
  local bP2  = bigint.add(bigint.add(b0, twoB1), fourB2)

  -- Pointwise products. These dispatch back through mulMag -> toom3 if
  -- the operands are still big enough, otherwise Karatsuba/schoolbook.
  -- bigint.mul is used (not mulMag directly) so signs are tracked through
  -- the interpolation math.
  local W0   = bigint.mul(a0,  b0)     -- c(0)   = c0
  local W1   = bigint.mul(aP1, bP1)    -- c(1)
  local Wm1  = bigint.mul(aM1, bM1)    -- c(-1)
  local W2   = bigint.mul(aP2, bP2)    -- c(2)
  local WInf = bigint.mul(a2,  b2)     -- c(inf) = c4

  -- Interpolation (Bodrato's sequence).
  -- We're solving for r0, r1, r2, r3, r4 in:
  --   c(x) = r0 + r1*x + r2*x^2 + r3*x^3 + r4*x^4
  -- Then the bigint result = r0 + r1*B^m + r2*B^(2m) + r3*B^(3m) + r4*B^(4m).
  --
  -- Standard identities:
  --   r0 = W0
  --   r4 = WInf
  --   r2 = (W1 + Wm1)/2 - r0 - r4
  --   D  = (W1 - Wm1)/2                          (= r1 + r3)
  --   E  = (W2 - r0 - 4*r2 - 16*r4)/2            (= r1 + 4*r3)
  --   r3 = (E - D) / 3
  --   r1 = D - r3
  local r0 = W0
  local r4 = WInf

  local Wsum  = bigint.add(W1, Wm1)
  local r2    = bigint.sub(bigint.sub(divSmall(Wsum, 2), r0), r4)

  local Wdiff = bigint.sub(W1, Wm1)
  local D     = divSmall(Wdiff, 2)

  local fourR2  = bigint.add(bigint.add(r2, r2), bigint.add(r2, r2))   -- 4*r2
  local twoR4   = bigint.add(r4, r4)
  local fourR4  = bigint.add(twoR4, twoR4)
  local eightR4 = bigint.add(fourR4, fourR4)
  local sixteenR4 = bigint.add(eightR4, eightR4)            -- 16*r4
  local Enum    = bigint.sub(bigint.sub(bigint.sub(W2, r0), fourR2), sixteenR4)
  local E       = divSmall(Enum, 2)

  local r3 = divSmall(bigint.sub(E, D), 3)
  local r1 = bigint.sub(D, r3)

  -- Reassemble: r0 + r1*B^m + r2*B^(2m) + r3*B^(3m) + r4*B^(4m)
  local result = r0
  result = bigint.add(result, shiftLimbs(r1, m))
  result = bigint.add(result, shiftLimbs(r2, 2 * m))
  result = bigint.add(result, shiftLimbs(r3, 3 * m))
  result = bigint.add(result, shiftLimbs(r4, 4 * m))

  yield()
  return result
end

mulMag = function(a, b)
  if (a.n == 1 and a[1] == 0) or (b.n == 1 and b[1] == 0) then
    return bigint.fromInt(0)
  end
  if a.n >= TOOM3_THRESHOLD and b.n >= TOOM3_THRESHOLD then
    return toom3(a, b)
  end
  return karatsuba(a, b)
end

function bigint.mul(a, b)
  local r = mulMag(a, b)
  r.sign = a.sign * b.sign
  return trim(r)
end

------------------------------------------------------------------
-- Dedicated squaring
--
-- x^2 needs only the n(n+1)/2 distinct products a[i]*a[j] (i<=j),
-- with cross terms (i~=j) counted twice, instead of the full n^2
-- schoolbook grid mul(a,a) computes. At the Karatsuba level, splitting
-- x = a0*B^m + a1 gives x^2 = a0^2*B^2m + 2*a0*a1*B^m + a1^2: two
-- recursive squarings plus ONE generic multiply for the cross term
-- (instead of three generic multiplies). Note: because that cross
-- term is still a full generic multiply, the saving compounds only
-- linearly with n rather than shrinking the O(n^1.585) exponent --
-- expect ~5-10% at real bootstrap sizes (tens/hundreds of thousands
-- of limbs), not a multiplicative speedup. It's still free, so it's
-- applied everywhere, including inside fast doubling below.
------------------------------------------------------------------

local function sqrSchool(a)
  local n = a.n
  local rn = 2 * n
  local r = newBig(1)
  for i = 1, rn do r[i] = 0 end
  -- Doubled cross terms a[i]*a[j] for i<j.
  for i = 1, n do
    local ai = a[i]
    if ai ~= 0 then
      local carry = 0
      for j = i + 1, n do
        local idx = i + j - 1
        local cur = r[idx] + 2 * ai * a[j] + carry
        r[idx] = cur % BASE
        carry = math.floor(cur / BASE)
      end
      local idx = i + n
      while carry > 0 do
        local cur = r[idx] + carry
        r[idx] = cur % BASE
        carry = math.floor(cur / BASE)
        idx = idx + 1
      end
    end
    if i % 400 == 0 then yield() end
  end
  -- Diagonal terms a[i]^2.
  for i = 1, n do
    local ai = a[i]
    if ai ~= 0 then
      local idx = 2 * i - 1
      local cur = r[idx] + ai * ai
      r[idx] = cur % BASE
      local carry = math.floor(cur / BASE)
      idx = idx + 1
      while carry > 0 do
        local cur2 = r[idx] + carry
        r[idx] = cur2 % BASE
        carry = math.floor(cur2 / BASE)
        idx = idx + 1
      end
    end
    if i % 2000 == 0 then yield() end
  end
  r.n = rn
  return trim(r)
end

local sqrMag -- forward declaration

local function karatsubaSqr(a)
  if a.n <= KARATSUBA_THRESHOLD then
    return sqrSchool(a)
  end
  local m = math.floor(a.n / 2)
  local a0, a1 = splitAt(a, m)
  local z0 = sqrMag(a0)
  local z2 = sqrMag(a1)
  local cross = mulMag(a0, a1)
  local crossDoubled = addMag(cross, cross)
  local result = addMag(addMag(z0, shiftLimbs(z2, 2 * m)), shiftLimbs(crossDoubled, m))
  yield()
  return result
end

------------------------------------------------------------------
-- Toom-Cook 3-way squaring
--
-- Same shape as toom3, but the 5 pointwise products become squarings,
-- which costs ~50% less at the schoolbook level and ~10% less at the
-- Karatsuba level. Since fibFastDoubling does TWO squarings per step
-- (sqr(a) + sqr(b)), this saving compounds through the recursion and
-- is the single biggest bootstrap win after toom3 mul itself.
--
-- The evaluation/interpolation math is identical to toom3 because
-- a(x)*a(x) is just c(x) = a(x)^2 with the same coefficients r0..r4.
------------------------------------------------------------------

local function toom3Sqr(a)
  local m = math.ceil(a.n / 3)
  if m < 2 then
    return karatsubaSqr(a)
  end

  local a0, a1, a2 = split3(a, m)

  -- Evaluate a(x) at the 5 evaluation points.
  local aP1   = bigint.add(bigint.add(a0, a1), a2)
  local aM1   = bigint.add(bigint.sub(a0, a1), a2)
  local twoA1 = bigint.add(a1, a1)
  local twoA2 = bigint.add(a2, a2)
  local fourA2 = bigint.add(twoA2, twoA2)
  local aP2   = bigint.add(bigint.add(a0, twoA1), fourA2)

  -- Pointwise SQUARINGS (instead of multiplications).
  local W0   = bigint.sqr(a0)     -- c(0)   = r0
  local W1   = bigint.sqr(aP1)    -- c(1)
  local Wm1  = bigint.sqr(aM1)    -- c(-1)
  local W2   = bigint.sqr(aP2)    -- c(2)
  local WInf = bigint.sqr(a2)     -- c(inf) = r4

  -- Identical interpolation sequence as toom3.
  local r0 = W0
  local r4 = WInf

  local Wsum  = bigint.add(W1, Wm1)
  local r2    = bigint.sub(bigint.sub(divSmall(Wsum, 2), r0), r4)

  local Wdiff = bigint.sub(W1, Wm1)
  local D     = divSmall(Wdiff, 2)

  local fourR2  = bigint.add(bigint.add(r2, r2), bigint.add(r2, r2))
  local twoR4   = bigint.add(r4, r4)
  local fourR4  = bigint.add(twoR4, twoR4)
  local eightR4 = bigint.add(fourR4, fourR4)
  local sixteenR4 = bigint.add(eightR4, eightR4)
  local Enum    = bigint.sub(bigint.sub(bigint.sub(W2, r0), fourR2), sixteenR4)
  local E       = divSmall(Enum, 2)

  local r3 = divSmall(bigint.sub(E, D), 3)
  local r1 = bigint.sub(D, r3)

  local result = r0
  result = bigint.add(result, shiftLimbs(r1, m))
  result = bigint.add(result, shiftLimbs(r2, 2 * m))
  result = bigint.add(result, shiftLimbs(r3, 3 * m))
  result = bigint.add(result, shiftLimbs(r4, 4 * m))

  yield()
  return result
end

sqrMag = function(a)
  if a.n == 1 and a[1] == 0 then
    return bigint.fromInt(0)
  end
  if a.n >= TOOM3_THRESHOLD then
    return toom3Sqr(a)
  end
  return karatsubaSqr(a)
end

function bigint.sqr(a)
  local r = sqrMag(a)
  r.sign = 1
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
  local d = bigint.add(bigint.sqr(a), bigint.sqr(b))         -- F(2k+1)
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
-- Distributed Kogge-Stone chunk primitives
--
-- The legacy chunkAdd() does a sequential carry chain *within* a chunk
-- and reports a single carryOut to the master. The master then has to
-- chain N chunks sequentially (carryOut[i] -> carryIn[i+1]) over the
-- network: O(N) sequential round-trips.
--
-- The Kogge-Stone parallel-prefix decomposition below lets the master do
-- the cross-chunk carry propagation in O(log N) rounds instead, by
-- treating each chunk as a single (generate, propagate) bit pair and
-- tree-reducing them in parallel:
--
--   Phase 1 (parallel across workers):
--     each worker fetches its A, B chunks and computes per-limb (g, p)
--     arrays via chunkGenProp, then reduces them to a single per-chunk
--     (gOut, pOut) pair via chunkReduceGP. The (gOut, pOut) is ~2 bits.
--
--   Phase 2 (master-side prefix scan, O(log N) rounds, no network):
--     the master applies combineGP() pairwise across all chunks to get
--     each chunk's carry-in bit. (gOut_acc[i] is the carry-out of the
--     prefix [0..i], so carry_in[i] = gOut_acc[i-1], carry_in[0] = 0.)
--
--   Phase 3 (parallel across workers):
--     each worker fetches its A, B chunks AGAIN (or has cached them)
--     and computes the final sum with its now-known carryIn via
--     chunkFinalAdd. Returns result chunk + carryOut.
--
-- The math: for chunks of length L, gOut = 1 iff (a[i]+b[i]) generates
-- a carry out of the chunk assuming carry-in 0; pOut = 1 iff the chunk
-- propagates an incoming carry end-to-end. combineGP is the associative
-- Kogge-Stone block operator:
--   (g2,p2) ∘ (g1,p1) = (g2 OR (p2 AND g1), p1 AND p2)
-- where (g1,p1) is the LOWER block and (g2,p2) is the UPPER block.
--
-- The protocol additions (task_chunk_gp / task_chunk_final) live in
-- fibbenchcompute.lua. The master side is described in the protocol
-- notes below; it can be added to fibbenchmaster.lua without changing
-- the existing task_chunk_add path.
------------------------------------------------------------------

-- Phase 1 helper: per-limb generate/propagate bits for a chunk.
-- Returns two arrays (g, p), each of length limbsPerChunk, with 0/1 entries:
--   g[i] = 1 if (A[i] + B[i]) generates a carry-out of position i
--   p[i] = 1 if (A[i] + B[i]) propagates a carry-in through position i
-- (i.e., p[i]=1 iff A[i]+B[i] == BASE-1; g[i]=1 iff A[i]+B[i] >= BASE.)
function bigint.chunkGenProp(chunkA, chunkB, limbsPerChunk)
  local g, p = {}, {}
  for i = 1, limbsPerChunk do
    local ai = (chunkA and chunkA[i]) or 0
    local bi = (chunkB and chunkB[i]) or 0
    local s = ai + bi
    if s >= BASE then
      g[i] = 1; p[i] = 0
    elseif s == BASE - 1 then
      g[i] = 0; p[i] = 1
    else
      g[i] = 0; p[i] = 0
    end
  end
  return g, p
end

-- Phase 1 helper: reduce a chunk's per-limb (g, p) arrays into a single
-- per-chunk (gOut, pOut) bit pair by running Kogge-Stone within the chunk.
--   gOut = 1 iff the chunk generates a carry-out (assuming carry-in 0)
--   pOut = 1 iff the chunk propagates a carry-in end-to-end
-- The reduction is O(L log L) for a chunk of L limbs; cheap relative to
-- the network round-trip a worker had to do to fetch the chunk.
function bigint.chunkReduceGP(g, p, limbsPerChunk)
  -- Kogge-Stone prefix scan over the per-limb (g, p) arrays.
  -- g_acc[i] will end up = prefix-carry-out of [1..i].
  -- p_acc[i] will end up = whether [1..i] propagates a carry end-to-end.
  local gAcc, pAcc = {}, {}
  for i = 1, limbsPerChunk do gAcc[i] = g[i]; pAcc[i] = p[i] end

  local d = 1
  while d < limbsPerChunk do
    for i = limbsPerChunk, d + 1, -1 do
      local gL = gAcc[i - d]
      local pL = pAcc[i - d]
      local newG = gAcc[i]
      if pAcc[i] == 1 and gL == 1 then newG = 1 end
      local newP = (pAcc[i] == 1 and pL == 1) and 1 or 0
      gAcc[i] = newG
      pAcc[i] = newP
    end
    d = d * 2
  end
  -- Final per-chunk gOut/pOut come from the last limb.
  local gOut = gAcc[limbsPerChunk] or 0
  local pOut = pAcc[limbsPerChunk] or 0
  return gOut, pOut
end

-- Phase 2 helper: combine two adjacent chunk-level (g, p) pairs into one.
-- (gA, pA) is the LOWER block, (gB, pB) is the UPPER block.
-- Returns (g, p) for the combined block:
--   g = gB OR (pB AND gA)
--   p = pA AND pB
-- All inputs are 0/1 numbers. This is the associative Kogge-Stone operator.
function bigint.combineGP(gA, pA, gB, pB)
  local g = (gB == 1 or (pB == 1 and gA == 1)) and 1 or 0
  local p = (pA == 1 and pB == 1) and 1 or 0
  return g, p
end

-- Convenience: run a Kogge-Stone prefix scan over a list of per-chunk
-- (g, p) pairs. Returns gOut[i] = carry-out of the prefix [1..i],
-- so carryIn[i] = gOut[i-1] (with gOut[0] = 0 conceptually).
-- This is what the master calls after collecting all workers' Phase-1
-- outputs. O(C log C) work, no network.
function bigint.prefixScanGP(gpList, count)
  -- gpList is a 1-indexed array of {g=.., p=..} tables (mutable copies).
  -- We do an in-place Kogge-Stone scan: at each "round" d, combine
  -- gpList[i] with gpList[i-d] for i = d+1..count.
  -- To keep this allocation-light, we mutate gpList in place.
  local gAcc, pAcc = {}, {}
  for i = 1, count do
    gAcc[i] = gpList[i].g or 0
    pAcc[i] = gpList[i].p or 0
  end
  local d = 1
  while d < count do
    for i = count, d + 1, -1 do
      local gL = gAcc[i - d]
      local pL = pAcc[i - d]
      local newG = (gAcc[i] == 1 or (pAcc[i] == 1 and gL == 1)) and 1 or 0
      local newP = (pAcc[i] == 1 and pL == 1) and 1 or 0
      gAcc[i] = newG
      pAcc[i] = newP
    end
    d = d * 2
  end
  return gAcc, pAcc
end

-- Phase 3 helper: compute the final sum of a chunk given a known carryIn.
-- This is just the second half of addMagKS specialized to plain arrays.
-- Returns (resultChunk, carryOut).
function bigint.chunkFinalAdd(chunkA, chunkB, carryIn, limbsPerChunk)
  local out = {}
  local carry = carryIn or 0
  for i = 1, limbsPerChunk do
    local ai = (chunkA and chunkA[i]) or 0
    local bi = (chunkB and chunkB[i]) or 0
    local s = ai + bi + carry
    if s >= BASE then s = s - BASE; carry = 1 else carry = 0 end
    out[i] = s
  end
  return out, carry
end

------------------------------------------------------------------
-- Master-side distributed Kogge-Stone driver
--
-- Given a list of (gOut, pOut) pairs returned by compute nodes (one per
-- chunk, in chunk-index order), this function runs the O(log C) prefix
-- scan to determine the carryIn bit for every chunk.
--
-- Returns: carryIns[1..count] where carryIns[1] = 0 (no carry into the
-- lowest chunk) and carryIns[i] = gOut_acc[i-1] for i > 1.
-- Also returns finalCarryOut (the carry out of the last chunk, which
-- may need to be appended as a new top limb/chunk).
--
-- The master calls this after all task_chunk_gp replies have come back,
-- then dispatches task_chunk_final to each worker with the matching
-- carryIn. This replaces the legacy O(C) sequential chunkAdd chain.
------------------------------------------------------------------
function bigint.masterPrefixCarries(gpList, count)
  local gAcc, _ = bigint.prefixScanGP(gpList, count)
  local carryIns = {}
  carryIns[1] = 0
  for i = 2, count do
    carryIns[i] = gAcc[i - 1]
  end
  local finalCarryOut = gAcc[count]
  return carryIns, finalCarryOut
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

  report(string.format("Total memory: %d bytes | budget (10%%): %d bytes",
    math.floor(totalMem), math.floor(budgetBytes)))
  report(string.format("Calibrated ~%.1f bytes/limb", bytesPerLimb))

  local n = math.floor(((budgetBytes / (bytesPerLimb * overhead)) * DIGITS_PER_LIMB) / bigint.LOG10_PHI)
  -- digitCount(F(n)) is (to an excellent approximation) exactly linear in n,
  -- so this analytic estimate is normally accurate to within a few
  -- thousandths of the true answer -- confirmed empirically: at ~100k-limb
  -- scale the very first trial routinely lands within ~0.001% of budget.
  -- The one failure mode is landing a hair OVER budget, which previously
  -- threw away that (expensive!) trial entirely and started a whole fresh
  -- one just to shave off a couple of percent. A small conservative
  -- pre-shrink makes the first trial land under budget in the common case,
  -- so it can usually be accepted immediately.
  n = math.floor(n * 0.999)
  n = math.max(n, 10)

  local best = nil
  for attempt = 1, 4 do
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
      -- Tight correction (no extra fudge beyond a hair of safety margin):
      -- the estimate is already close, so overshoot here should be small
      -- and we want this to be the LAST expensive trial, not another
      -- pessimistic guess that then needs its own correction.
      local ratio = budgetBytes / actualBytes
      local nextN = math.floor(n * ratio * 0.999)
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
  return string.format("%d B", math.floor(b))
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
  return string.format("fb%d%04x", math.floor(os.time()), math.random(0, 0xFFFF))
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