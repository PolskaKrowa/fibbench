-- =============================================================
--  fibbenchcompute.lua  -  Fibonacci Compute Worker
--
--  Standalone worker for the Fibonacci master.
--  Handles PING, STAT, ADDJOB, and CHUNK broadcasts.
-- =============================================================

local component = require("component")
local computer  = require("computer")
local event     = require("event")

if not component.isAvailable("modem") then
  error("No modem found - install a network card!")
end

local modem = component.modem
local PORT  = 5757
modem.open(PORT)

local LIMB_BASE = 10000000
local LIMB_WIDTH = 7
local CHUNK_LIMBS = 64

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

local lastFree = computer.freeMemory()
local lastTotal = computer.totalMemory()

while true do
  local ev, _, sender, port, _, cmd, a1, a2, a3, a4, a5, a6 =
    event.pull("modem_message")
  if ev and port == PORT then
    if cmd == "PING" then
      modem.send(sender, PORT, "PONG", lastFree, lastTotal, 0, "compute")
    elseif cmd == "STAT" then
      modem.send(sender, PORT, "STAT", lastFree, lastTotal, 0, "compute")
    elseif cmd == "ADDJOB" then
      local jobId  = tostring(a1 or "")
      local part   = tonumber(a2) or 0
      local total  = tonumber(a3) or 0
      local chunkA = decodeChunk(a4)
      local chunkB = decodeChunk(a5)
      local sum0, carry0, sum1, carry1 = addChunkVariants(chunkA, chunkB)
      modem.send(sender, PORT, "ADDRES",
        jobId, tostring(part), encodeChunk(sum0), tostring(carry0),
        encodeChunk(sum1), tostring(carry1))
    elseif cmd == "CHUNK" then
      -- Broadcast only. No persistence required on workers.
    end
  end
end
