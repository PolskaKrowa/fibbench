-- fibbenchcompute.lua
--
-- FibBench COMPUTE node.
--
-- Role: registers with a master, then executes small "add this chunk"
-- tasks on demand. It never holds more than one chunk's worth of limbs
-- in memory at a time - it fetches the two input chunks from whichever
-- storage nodes hold them, adds them with the carry assumption the
-- master gave it, writes the result chunk back out to a storage node,
-- and reports the resulting carry bit. This keeps its own memory
-- footprint flat regardless of how large the overall Fibonacci number
-- has grown.
--
-- Run with: fibbenchcompute

local event = require("event")
local term  = require("term")

local scriptDir = (...) and (...):match("(.*/)") or ""
if scriptDir == "" then
  local ok, shell = pcall(require, "shell")
  if ok then
    local resolved = shell.resolve("fibbenchcompute.lua")
    if resolved then scriptDir = resolved:match("(.*/)") or "" end
  end
end
local common = dofile(scriptDir .. "fibbenchcommon.lua")
local ui, net, util, bigint = common.ui, common.net, common.util, common.bigint
local keys = net.keyboard.keys
local computer = require("computer")

------------------------------------------------------------------
-- Setup
------------------------------------------------------------------

local gpu, W, H = ui.init("FibBench Compute Node")
ui.box(2, 2, W - 2, 5, "Connection")
ui.box(2, 8, W - 2, 6, "Current Task")
ui.box(2, 15, W - 2, H - 16, "Log")
local logPanel = ui.newLog(3, 16, W - 4, H - 18)

local modems = net.openModems()
local myId = net.myAddress(modems)
if #modems == 0 then
  logPanel:push("WARNING: no modem found - cannot join a network.", ui.palette.bad)
end

local state = {
  connected = false,
  masterAddr = nil,
  masterName = nil,
  seriesId = nil,
  lastMasterSeen = 0,
  tasksDone = 0,
  tasksFailed = 0,
  busy = false,
  currentTask = nil,
}

local replyCounter = 0
local function nextReplyId()
  replyCounter = replyCounter + 1
  return myId .. "-r" .. replyCounter
end

local function drawConnection()
  ui.clearArea(4, 3, W - 6, 3)
  if state.connected then
    ui.text(4, 3, "Status:  CONNECTED", ui.palette.good)
    ui.text(4, 4, "Master:  " .. util.shortId(state.masterAddr) .. "  (" .. (state.masterName or "?") .. ")", ui.palette.text)
    ui.text(4, 5, "Series:  " .. (state.seriesId or "-") .. "   Tasks OK: " .. state.tasksDone ..
      "   Failed: " .. state.tasksFailed, ui.palette.dim)
  else
    ui.text(4, 3, "Status:  SEARCHING FOR MASTER...", ui.palette.warn)
    ui.text(4, 4, "My address: " .. util.shortId(myId), ui.palette.dim)
  end
end

local function drawTask()
  ui.clearArea(4, 9, W - 6, 4)
  if state.busy and state.currentTask then
    local t = state.currentTask
    ui.text(4, 9, string.format("Chunk #%d   carryIn=%d   limbsPerChunk=%d", t.chunkIndex, t.carryIn, t.limbsPerChunk), ui.palette.accent2)
    ui.text(4, 10, "Stage: " .. (t.stage or "?"), ui.palette.text)
  else
    ui.text(4, 9, "Idle - waiting for work...", ui.palette.dim)
  end
end

local function drawFooter()
  ui.footer(string.format("free mem: %s   |   q = quit and deregister",
    util.formatBytes(computer.freeMemory())))
end

drawConnection()
drawTask()
drawFooter()
logPanel:push("Compute node ready. Address: " .. util.shortId(myId), ui.palette.accent)

------------------------------------------------------------------
-- Task execution
--
-- Three task types are supported:
--
--   task_chunk_add    (legacy)   sequential carry chain: fetch A, B;
--                               chunkAdd(A,B,carryIn) -> result + carryOut;
--                               store result; report carryOut. The master
--                               chains these one chunk at a time.
--
--   task_chunk_gp     (Kogge-Stone phase 1)   fetch A, B; compute per-limb
--                               (g,p) arrays via bigint.chunkGenProp; reduce
--                               them to a single per-chunk (gOut, pOut) via
--                               bigint.chunkReduceGP; return (gOut, pOut)
--                               to the master (no storage write needed).
--                               The master runs an O(log N) prefix scan
--                               over all chunks' (gOut, pOut) pairs to
--                               determine every chunk's carryIn.
--
--   task_chunk_final  (Kogge-Stone phase 3)   fetch A, B again (or use a
--                               cached copy); chunkFinalAdd(A,B,carryIn)
--                               with the now-known carryIn -> result +
--                               carryOut; store result; report carryOut.
--
-- The phase-1 + phase-3 pair replaces N sequential task_chunk_add rounds
-- (each waiting for the previous chunk's carryOut) with 1 parallel phase-1
-- round + 1 parallel phase-3 round, plus an O(log N) in-master prefix
-- scan with no network. At FibBench scales (chunks of ~500 limbs across
-- many nodes), this can cut the wall-clock cost of a distributed add by
-- 10x+ compared to the legacy chain.
------------------------------------------------------------------

-- Fetch a chunk of limbs from a storage node, or return a zero chunk
-- if ref is nil (meaning "this operand doesn't have a chunk here").
local function fetchChunk(ref, limbsPerChunk)
  if ref == nil then
    return bigint.zeroChunk(limbsPerChunk), true
  end
  local req = { type = "fetch_chunk", replyId = nextReplyId(), path = ref.path, from = myId }
  local reply = net.request(modems, ref.node, req, "chunk_data", { attempts = 4, perWait = 3 })
  if not reply or not reply.ok then
    return nil, false, reply and reply.err or "no reply from storage node"
  end
  return reply.data, true
end

local function storeChunk(ref, data)
  local req = { type = "store_chunk", replyId = nextReplyId(), path = ref.path, data = data, from = myId }
  local reply = net.request(modems, ref.node, req, "store_ack", { attempts = 4, perWait = 3 })
  if not reply or not reply.ok then
    return false, reply and reply.err or "no reply from storage node"
  end
  return true
end

-- Legacy task: sequential carry-chain add within a single chunk.
local function executeTask(task)
  state.busy = true
  state.currentTask = task
  task.stage = "fetching operand A"
  drawTask()
  local chunkA, okA, errA = fetchChunk(task.aRef, task.limbsPerChunk)
  if not okA then
    return false, "fetch A failed: " .. tostring(errA)
  end

  task.stage = "fetching operand B"
  drawTask()
  local chunkB, okB, errB = fetchChunk(task.bRef, task.limbsPerChunk)
  if not okB then
    return false, "fetch B failed: " .. tostring(errB)
  end

  task.stage = "adding"
  drawTask()
  local resultChunk, carryOut = bigint.chunkAdd(chunkA, chunkB, task.carryIn, task.limbsPerChunk)

  task.stage = "storing result"
  drawTask()
  local okStore, errStore = storeChunk(task.resultRef, resultChunk)
  if not okStore then
    return false, "store failed: " .. tostring(errStore)
  end

  return true, nil, carryOut
end

-- Phase 1 of distributed Kogge-Stone: compute a chunk's (gOut, pOut) pair.
-- Returns gOut, pOut, and the per-limb arrays (so the master can do its
-- own prefix scan or store them for debugging). The per-limb arrays are
-- optional to return over the wire - if the master only wants the
-- per-chunk scalars, set task.returnArrays = false.
local function executeGPTask(task)
  state.busy = true
  state.currentTask = task

  task.stage = "gp: fetching operand A"
  drawTask()
  local chunkA, okA, errA = fetchChunk(task.aRef, task.limbsPerChunk)
  if not okA then
    return false, "fetch A failed: " .. tostring(errA)
  end

  task.stage = "gp: fetching operand B"
  drawTask()
  local chunkB, okB, errB = fetchChunk(task.bRef, task.limbsPerChunk)
  if not okB then
    return false, "fetch B failed: " .. tostring(errB)
  end

  task.stage = "gp: computing (g, p)"
  drawTask()
  local gArray, pArray = bigint.chunkGenProp(chunkA, chunkB, task.limbsPerChunk)
  local gOut, pOut = bigint.chunkReduceGP(gArray, pArray, task.limbsPerChunk)

  -- Per-chunk scalars go back to the master; the per-limb arrays are
  -- optional (default off, since they're O(L) per chunk and the master
  -- doesn't need them for the cross-chunk prefix scan).
  local payload
  if task.returnArrays then
    payload = { gOut = gOut, pOut = pOut, gArray = gArray, pArray = pArray }
  else
    payload = { gOut = gOut, pOut = pOut }
  end
  -- Return via a side channel so handleGPTask can ship it back to the master.
  task._gpResult = payload
  return true, nil, nil
end

-- Phase 3 of distributed Kogge-Stone: final chunk sum with a known carryIn.
-- Identical to executeTask but uses bigint.chunkFinalAdd (which is just
-- a sequential carry chain too, but the API is named for clarity and
-- makes the pipeline structure explicit). Returns (ok, err, carryOut).
local function executeFinalTask(task)
  state.busy = true
  state.currentTask = task

  task.stage = "final: fetching operand A"
  drawTask()
  local chunkA, okA, errA = fetchChunk(task.aRef, task.limbsPerChunk)
  if not okA then
    return false, "fetch A failed: " .. tostring(errA)
  end

  task.stage = "final: fetching operand B"
  drawTask()
  local chunkB, okB, errB = fetchChunk(task.bRef, task.limbsPerChunk)
  if not okB then
    return false, "fetch B failed: " .. tostring(errB)
  end

  task.stage = "final: summing with known carryIn"
  drawTask()
  local resultChunk, carryOut = bigint.chunkFinalAdd(
    chunkA, chunkB, task.carryIn, task.limbsPerChunk)

  task.stage = "final: storing result"
  drawTask()
  local okStore, errStore = storeChunk(task.resultRef, resultChunk)
  if not okStore then
    return false, "store failed: " .. tostring(errStore)
  end

  return true, nil, carryOut
end

------------------------------------------------------------------
-- Message handling
------------------------------------------------------------------

local function sendHello()
  net.broadcast(modems, {
    type = "hello_compute", role = "compute", id = myId,
    memFree = computer.freeMemory(), memTotal = computer.totalMemory(),
  })
end

local function handleTask(msg, remoteAddr)
  if state.busy then
    -- Already working (shouldn't normally happen; master tracks busy
    -- state) - politely refuse so the master reassigns promptly.
    net.send(modems, remoteAddr, {
      type = "task_done", taskId = msg.taskId, chunkIndex = msg.chunkIndex,
      carryIn = msg.carryIn, ok = false, err = "worker busy",
    })
    return
  end

  -- Common task fields. Each executor returns (ok, err, carryOut);
  -- executeGPTask additionally stuffs its (gOut, pOut) payload into
  -- task._gpResult for the dispatcher to ship back to the master.
  local task = {
    chunkIndex    = msg.chunkIndex,
    carryIn       = msg.carryIn,
    limbsPerChunk = msg.limbsPerChunk,
    aRef          = msg.aRef,
    bRef          = msg.bRef,
    resultRef     = msg.resultRef,
    returnArrays  = msg.returnArrays,   -- only used by task_chunk_gp
  }

  local ok, err, carryOut
  if msg.type == "task_chunk_gp" then
    ok, err, carryOut = executeGPTask(task)
  elseif msg.type == "task_chunk_final" then
    ok, err, carryOut = executeFinalTask(task)
  else  -- "task_chunk_add" (legacy)
    ok, err, carryOut = executeTask(task)
  end

  state.busy = false
  state.currentTask = nil
  if ok then
    state.tasksDone = state.tasksDone + 1
    if msg.type == "task_chunk_gp" then
      logPanel:push(string.format("gp chunk %d -> gOut=%d pOut=%d",
        msg.chunkIndex, task._gpResult.gOut, task._gpResult.pOut), ui.palette.text)
    else
      logPanel:push(string.format("chunk %d (carryIn=%d) done -> carryOut=%d",
        msg.chunkIndex, msg.carryIn or 0, carryOut or 0), ui.palette.text)
    end
  else
    state.tasksFailed = state.tasksFailed + 1
    logPanel:push(string.format("%s chunk %d FAILED: %s",
      msg.type, msg.chunkIndex, tostring(err)), ui.palette.bad)
  end
  drawConnection()
  drawTask()

  -- Reply shape:
  --   task_chunk_add / task_chunk_final -> task_done with carryOut
  --   task_chunk_gp                    -> task_gp_done with {gOut, pOut, ...}
  if msg.type == "task_chunk_gp" then
    net.send(modems, state.masterAddr or remoteAddr, {
      type        = "task_gp_done",
      taskId      = msg.taskId,
      chunkIndex  = msg.chunkIndex,
      ok          = ok,
      err         = err,
      gOut        = ok and task._gpResult.gOut  or nil,
      pOut        = ok and task._gpResult.pOut  or nil,
      gArray      = ok and task._gpResult.gArray or nil,
      pArray      = ok and task._gpResult.pArray or nil,
      resultRef   = msg.resultRef,
    })
  else
    net.send(modems, state.masterAddr or remoteAddr, {
      type      = "task_done",
      taskId    = msg.taskId,
      chunkIndex= msg.chunkIndex,
      carryIn   = msg.carryIn,
      carryOut  = carryOut,
      ok        = ok,
      err       = err,
      resultRef = msg.resultRef,
    })
  end
end

local function handleMessage(msg, remoteAddr)
  -- Any traffic at all from our current master is proof it's still there,
  -- not just the two message types that happened to update this before.
  -- This is what actually fixes the false "lost contact" loop: previously
  -- an idle worker's clock only got refreshed by "welcome" (once) or
  -- "task_chunk_add" (only while there's work), so it would time itself
  -- out and re-register - always landing on the same name, since the
  -- master never actually forgot it either.
  if state.connected and remoteAddr == state.masterAddr then
    state.lastMasterSeen = computer.uptime()
  end

  if msg.type == "welcome" and msg.to == myId then
    state.connected = true
    state.masterAddr = remoteAddr
    state.masterName = msg.name
    state.seriesId = msg.seriesId
    state.lastMasterSeen = computer.uptime()
    logPanel:push("Registered with master " .. util.shortId(remoteAddr) .. " as " .. tostring(msg.name), ui.palette.good)
    drawConnection()

  elseif msg.type == "master_shutdown" then
    if state.connected and remoteAddr == state.masterAddr then
      logPanel:push("Master shut down. Returning to search mode.", ui.palette.warn)
      state.connected = false
      state.masterAddr = nil
      drawConnection()
    end

  elseif msg.type == "task_chunk_add"
      or msg.type == "task_chunk_gp"
      or msg.type == "task_chunk_final" then
    if not state.connected or remoteAddr ~= state.masterAddr then
      return -- ignore tasks from anyone but our current master
    end
    state.lastMasterSeen = computer.uptime()
    handleTask(msg, remoteAddr)
  end
end

------------------------------------------------------------------
-- Main loop
------------------------------------------------------------------

sendHello()
local lastHelloOrHeartbeat = computer.uptime()

local running = true
while running do
  local msg, remoteAddr = net.pull(2)
  if msg then
    local ok, err = pcall(handleMessage, msg, remoteAddr)
    if not ok then
      logPanel:push("handler error: " .. tostring(err), ui.palette.bad)
    end
  end

  local now = computer.uptime()
  if not state.connected and (now - lastHelloOrHeartbeat) >= 2 then
    sendHello()
    lastHelloOrHeartbeat = now
  elseif state.connected then
    if not state.busy and (now - lastHelloOrHeartbeat) >= common.HEARTBEAT_INTERVAL then
      net.send(modems, state.masterAddr, {
        type = "heartbeat", id = myId, role = "compute",
        stats = { tasksDone = state.tasksDone, tasksFailed = state.tasksFailed, freeMem = computer.freeMemory() },
      })
      lastHelloOrHeartbeat = now
    end
    if state.lastMasterSeen > 0 and (now - state.lastMasterSeen) > common.HEARTBEAT_TIMEOUT then
      logPanel:push("Lost contact with master. Returning to search mode.", ui.palette.warn)
      state.connected = false
      state.masterAddr = nil
      drawConnection()
    end
  end

  drawFooter()

  local ev, _, _char, code = event.pull(0, "key_down")
  if ev and code == keys.q then
    running = false
  end
end

if state.connected then
  net.send(modems, state.masterAddr, { type = "bye", id = myId, role = "compute" })
end
term.clear()
print("FibBench compute node stopped.")