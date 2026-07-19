-- fibbenchmaster.lua
--
-- FibBench MASTER node.
--
-- Role:
--   1. BOOTSTRAP: locally (fast doubling / matrix-identity recursion)
--      compute the largest Fibonacci number that fits in half of this
--      machine's own memory, plus the one before it - "two for the
--      price of one" - using a runtime-calibrated memory estimate
--      rather than a guessed constant.
--   2. Once at least one storage node has joined, that pair of huge
--      numbers is split into fixed-size limb "chunks" and handed off
--      to the storage network, freeing the master's own RAM back down
--      to near nothing.
--   3. GROWTH: forever after, the master advances the sequence one
--      Fibonacci step at a time (A,B -> B,A+B) by handing out tiny
--      "add this chunk, assuming carry-in X" tasks to compute nodes.
--      Both carryIn=0 and carryIn=1 are dispatched for every chunk in
--      parallel (a "carry-select adder", borrowed from hardware
--      design) so the whole step parallelises across however many
--      compute nodes are connected, instead of being a strict
--      chunk-by-chunk ripple chain. The master only ever resolves a
--      handful of carry BITS itself - never full chunk data - so its
--      own memory usage stays flat no matter how large the number
--      that the network as a whole is holding gets.
--   4. Workers (compute or storage) may join or leave at any time;
--      the master re-balances automatically and requeues any task an
--      unresponsive worker was holding.
--   5. Progress is checkpointed to disk periodically (and on request/
--      quit) so the whole network can pick up where it left off after
--      a crash or restart, as long as the storage nodes referenced in
--      the checkpoint eventually come back online.
--
-- Run with: fibbenchmaster

local event = require("event")
local term  = require("term")
local computer = require("computer")

local scriptDir = (...) and (...):match("(.*/)") or ""
if scriptDir == "" then
  local ok, shell = pcall(require, "shell")
  if ok then
    local resolved = shell.resolve("fibbenchmaster.lua")
    if resolved then scriptDir = resolved:match("(.*/)") or "" end
  end
end
local common = dofile(scriptDir .. "fibbenchcommon.lua")
local ui, net, util, bigint = common.ui, common.net, common.util, common.bigint
local keys = net.keyboard.keys

local CHECKPOINT_PATH = scriptDir .. "fibbench_checkpoint.chk"
local CHECKPOINT_EVERY_STEPS = 10
local CHECKPOINT_EVERY_SECONDS = 120

------------------------------------------------------------------
-- TUI setup
------------------------------------------------------------------

local gpu, W, H = ui.init("FibBench Master")
local netBoxH = math.max(8, math.floor(H * 0.35))
ui.box(2, 2, math.floor(W/2) - 1, netBoxH, "Network")
ui.box(math.floor(W/2) + 1, 2, W - math.floor(W/2) - 2, netBoxH, "Progress")
ui.box(2, 2 + netBoxH, W - 2, H - 3 - netBoxH, "Log")
local logPanel = ui.newLog(3, 3 + netBoxH, W - 4, H - 5 - netBoxH)

local function log(msg, color) logPanel:push(msg, color) end

------------------------------------------------------------------
-- Networking + node registry
------------------------------------------------------------------

local modems = net.openModems()
local myId = net.myAddress(modems)
if #modems == 0 then
  log("WARNING: no modem found - the network cannot form.", ui.palette.bad)
end

local nodes = {}       -- [addr] = {role, name, lastSeen, busy, stats}
local computeOrder = {} -- addresses, for round robin
local storageOrder = {} -- addresses, for round robin
local nextComputeNum, nextStorageNum = 1, 1
local rrComputeIdx, rrStorageIdx = 1, 1

local function computeNodes()
  local out = {}
  for _, addr in ipairs(computeOrder) do
    if nodes[addr] then out[#out+1] = addr end
  end
  return out
end

local function storageNodes()
  local out = {}
  for _, addr in ipairs(storageOrder) do
    if nodes[addr] then out[#out+1] = addr end
  end
  return out
end

local function pickStorageNode()
  local pool = storageNodes()
  if #pool == 0 then return nil end
  rrStorageIdx = (rrStorageIdx % #pool) + 1
  return pool[rrStorageIdx]
end

local function pickIdleComputeNode()
  local pool = computeNodes()
  if #pool == 0 then return nil end
  for _ = 1, #pool do
    rrComputeIdx = (rrComputeIdx % #pool) + 1
    local addr = pool[rrComputeIdx]
    if nodes[addr] and not nodes[addr].busy then return addr end
  end
  return nil
end

local function registerNode(addr, role, extra)
  if nodes[addr] then
    nodes[addr].lastSeen = computer.uptime()
    for k, v in pairs(extra or {}) do nodes[addr][k] = v end
    return nodes[addr], false
  end
  local name
  if role == "compute" then
    name = "Compute-" .. nextComputeNum; nextComputeNum = nextComputeNum + 1
    computeOrder[#computeOrder+1] = addr
  else
    name = "Storage-" .. nextStorageNum; nextStorageNum = nextStorageNum + 1
    storageOrder[#storageOrder+1] = addr
  end
  nodes[addr] = { role = role, name = name, lastSeen = computer.uptime(), busy = false, stats = extra or {} }
  return nodes[addr], true
end

local function dropNode(addr)
  nodes[addr] = nil
end

------------------------------------------------------------------
-- Chunk-level helpers (master's own fetch/store/delete requests -
-- used for bootstrap seeding, the rare top-chunk-extend case, and
-- periodic exact-digit-count display)
------------------------------------------------------------------

local replyCounter = 0
local function nextReplyId()
  replyCounter = replyCounter + 1
  return myId .. "-m" .. replyCounter
end

local function masterStoreChunk(nodeAddr, path, data)
  local req = { type = "store_chunk", replyId = nextReplyId(), path = path, data = data, from = myId }
  local reply = net.request(modems, nodeAddr, req, "store_ack", { attempts = 5, perWait = 3 })
  return reply ~= nil and reply.ok, reply and reply.err
end

local function masterFetchChunk(nodeAddr, path)
  local req = { type = "fetch_chunk", replyId = nextReplyId(), path = path, from = myId }
  local reply = net.request(modems, nodeAddr, req, "chunk_data", { attempts = 3, perWait = 3 })
  if not reply or not reply.ok then return nil, reply and reply.err or "timeout" end
  return reply.data
end

local function masterDeleteChunk(nodeAddr, path)
  net.send(modems, nodeAddr, { type = "delete_chunk", path = path, from = myId })
end

local function digitsFromTopChunk(topChunk, limbsPerChunk, totalChunkCount)
  local top = 1
  for i = limbsPerChunk, 1, -1 do
    if (topChunk[i] or 0) ~= 0 then top = i; break end
  end
  local leadDigits = #tostring(topChunk[top] or 0)
  local digitsInTopChunk = (top - 1) * bigint.DIGITS_PER_LIMB + leadDigits
  return (totalChunkCount - 1) * limbsPerChunk * bigint.DIGITS_PER_LIMB + digitsInTopChunk
end

------------------------------------------------------------------
-- Run state
------------------------------------------------------------------

local state = {
  phase = "INIT",           -- INIT, BOOTSTRAP, WAITING_FOR_STORAGE, SEEDING_STORAGE, GROWING
  seriesId = nil,
  n = 0,                    -- Fibonacci index currently represented by B
  limbsPerChunk = common.DEFAULT_CHUNK_LIMBS,
  A = nil,                  -- {chunkCount=, manifest={[i]={node=,path=}}}
  B = nil,
  stepsCompleted = 0,
  paused = false,
  startTime = computer.uptime(),
  lastCheckpointTime = 0,
  lastDigits = nil,
  lastStepDurations = {},
}

------------------------------------------------------------------
-- Checkpointing
------------------------------------------------------------------

local function saveCheckpoint()
  if not (state.A and state.B) then return end
  local ok = common.saveTable(CHECKPOINT_PATH, {
    seriesId = state.seriesId,
    n = state.n,
    limbsPerChunk = state.limbsPerChunk,
    stepsCompleted = state.stepsCompleted,
    A = state.A,
    B = state.B,
    savedAt = os.time(),
  })
  state.lastCheckpointTime = computer.uptime()
  if ok then
    log("Checkpoint saved (step " .. state.stepsCompleted .. ", n=" .. state.n .. ").", ui.palette.dim)
  else
    log("Checkpoint save FAILED.", ui.palette.bad)
  end
end

local function loadCheckpoint()
  return common.loadTable(CHECKPOINT_PATH)
end

------------------------------------------------------------------
-- Drawing
------------------------------------------------------------------

local function drawNetwork()
  local x, y, w = 3, 3, math.floor(W/2) - 3
  ui.clearArea(x, y, w, netBoxH - 2)
  local line = y
  ui.text(x, line, string.format("My address: %s", util.shortId(myId)), ui.palette.dim); line = line + 1
  local comp, stor = computeNodes(), storageNodes()
  ui.text(x, line, string.format("Compute nodes: %d    Storage nodes: %d", #comp, #stor), ui.palette.text)
  line = line + 2
  local shown = 0
  local maxShow = netBoxH - (line - y) - 1
  for _, addr in ipairs(comp) do
    if shown >= maxShow then break end
    local n2 = nodes[addr]
    if n2 then
      local busyTxt = n2.busy and "BUSY" or "idle"
      local color = n2.busy and ui.palette.accent2 or ui.palette.good
      ui.text(x, line, string.format("  %-10s %-6s %s", n2.name, busyTxt, util.shortId(addr)), color)
      line = line + 1; shown = shown + 1
    end
  end
  for _, addr in ipairs(stor) do
    if shown >= maxShow then break end
    local n2 = nodes[addr]
    if n2 then
      ui.text(x, line, string.format("  %-10s %s", n2.name, util.shortId(addr)), ui.palette.accent)
      line = line + 1; shown = shown + 1
    end
  end
end

local function drawProgress()
  local x, y = math.floor(W/2) + 3, 3
  local w = W - x - 1
  ui.clearArea(x, y, w, netBoxH - 2)
  local line = y
  local phaseColor = ui.palette.warn
  if state.phase == "GROWING" then phaseColor = ui.palette.good end
  if state.phase == "SEEDING_STORAGE" or state.phase == "BOOTSTRAP" then phaseColor = ui.palette.accent2 end
  ui.text(x, line, "Phase: " .. state.phase .. (state.paused and " (PAUSED)" or ""), phaseColor); line = line + 1
  ui.text(x, line, "Series: " .. (state.seriesId or "-"), ui.palette.dim); line = line + 2
  if state.n > 0 then
    ui.text(x, line, "Index n = F(" .. util.commas(state.n) .. ")", ui.palette.text); line = line + 1
    if state.lastDigits then
      ui.text(x, line, "Digits ~= " .. util.commas(state.lastDigits), ui.palette.text); line = line + 1
    end
    if state.B then
      ui.text(x, line, "Chunks: " .. state.B.chunkCount .. " x " .. state.limbsPerChunk .. " limbs", ui.palette.dim); line = line + 1
    end
    ui.text(x, line, "Steps completed: " .. util.commas(state.stepsCompleted), ui.palette.text); line = line + 1
    local elapsed = computer.uptime() - state.startTime
    ui.text(x, line, "Elapsed: " .. util.formatDuration(elapsed), ui.palette.dim); line = line + 1
    if #state.lastStepDurations > 0 then
      local sum = 0
      for _, d in ipairs(state.lastStepDurations) do sum = sum + d end
      local avg = sum / #state.lastStepDurations
      ui.text(x, line, string.format("Avg step time: %.1fs", avg), ui.palette.dim); line = line + 1
    end
  end
end

local function drawFooter()
  ui.footer("q=quit  p=pause/resume  c=checkpoint now   |   free mem: " .. util.formatBytes(computer.freeMemory()))
end

drawNetwork(); drawProgress(); drawFooter()

------------------------------------------------------------------
-- Message handling (registration / heartbeats / bye) - used in
-- every phase.
------------------------------------------------------------------

local function welcomeNode(addr, name)
  net.send(modems, addr, {
    type = "welcome", to = addr, name = name,
    seriesId = state.seriesId, resume = (state.stepsCompleted > 0),
  })
end

-- forward-declared; set once we know how to requeue a task (defined
-- later, near the growth-loop task queue)
local requeueTaskForWorker

local function handleRegistryMessage(msg, remoteAddr)
  if msg.type == "hello_compute" then
    local n2, isNew = registerNode(remoteAddr, "compute", { memFree = msg.memFree, memTotal = msg.memTotal })
    welcomeNode(remoteAddr, n2.name)
    if isNew then log(n2.name .. " joined (compute, " .. util.formatBytes(msg.memFree or 0) .. " free).", ui.palette.good) end
    drawNetwork()

  elseif msg.type == "hello_storage" then
    local n2, isNew = registerNode(remoteAddr, "storage", { disks = msg.disks, inventoryCount = msg.inventoryCount })
    welcomeNode(remoteAddr, n2.name)
    if isNew then log(n2.name .. " joined (storage, " .. (msg.inventoryCount or 0) .. " existing chunk files seen).", ui.palette.good) end
    drawNetwork()

  elseif msg.type == "heartbeat" then
    if nodes[remoteAddr] then
      nodes[remoteAddr].lastSeen = computer.uptime()
      nodes[remoteAddr].stats = msg.stats or nodes[remoteAddr].stats
    else
      -- missed the original hello; re-register from the heartbeat
      local n2 = registerNode(remoteAddr, msg.role or "compute", msg.stats)
      welcomeNode(remoteAddr, n2.name)
      drawNetwork()
    end

  elseif msg.type == "bye" then
    if nodes[remoteAddr] then
      local name = nodes[remoteAddr].name
      if nodes[remoteAddr].role == "compute" and nodes[remoteAddr].busy and requeueTaskForWorker then
        requeueTaskForWorker(remoteAddr)
      end
      dropNode(remoteAddr)
      log(name .. " left the network.", ui.palette.warn)
      drawNetwork()
    end
  end
end

local function sweepDeadNodes()
  local now = computer.uptime()
  for addr, n2 in pairs(nodes) do
    if now - n2.lastSeen > common.HEARTBEAT_TIMEOUT then
      local name = n2.name
      if n2.role == "compute" and n2.busy and requeueTaskForWorker then
        requeueTaskForWorker(addr)
      end
      dropNode(addr)
      log(name .. " timed out and was dropped.", ui.palette.warn)
      drawNetwork()
    end
  end
end

------------------------------------------------------------------
-- Phase: bootstrap
------------------------------------------------------------------

local function runBootstrap()
  state.phase = "BOOTSTRAP"
  drawProgress()
  log("Starting bootstrap: searching for the largest Fibonacci number", ui.palette.accent2)
  log("that fits in half of this machine's memory (fast doubling)...", ui.palette.accent2)
  local n, a, b = common.bootstrapFindMaxFib(function(s) log(s, ui.palette.dim) end)
  state.n = n + 1 -- B represents F(n+1)
  state.seriesId = util.newSeriesId()
  log(string.format("Bootstrap complete: F(%d) and F(%d), %d digits.", n, n + 1, bigint.digitCount(b)), ui.palette.good)
  return a, b -- a = F(n), b = F(n+1)
end

------------------------------------------------------------------
-- Phase: seed storage (convert in-memory bigints to chunk manifests)
------------------------------------------------------------------

local function seedNumberToStorage(x, label)
  local chunks, count = bigint.toChunks(x, state.limbsPerChunk)
  local manifest = {}
  for i = 1, count do
    local node = pickStorageNode()
    if not node then error("no storage nodes available while seeding") end
    local filename = string.format("%s_%s_%d.chunk", state.seriesId, label, i)
    local ok, err = masterStoreChunk(node, filename, chunks[i])
    if not ok then error("failed to seed chunk " .. i .. " of " .. label .. ": " .. tostring(err)) end
    manifest[i] = { node = node, path = filename }
    if i % 5 == 0 or i == count then
      log(string.format("Seeding %s: %d/%d chunks stored...", label, i, count), ui.palette.dim)
    end
  end
  return { chunkCount = count, manifest = manifest }
end

local function runSeeding(a, b)
  state.phase = "SEEDING_STORAGE"
  drawProgress()
  log("Splitting bootstrap numbers into " .. state.limbsPerChunk .. "-limb chunks and", ui.palette.accent2)
  log("distributing them across connected storage nodes...", ui.palette.accent2)
  state.A = seedNumberToStorage(a, "A")
  state.B = seedNumberToStorage(b, "B")
  log("Seeding complete. Master's own memory is now free of the number.", ui.palette.good)
  saveCheckpoint()
end

------------------------------------------------------------------
-- Growth loop (task-queue state machine, ticked from the main loop)
------------------------------------------------------------------

local growth = nil -- current in-progress step, or nil if none active

local function beginStep()
  local chunkCount = math.max(state.A.chunkCount, state.B.chunkCount)
  local pending = {}
  for i = 1, chunkCount do
    local aRef = state.A.manifest[i]
    local bRef = state.B.manifest[i]
    for carryIn = 0, 1 do
      local storageNode = pickStorageNode()
      local filename = string.format("%s_R%d_s%d_c%d.chunk", state.seriesId, carryIn, state.stepsCompleted + 1, i)
      pending[#pending+1] = {
        taskId = myId .. "-t" .. state.stepsCompleted .. "-" .. i .. "-" .. carryIn,
        chunkIndex = i, carryIn = carryIn,
        aRef = aRef, bRef = bRef,
        limbsPerChunk = state.limbsPerChunk,
        resultRef = { node = storageNode, path = filename },
        retries = 0,
      }
    end
  end
  growth = {
    chunkCount = chunkCount,
    pending = pending,      -- queue of tasks not yet dispatched
    inFlight = {},          -- taskId -> {task=, worker=, sentAt=}
    results = {},           -- [chunkIndex] = {c0={carryOut=,resultRef=}, c1={...}}
    resolvedCount = 0,
    total = #pending,
    startedAt = computer.uptime(),
  }
end

requeueTaskForWorker = function(workerAddr)
  if not growth then return end
  for taskId, entry in pairs(growth.inFlight) do
    if entry.worker == workerAddr then
      entry.task.retries = entry.task.retries + 1
      growth.pending[#growth.pending+1] = entry.task
      growth.inFlight[taskId] = nil
      log(string.format("Requeued chunk %d (carryIn=%d) after worker loss (retry #%d).",
        entry.task.chunkIndex, entry.task.carryIn, entry.task.retries), ui.palette.warn)
    end
  end
end

local function dispatchPending()
  if not growth then return end
  while #growth.pending > 0 do
    local worker = pickIdleComputeNode()
    if not worker then break end
    local task = table.remove(growth.pending, 1)
    nodes[worker].busy = true
    growth.inFlight[task.taskId] = { task = task, worker = worker, sentAt = computer.uptime() }
    net.send(modems, worker, {
      type = "task_chunk_add", taskId = task.taskId,
      chunkIndex = task.chunkIndex, carryIn = task.carryIn,
      aRef = task.aRef, bRef = task.bRef, limbsPerChunk = task.limbsPerChunk,
      resultRef = task.resultRef,
    })
  end
end

local function sweepTaskTimeouts()
  if not growth then return end
  local now = computer.uptime()
  for taskId, entry in pairs(growth.inFlight) do
    if now - entry.sentAt > common.TASK_TIMEOUT then
      if nodes[entry.worker] then nodes[entry.worker].busy = false end
      entry.task.retries = entry.task.retries + 1
      growth.pending[#growth.pending+1] = entry.task
      growth.inFlight[taskId] = nil
      log(string.format("Chunk %d (carryIn=%d) timed out, retrying (#%d).",
        entry.task.chunkIndex, entry.task.carryIn, entry.task.retries), ui.palette.warn)
    end
  end
end

local function handleTaskDone(msg, remoteAddr)
  -- Whoever just replied is done working, full stop - free them up
  -- immediately regardless of whether our task bookkeeping below still
  -- considers this particular attempt current. Without this, a worker
  -- whose task was reassigned after a timeout (but who was only slow,
  -- not actually gone) could stay marked "busy" forever once its late
  -- reply arrives after the reassigned attempt already resolved things.
  if nodes[remoteAddr] then nodes[remoteAddr].busy = false end

  if not growth then return end
  local entry = growth.inFlight[msg.taskId]
  if not entry then return end -- stale/duplicate reply for an already-resolved attempt
  growth.inFlight[msg.taskId] = nil

  if not msg.ok then
    entry.task.retries = entry.task.retries + 1
    growth.pending[#growth.pending+1] = entry.task
    if entry.task.retries % 4 == 0 then
      log(string.format("Chunk %d (carryIn=%d) still failing after %d retries: %s",
        entry.task.chunkIndex, entry.task.carryIn, entry.task.retries, tostring(msg.err)), ui.palette.bad)
    end
    return
  end

  local rec = growth.results[msg.chunkIndex] or {}
  if msg.carryIn == 0 then rec.c0 = { carryOut = msg.carryOut, resultRef = entry.task.resultRef } end
  if msg.carryIn == 1 then rec.c1 = { carryOut = msg.carryOut, resultRef = entry.task.resultRef } end
  growth.results[msg.chunkIndex] = rec
  growth.resolvedCount = growth.resolvedCount + 1
end

-- Sequentially resolves the real carry chain using only the tiny
-- carry BITS already collected (never touches chunk data), deletes
-- the losing tentative chunk of each pair, and returns the new B.
local function finalizeStep()
  local newManifest = {}
  local carry = 0
  for i = 1, growth.chunkCount do
    local rec = growth.results[i]
    local chosen, loser
    if carry == 0 then chosen, loser = rec.c0, rec.c1 else chosen, loser = rec.c1, rec.c0 end
    newManifest[i] = chosen.resultRef
    if loser then masterDeleteChunk(loser.resultRef.node, loser.resultRef.path) end
    carry = chosen.carryOut
  end
  local newChunkCount = growth.chunkCount
  if carry == 1 then
    newChunkCount = newChunkCount + 1
    local extra = bigint.zeroChunk(state.limbsPerChunk)
    extra[1] = 1
    local node = pickStorageNode()
    local filename = string.format("%s_Rtop_s%d.chunk", state.seriesId, state.stepsCompleted + 1)
    local ok = masterStoreChunk(node, filename, extra)
    if ok then
      newManifest[newChunkCount] = { node = node, path = filename }
    else
      log("Failed to store carry-extension chunk - number may be truncated!", ui.palette.bad)
    end
  end

  -- old A is now fully superseded; free its chunks from storage
  for i = 1, state.A.chunkCount do
    local ref = state.A.manifest[i]
    if ref then masterDeleteChunk(ref.node, ref.path) end
  end

  state.A = state.B
  state.B = { chunkCount = newChunkCount, manifest = newManifest }
  state.n = state.n + 1
  state.stepsCompleted = state.stepsCompleted + 1

  local dur = computer.uptime() - growth.startedAt
  table.insert(state.lastStepDurations, dur)
  while #state.lastStepDurations > 10 do table.remove(state.lastStepDurations, 1) end

  -- Cheap exact digit count: only the (small) top chunk needs fetching.
  local topRef = state.B.manifest[state.B.chunkCount]
  if topRef then
    local topChunk, err = masterFetchChunk(topRef.node, topRef.path)
    if topChunk then
      state.lastDigits = digitsFromTopChunk(topChunk, state.limbsPerChunk, state.B.chunkCount)
    end
  end

  growth = nil
end

------------------------------------------------------------------
-- Startup: check for an existing checkpoint
------------------------------------------------------------------

local function promptResumeOrFresh(cp)
  local ago = os.time() - (cp.savedAt or os.time())
  ui.clearArea(3, 3 + netBoxH, W - 4, 3)
  log(string.format("Found checkpoint: series %s, step %d (n=%d), saved %ds ago.",
    tostring(cp.seriesId), cp.stepsCompleted or 0, cp.n or 0, ago), ui.palette.accent2)
  ui.footer("Resume from checkpoint? [r]esume / [f]resh start")
  while true do
    local ev, _, _char, code = event.pull("key_down")
    if ev then
      if code == keys.r then return true end
      if code == keys.f then return false end
    end
  end
end

local cp = loadCheckpoint()
local aBig, bBig
if cp and cp.A and cp.B then
  if promptResumeOrFresh(cp) then
    state.seriesId = cp.seriesId
    state.n = cp.n
    state.limbsPerChunk = cp.limbsPerChunk
    state.stepsCompleted = cp.stepsCompleted
    state.A = cp.A
    state.B = cp.B
    state.phase = "GROWING"
    log("Resumed series " .. tostring(state.seriesId) .. " at step " .. state.stepsCompleted .. ".", ui.palette.good)
    log("If any storage nodes referenced by the checkpoint are offline,", ui.palette.dim)
    log("growth will automatically wait for them to reconnect.", ui.palette.dim)
  else
    aBig, bBig = runBootstrap()
    state.phase = "WAITING_FOR_STORAGE"
  end
else
  aBig, bBig = runBootstrap()
  state.phase = "WAITING_FOR_STORAGE"
end

drawNetwork(); drawProgress(); drawFooter()

------------------------------------------------------------------
-- Main loop
------------------------------------------------------------------

local running = true
local lastCheckpointStepCount = state.stepsCompleted

while running do
  local msg, remoteAddr = net.pull(1)
  if msg then
    local ok, err = pcall(function()
      if msg.type == "hello_compute" or msg.type == "hello_storage" or msg.type == "heartbeat" or msg.type == "bye" then
        handleRegistryMessage(msg, remoteAddr)
      elseif msg.type == "task_done" then
        handleTaskDone(msg, remoteAddr)
      end
    end)
    if not ok then log("message handler error: " .. tostring(err), ui.palette.bad) end
  end

  sweepDeadNodes()

  if state.phase == "WAITING_FOR_STORAGE" then
    if #storageNodes() > 0 then
      local ok, err = pcall(runSeeding, aBig, bBig)
      aBig, bBig = nil, nil
      collectgarbage()
      if ok then
        state.phase = "GROWING"
      else
        log("Seeding failed: " .. tostring(err), ui.palette.bad)
        state.phase = "WAITING_FOR_STORAGE"
      end
    end

  elseif state.phase == "GROWING" and not state.paused then
    if not growth then
      if #storageNodes() > 0 then
        beginStep()
      end
    end
    if growth then
      dispatchPending()
      sweepTaskTimeouts()
      if growth.resolvedCount >= growth.total then
        local ok, err = pcall(finalizeStep)
        if not ok then
          log("finalizeStep error: " .. tostring(err), ui.palette.bad)
          growth = nil
        end
      end
    end

    local now = computer.uptime()
    if (state.stepsCompleted - lastCheckpointStepCount >= CHECKPOINT_EVERY_STEPS)
       or (now - state.lastCheckpointTime >= CHECKPOINT_EVERY_SECONDS and state.stepsCompleted > lastCheckpointStepCount) then
      saveCheckpoint()
      lastCheckpointStepCount = state.stepsCompleted
    end
  end

  drawNetwork()
  drawProgress()
  drawFooter()

  local ev, _, _char, code = event.pull(0, "key_down")
  if ev then
    if code == keys.q then
      running = false
    elseif code == keys.p then
      state.paused = not state.paused
      log(state.paused and "Paused." or "Resumed.", ui.palette.warn)
    elseif code == keys.c then
      saveCheckpoint()
      lastCheckpointStepCount = state.stepsCompleted
    end
  end
end

if state.A and state.B then saveCheckpoint() end
net.broadcast(modems, { type = "master_shutdown" })
term.clear()
print("FibBench master stopped." .. (state.n > 0 and (" Last computed index: F(" .. state.n .. ")") or ""))