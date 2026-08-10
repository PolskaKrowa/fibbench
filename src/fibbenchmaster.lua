-- fibbenchmaster.lua
--
-- FibBench MASTER node.
--
-- Role:
--   1. BOOTSTRAP: locally (fast doubling / matrix-identity recursion)
--      compute the largest Fibonacci number that fits in half of this
--      machine's own memory, plus the one before it - "two for the
--      price of one" - using a runtime-calibrated memory estimate
--      rather than a guessed constant. Multiplication now dispatches
--      schoolbook -> Karatsuba -> Toom-Cook 3 (see fibbenchcommon.lua);
--      the bootstrap path benefits directly.
--   2. Once at least one storage node has joined, that pair of huge
--      numbers is split into fixed-size limb "chunks" and handed off
--      to the storage network, freeing the master's own RAM back down
--      to near nothing.
--   3. GROWTH: forever after, the master advances the sequence one
--      Fibonacci step at a time (A,B -> B,A+B) by handing out tiny
--      "add this chunk, assuming carry-in X" tasks to compute nodes.
--      Two algorithms are available, selected at runtime:
--
--        ADD_ALGORITHM = "carry_select"  (legacy, default)
--            Both carryIn=0 and carryIn=1 are dispatched for every chunk
--            in parallel (a "carry-select adder", borrowed from hardware
--            design) so the whole step parallelises across however many
--            compute nodes are connected, instead of being a strict
--            chunk-by-chunk ripple chain. The master only ever resolves
--            a handful of carry BITS itself - never full chunk data - so
--            its own memory usage stays flat no matter how large the
--            number that the network as a whole is holding gets.
--
--        ADD_ALGORITHM = "kogge_stone"   (new)
--            Each step runs as three phases:
--              Phase 1 (parallel across workers):
--                each worker fetches A,B for its chunk, computes per-limb
--                (g, p) arrays via bigint.chunkGenProp, reduces them to
--                a single per-chunk (gOut, pOut) bit pair via
--                bigint.chunkReduceGP, and returns just those 2 bits.
--              Phase 2 (master, no network, O(log N)):
--                bigint.masterPrefixCarries() runs a Kogge-Stone prefix
--                scan over the collected (gOut, pOut) pairs to compute
--                every chunk's carryIn bit.
--              Phase 3 (parallel across workers):
--                each worker fetches A,B again, computes the final sum
--                via bigint.chunkFinalAdd with its now-known carryIn,
--                stores the result, and returns carryOut.
--            Total network waves per step: 2 (vs ~1 for carry-select,
--            but carry-select sends 2x the chunks per wave). The master
--            never touches chunk data in either algorithm.
--
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
-- Configuration
--
-- Selects which distributed addition algorithm the GROWTH phase uses.
--   "carry_select"           - 2x chunk parallelism per wave, 1 wave
--   "kogge_stone" (default)  - 1x chunk parallelism per wave, 2 waves,
--                              but the cross-chunk carry is resolved
--                              in O(log N) on the master instead of
--                              chunk-by-chunk at the network.
-- Override at startup by setting ADD_ALGORITHM in the environment or
-- by editing this line. Workers support both protocols transparently
-- (task_chunk_add / task_chunk_gp / task_chunk_final - see
-- fibbenchcompute.lua), so the master can switch algorithms between
-- runs without redeploying compute nodes.
------------------------------------------------------------------
local ADD_ALGORITHM = os.getenv and os.getenv("ADD_ALGORITHM") or "kogge_stone"
if ADD_ALGORITHM ~= "carry_select" then ADD_ALGORITHM = "kogge_stone" end

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

-- Forward declaration: the growth-loop task state machine is built later
-- (around line ~510, after the chunk-helper and node-registry code),
-- but drawProgress() wants to peek at it to show the current algorithm
-- sub-phase. We forward-declare the local here so the lexical closure
-- in drawProgress binds to THIS local, not a global of the same name.
local growth

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
  -- When in GROWING with an active step, show the algorithm + sub-phase.
  local phaseLabel = state.phase
  if state.phase == "GROWING" and growth then
    local sub
    if growth.algorithm == "kogge_stone" then
      sub = " (KS: " .. growth.subPhase .. " " .. growth.resolvedCount .. "/" .. growth.total .. ")"
    else
      sub = " (CS: " .. growth.resolvedCount .. "/" .. growth.total .. ")"
    end
    phaseLabel = phaseLabel .. sub
  elseif state.phase == "GROWING" then
    phaseLabel = phaseLabel .. " [" .. ADD_ALGORITHM .. "]"
  end
  ui.text(x, line, "Phase: " .. phaseLabel .. (state.paused and " (PAUSED)" or ""), phaseColor); line = line + 1
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
log(string.format("Master up. Add algorithm: %s.", ADD_ALGORITHM), ui.palette.dim)
log("To switch, set ADD_ALGORITHM=kogge_stone in env and restart.", ui.palette.dim)
log("Workers support both protocols transparently.", ui.palette.dim)

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
      -- Ack it. Without this, a worker with nothing to do for
      -- HEARTBEAT_TIMEOUT seconds has no way to know the master heard
      -- its heartbeats, and will conclude it lost the master even
      -- though it never actually did - see fibbenchcompute.lua's
      -- handleMessage for the other half of this fix.
      net.send(modems, remoteAddr, { type = "heartbeat_ack", to = remoteAddr })
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
  log("that fits in 10% of this machine's memory (fast doubling)...", ui.palette.accent2)
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
--
-- Two algorithms are supported:
--
--   "carry_select" (legacy):  per chunk, dispatches task_chunk_add
--     twice (carryIn=0 and carryIn=1) in parallel; the master then
--     walks the chain in finalizeStep and picks the right carryIn
--     variant per chunk based on the real carry bit. 1 wave, 2x
--     chunk parallelism.
--
--   "kogge_stone" (new):  three sub-phases.
--     subPhase="gp"     - dispatches task_chunk_gp to each chunk's
--                         worker; worker computes (gOut, pOut) and
--                         returns just those 2 bits. No storage write.
--     subPhase="prefix" - master runs bigint.masterPrefixCarries over
--                         the collected (gOut, pOut) pairs to get the
--                         per-chunk carryIn bits. No network.
--     subPhase="final"  - dispatches task_chunk_final to each chunk's
--                         worker with the now-known carryIn; worker
--                         computes the final sum and stores the result.
--     2 waves (gp + final), 1x chunk parallelism per wave, but the
--     cross-chunk carry is resolved in O(log N) on the master
--     instead of being precomputed by sending 2x the work.
--
-- The two algorithms share the same growth{} table, inFlight{} map,
-- requeueTaskForWorker / sweepTaskTimeouts / handleTaskDone paths.
-- The only differences are in beginStep (what task to enqueue),
-- dispatchPending (what message type to send), and finalizeStep
-- (how to assemble the final manifest from the results).
------------------------------------------------------------------

-- The growth state machine (forward-declared near the top so drawProgress
-- can peek at it). Set to a fresh table by beginStep(); cleared by
-- finalizeStep() or on unrecoverable error.
growth = nil

-- Build the task list for one chunk index. Returns 1 task (KS mode) or
-- 2 tasks (carry-select mode: one for each carryIn value).
local function buildTasksForChunk(i, aRef, bRef, stepNum)
  local tasks = {}
  if ADD_ALGORITHM == "kogge_stone" then
    -- For the gp sub-phase we use a placeholder resultRef (the worker
    -- doesn't store anything during gp); the real resultRef gets
    -- created in the final sub-phase. We use the SAME aRef/bRef for
    -- both sub-phases, but distinct taskIds so they don't collide.
    local gpFilename = string.format("%s_gp_s%d_c%d.chunk", state.seriesId, stepNum, i)
    local finalFilename = string.format("%s_R_s%d_c%d.chunk", state.seriesId, stepNum, i)
    tasks[#tasks+1] = {
      taskId    = myId .. "-gp-" .. stepNum .. "-" .. i,
      kind      = "gp",
      chunkIndex= i, carryIn = 0,   -- gp doesn't use carryIn; 0 is a placeholder
      aRef      = aRef, bRef = bRef,
      limbsPerChunk = state.limbsPerChunk,
      resultRef = { node = pickStorageNode(), path = gpFilename },
      -- For the final sub-phase, we'll allocate a fresh storage slot
      -- when we re-dispatch. Stash the filename pattern here so we
      -- don't have to recompute it.
      finalFilename = finalFilename,
      retries   = 0,
    }
  else
    -- carry_select: 2 tasks per chunk (carryIn=0 and carryIn=1).
    for carryIn = 0, 1 do
      local filename = string.format("%s_R%d_s%d_c%d.chunk", state.seriesId, carryIn, stepNum, i)
      tasks[#tasks+1] = {
        taskId    = myId .. "-t" .. stepNum .. "-" .. i .. "-" .. carryIn,
        kind      = "cs",
        chunkIndex= i, carryIn = carryIn,
        aRef      = aRef, bRef = bRef,
        limbsPerChunk = state.limbsPerChunk,
        resultRef = { node = pickStorageNode(), path = filename },
        retries   = 0,
      }
    end
  end
  return tasks
end

local function beginStep()
  local chunkCount = math.max(state.A.chunkCount, state.B.chunkCount)
  local pending = {}
  local stepNum = state.stepsCompleted + 1
  for i = 1, chunkCount do
    local aRef = state.A.manifest[i]
    local bRef = state.B.manifest[i]
    local tasks = buildTasksForChunk(i, aRef, bRef, stepNum)
    for _, t in ipairs(tasks) do pending[#pending+1] = t end
  end

  -- Shared growth table. The "results" map has different shapes per
  -- algorithm (see handleTaskDone / finalizeStep), but the inFlight map
  -- and pending queue are identical.
  growth = {
    algorithm  = ADD_ALGORITHM,
    subPhase   = ADD_ALGORITHM == "kogge_stone" and "gp" or "cs",
    chunkCount = chunkCount,
    pending    = pending,      -- queue of tasks not yet dispatched
    inFlight   = {},           -- taskId -> {task=, worker=, sentAt=}
    results    = {},           -- algorithm-specific (see below)
    resolvedCount = 0,
    total      = #pending,
    startedAt  = computer.uptime(),
    -- Kogge-Stone phase-2 outputs (filled when subPhase transitions
    -- from "gp" to "final"):
    gpList     = nil,          -- [chunkIndex] = {g=, p=}
    carryIns   = nil,          -- [chunkIndex] = 0|1
    finalCarry = nil,          -- 0|1 (carry out of the last chunk)
  }
end

requeueTaskForWorker = function(workerAddr)
  if not growth then return end
  for taskId, entry in pairs(growth.inFlight) do
    if entry.worker == workerAddr then
      entry.task.retries = entry.task.retries + 1
      growth.pending[#growth.pending+1] = entry.task
      growth.inFlight[taskId] = nil
      if entry.task.kind == "gp" then
        log(string.format("Requeued gp chunk %d after worker loss (retry #%d).",
          entry.task.chunkIndex, entry.task.retries), ui.palette.warn)
      else
        log(string.format("Requeued chunk %d (carryIn=%d) after worker loss (retry #%d).",
          entry.task.chunkIndex, entry.task.carryIn, entry.task.retries), ui.palette.warn)
      end
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

    -- Message shape depends on task.kind:
    --   cs    -> task_chunk_add    (legacy carry-select)
    --   gp    -> task_chunk_gp     (KS phase 1)
    --   final -> task_chunk_final (KS phase 3)
    -- task.kind is set by buildTasksForChunk for cs/gp; for final tasks
    -- we mutate kind in place when transitioning sub-phases (see
    -- transitionGpToFinal below).
    if task.kind == "gp" then
      net.send(modems, worker, {
        type        = "task_chunk_gp",
        taskId      = task.taskId,
        chunkIndex  = task.chunkIndex,
        aRef        = task.aRef, bRef = task.bRef,
        limbsPerChunk = task.limbsPerChunk,
        -- carryIn is irrelevant for gp; the worker will compute (gOut, pOut)
        -- assuming carry-in 0 (which is exactly what we want for the prefix
        -- scan, since the master adds the real carry-in via chunkFinalAdd
        -- in phase 3).
        returnArrays = false,
        -- resultRef unused for gp (no storage write); pass nil to keep
        -- the wire format consistent with the other task types.
        resultRef   = nil,
      })
    elseif task.kind == "final" then
      net.send(modems, worker, {
        type        = "task_chunk_final",
        taskId      = task.taskId,
        chunkIndex  = task.chunkIndex,
        carryIn     = task.carryIn,
        aRef        = task.aRef, bRef = task.bRef,
        limbsPerChunk = task.limbsPerChunk,
        resultRef   = task.resultRef,
      })
    else  -- "cs"
      net.send(modems, worker, {
        type        = "task_chunk_add",
        taskId      = task.taskId,
        chunkIndex  = task.chunkIndex,
        carryIn     = task.carryIn,
        aRef        = task.aRef, bRef = task.bRef,
        limbsPerChunk = task.limbsPerChunk,
        resultRef   = task.resultRef,
      })
    end
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
      if entry.task.kind == "gp" then
        log(string.format("gp chunk %d timed out, retrying (#%d).",
          entry.task.chunkIndex, entry.task.retries), ui.palette.warn)
      else
        log(string.format("Chunk %d (carryIn=%d) timed out, retrying (#%d).",
          entry.task.chunkIndex, entry.task.carryIn, entry.task.retries), ui.palette.warn)
      end
    end
  end
end

-- Handle a task_done reply. Shape varies by algorithm / sub-phase:
--   cs     : rec = {c0={carryOut=, resultRef=}, c1={...}}
--   final  : rec = {result=, carryOut=}  (one per chunk; chunkFinalAdd
--            returns carryOut too, which we use to detect top-chunk extend)
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
      log(string.format("%s chunk %d still failing after %d retries: %s",
        entry.task.kind, entry.task.chunkIndex, entry.task.retries, tostring(msg.err)), ui.palette.bad)
    end
    return
  end

  if entry.task.kind == "cs" then
    -- Carry-select: store the result keyed by (chunkIndex, carryIn).
    local rec = growth.results[msg.chunkIndex] or {}
    if msg.carryIn == 0 then rec.c0 = { carryOut = msg.carryOut, resultRef = entry.task.resultRef } end
    if msg.carryIn == 1 then rec.c1 = { carryOut = msg.carryOut, resultRef = entry.task.resultRef } end
    growth.results[msg.chunkIndex] = rec
  elseif entry.task.kind == "final" then
    -- Kogge-Stone phase 3: store the single final result + carryOut.
    growth.results[msg.chunkIndex] = {
      resultRef = entry.task.resultRef,
      carryOut  = msg.carryOut,
    }
  end
  growth.resolvedCount = growth.resolvedCount + 1
end

-- Handle a task_gp_done reply (Kogge-Stone phase 1 only).
-- Stores the (gOut, pOut) pair into growth.gpList, indexed by chunkIndex.
local function handleGpDone(msg, remoteAddr)
  -- Same busy-flag hygiene as handleTaskDone.
  if nodes[remoteAddr] then nodes[remoteAddr].busy = false end

  if not growth then return end
  local entry = growth.inFlight[msg.taskId]
  if not entry then return end
  growth.inFlight[msg.taskId] = nil

  if not msg.ok then
    entry.task.retries = entry.task.retries + 1
    growth.pending[#growth.pending+1] = entry.task
    if entry.task.retries % 4 == 0 then
      log(string.format("gp chunk %d still failing after %d retries: %s",
        entry.task.chunkIndex, entry.task.retries, tostring(msg.err)), ui.palette.bad)
    end
    return
  end

  if not growth.gpList then growth.gpList = {} end
  growth.gpList[msg.chunkIndex] = { g = msg.gOut, p = msg.pOut }
  growth.resolvedCount = growth.resolvedCount + 1
end

-- Kogge-Stone: once all gp replies are in, run the master-side prefix
-- scan to get every chunk's carryIn, then build the final-phase task
-- queue and switch sub-phase to "final". Returns true on success,
-- false (with growth cleared) on internal inconsistency.
local function transitionGpToFinal()
  if not growth or growth.subPhase ~= "gp" then return false end
  if not growth.gpList then
    log("transitionGpToFinal: no gp results collected - aborting step.", ui.palette.bad)
    growth = nil
    return false
  end

  -- Build a Lua array of {g, p} in chunk-index order (1..chunkCount).
  -- The prefix scan needs them contiguous; nil gaps mean a chunk
  -- somehow didn't reply, which shouldn't happen since resolvedCount
  -- reached total. Defensive: pad missing entries with {g=0, p=0}
  -- (which means "this chunk never generates or propagates a carry",
  -- the safe default if a reply was lost).
  local gpArray = {}
  for i = 1, growth.chunkCount do
    gpArray[i] = growth.gpList[i] or { g = 0, p = 0 }
  end

  local carryIns, finalCarry = bigint.masterPrefixCarries(gpArray, growth.chunkCount)
  growth.carryIns   = carryIns
  growth.finalCarry = finalCarry

  -- Sanity: carryIns[1] must be 0 (no carry into the lowest chunk).
  if carryIns[1] ~= 0 then
    log("transitionGpToFinal: masterPrefixCarries returned non-zero carryIn[1] - clamping.", ui.palette.warn)
    carryIns[1] = 0
  end

  -- Build the final-phase task queue. Re-use the same aRef/bRef as
  -- the gp phase (they're the same operands). The resultRef for each
  -- final task points to a fresh storage slot.
  local stepNum = state.stepsCompleted + 1
  growth.pending = {}
  for i = 1, growth.chunkCount do
    local aRef = state.A.manifest[i]
    local bRef = state.B.manifest[i]
    local finalFilename = string.format("%s_R_s%d_c%d.chunk", state.seriesId, stepNum, i)
    growth.pending[#growth.pending+1] = {
      taskId    = myId .. "-fin-" .. stepNum .. "-" .. i,
      kind      = "final",
      chunkIndex= i,
      carryIn   = carryIns[i] or 0,
      aRef      = aRef, bRef = bRef,
      limbsPerChunk = state.limbsPerChunk,
      resultRef = { node = pickStorageNode(), path = finalFilename },
      retries   = 0,
    }
  end
  growth.subPhase      = "final"
  growth.resolvedCount = 0
  growth.total         = #growth.pending
  log(string.format("KS phase 2 done: %d chunks, finalCarry=%d. Dispatching phase 3.",
    growth.chunkCount, finalCarry), ui.palette.accent2)
  return true
end

-- Carry-select: sequentially resolves the real carry chain using only
-- the tiny carry BITS already collected (never touches chunk data),
-- deletes the losing tentative chunk of each pair, returns new B manifest.
-- Kogge-Stone: just walks the final-phase results, picks each chunk's
-- resultRef (one per chunk), and applies the precomputed finalCarry
-- to decide whether to add a top-extension chunk.
local function finalizeStep()
  local newManifest = {}
  local newChunkCount

  if growth.algorithm == "kogge_stone" then
    -- One final result per chunk; the real carry-in for each was already
    -- baked into the stored result by the worker (via chunkFinalAdd).
    -- The per-chunk carryOut reported by the worker is informational only
    -- (we already know the prefix carry); we use the master-side
    -- finalCarry to decide whether to extend.
    for i = 1, growth.chunkCount do
      local rec = growth.results[i]
      if not rec then
        log(string.format("finalizeStep: chunk %d missing final result - aborting step.", i), ui.palette.bad)
        growth = nil
        return
      end
      newManifest[i] = rec.resultRef
    end
    newChunkCount = growth.chunkCount
    -- The carry-out of the last chunk is growth.finalCarry (the prefix
    -- carry-out of all chunks). If it's 1, we need a new top chunk.
    if growth.finalCarry == 1 then
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
  else
    -- Carry-select: walk chunks in order, picking the (c0 or c1)
    -- variant that matches the running carry, deleting the loser.
    local carry = 0
    for i = 1, growth.chunkCount do
      local rec = growth.results[i]
      local chosen, loser
      if carry == 0 then chosen, loser = rec.c0, rec.c1 else chosen, loser = rec.c1, rec.c0 end
      newManifest[i] = chosen.resultRef
      if loser then masterDeleteChunk(loser.resultRef.node, loser.resultRef.path) end
      carry = chosen.carryOut
    end
    newChunkCount = growth.chunkCount
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
  -- os.time() on OpenComputers / Lua 5.3+ returns a FLOAT (Unix time
  -- with fractional seconds). Subtracting two such floats yields a
  -- float, and string.format("%d", x) raises
  --   "bad argument #N to 'format' (number has no integer representation)"
  -- if x has a fractional part. Floor both ends so the subtraction is
  -- always over integers (and the result is always integral).
  local now  = math.floor(os.time())
  local ago  = now - math.floor(cp.savedAt or now)
  ui.clearArea(3, 3 + netBoxH, W - 4, 3)
  log(string.format("Found checkpoint: series %s, step %d (n=%d), saved %ds ago.",
    tostring(cp.seriesId), math.floor(cp.stepsCompleted or 0), math.floor(cp.n or 0), ago), ui.palette.accent2)
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
      elseif msg.type == "task_gp_done" then
        handleGpDone(msg, remoteAddr)
      end
    end)
    if not ok then log("message handler error: " .. tostring(err), ui.palette.bad) end
  end

  sweepDeadNodes()

  if state.phase == "WAITING_FOR_STORAGE" then
    if #storageNodes() > 0 then
      local ok, err = pcall(runSeeding, aBig, bBig)
      aBig, bBig = nil, nil
      if collectgarbage then collectgarbage() end
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

      -- Kogge-Stone phase transition: when the gp sub-phase has all its
      -- replies in, run the master-side prefix scan and rebuild the
      -- pending queue with task_chunk_final tasks. Then continue
      -- dispatching in the "final" sub-phase.
      if growth.algorithm == "kogge_stone"
         and growth.subPhase == "gp"
         and growth.resolvedCount >= growth.total then
        local ok, err = pcall(transitionGpToFinal)
        if not ok then
          log("transitionGpToFinal error: " .. tostring(err), ui.palette.bad)
          growth = nil
        end
      end

      -- Step is fully done when the current sub-phase has resolved all
      -- its tasks. For carry_select this is the only sub-phase; for
      -- kogge_stone this only fires once subPhase == "final" has
      -- resolved everything (because the gp sub-phase was already
      -- transitioned out of above).
      if growth
         and (growth.algorithm ~= "kogge_stone" or growth.subPhase == "final")
         and growth.resolvedCount >= growth.total then
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