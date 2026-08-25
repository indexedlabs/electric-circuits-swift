#!/usr/bin/env node
// SWF-P1-2 Layer-B: one disposable PG18 -> logical replication -> engine/Axum -> durable-streams
// topology. It deliberately owns every process, fixed port, data root, and phase receipt.
import { execFileSync, spawn } from 'node:child_process'
import { appendFileSync, existsSync, lstatSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { createHash } from 'node:crypto'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import pg from 'pg'

const server = process.env.ELECTRIC_CIRCUITS_SERVER_ROOT ?? '/Users/bozilabs/labs/electric-circuits'
const candidate = dirname(dirname(fileURLToPath(import.meta.url)))
const retainedEvidence = join(candidate, 'Evidence')
mkdirSync(retainedEvidence, { recursive: true })
const runRoot = mkdtempSync(join(tmpdir(), 'electric-circuits-swift-engine-ds-outage-v1-'))
const pgData = join(runRoot, 'postgres')
const pgLog = join(runRoot, 'postgres.log')
const dsData = join(runRoot, 'durable-streams')
const phase = join(runRoot, 'phase')
mkdirSync(dsData)
mkdirSync(phase)

type RawEngine = { proc: ReturnType<typeof spawn>, stderr(): string, waitForListening(timeoutMs?: number): Promise<string>, waitForExit(timeoutMs?: number): Promise<{ code: number | null, signal: string | null }>, signal(sig?: NodeJS.Signals): void }
let pgCtl = ''
let pgPort = 0
let enginePort = 0
let dsPort = 0
let swift: ReturnType<typeof spawn> | undefined
let engine: RawEngine | undefined
let ds: { start(): Promise<string>, stop(): Promise<void>, pid?: number } | undefined
let passed = false
let failure: string | undefined
let cleanupFailure: string | undefined

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))
const sha256 = (path: string) => createHash('sha256').update(readFileSync(path)).digest('hex')
function command(command: string, args: string[], cwd = server): string {
  return execFileSync(command, args, { cwd, encoding: 'utf8' }).trim()
}
function digestTree(root: string): Array<{ path: string, mode: string, sha256: string }> {
  const ignored = new Set(['.git', 'node_modules', 'target', '.claude', '.build', '.derivedData'])
  const walk = (relative: string): Array<{ path: string, mode: string, sha256: string }> => {
    const absolute = join(root, relative)
    return readdirSync(absolute, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name)).flatMap(entry => {
      if (ignored.has(entry.name)) return []
      const child = relative ? join(relative, entry.name) : entry.name
      const full = join(root, child)
      if (entry.isDirectory()) return walk(child)
      if (!entry.isFile()) return []
      return [{ path: child, mode: (lstatSync(full).mode & 0o777).toString(8), sha256: sha256(full) }]
    })
  }
  return walk('')
}
function runtimeIdentity(tools: { initdb: string, pgCtl: string }, dsBinary: string) {
  const status = command('git', ['status', '--porcelain=v1', '--untracked-files=all'])
  if (status && process.env.ECS_ALLOW_DIRTY_SERVER !== '1') {
    throw new Error('server checkout is dirty; set ECS_ALLOW_DIRTY_SERVER=1 only to record its complete source manifest')
  }
  const serverTree = digestTree(server)
  const candidateTree = digestTree(candidate)
  return {
    server: {
      root: server, head: command('git', ['rev-parse', 'HEAD']), tree: command('git', ['rev-parse', 'HEAD^{tree}']),
      statusPorcelainV1: status, completeRuntimeSourceManifest: serverTree,
      completeRuntimeSourceManifestSha256: createHash('sha256').update(JSON.stringify(serverTree)).digest('hex'),
    },
    swiftCandidate: {
      root: candidate, completeRuntimeSourceManifest: candidateTree,
      completeRuntimeSourceManifestSha256: createHash('sha256').update(JSON.stringify(candidateTree)).digest('hex'),
    },
    runtimeExecutables: {
      engine: { path: join(server, 'target/debug/electric-circuits-engine'), sha256: sha256(join(server, 'target/debug/electric-circuits-engine')) },
      durableStreams: { path: dsBinary, sha256: sha256(dsBinary) },
      postgres18: {
        initdb: { path: tools.initdb, version: command(tools.initdb, ['--version'], candidate), sha256: sha256(tools.initdb) },
        pgCtl: { path: tools.pgCtl, version: command(tools.pgCtl, ['--version'], candidate), sha256: sha256(tools.pgCtl) },
      },
    },
    toolchain: { node: process.version, pnpm: command('pnpm', ['--version'], candidate), swift: command('swift', ['--version'], candidate).split('\n')[0] },
  }
}
async function freePort(): Promise<number> {
  const net = await import('node:net')
  return await new Promise((resolve, reject) => {
    const listener = net.createServer().once('error', reject).listen(0, '127.0.0.1', () => {
      const address = listener.address()
      listener.close(() => typeof address === 'object' && address ? resolve(address.port) : reject(new Error('no port')))
    })
  })
}
async function waitFor(name: string, predicate: () => Promise<boolean> | boolean, timeoutMs = 45_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await sleep(25)
  }
  throw new Error(`deadline waiting for ${name}; root=${runRoot}`)
}
function writePhase(name: string, body: unknown = {}): void {
  writeFileSync(join(phase, name), JSON.stringify(body, null, 2))
}
function assertNoListener(port: number, label: string): void {
  try {
    const pids = execFileSync('lsof', ['-tiTCP:' + port, '-sTCP:LISTEN'], { encoding: 'utf8' }).trim()
    if (pids) throw new Error(`${label} listener survived on ${port}: ${pids}`)
  } catch (error: any) {
    if (error.status === 1) return
    throw error
  }
}
async function sql(url: string, query: string, values: unknown[] = []) {
  const client = new pg.Client({ connectionString: url })
  await client.connect()
  try { return await client.query(query, values) } finally { await client.end() }
}
async function awaitChildWithin(child: ReturnType<typeof spawn>, label: string, deadlineMs: number): Promise<void> {
  const exitState = () => child.exitCode !== null || child.signalCode !== null
  const exitDescription = () => `code=${child.exitCode} signal=${child.signalCode}`
  if (exitState()) {
    if (child.exitCode === 0 && child.signalCode === null) return
    throw new Error(`${label} pre-exited ${exitDescription()}`)
  }
  await new Promise<void>((resolve, reject) => {
    let settled = false
    let deadlineExpired = false
    let timer: ReturnType<typeof setTimeout> | undefined
    let hardKillTimer: ReturnType<typeof setTimeout> | undefined
    let reapTimer: ReturnType<typeof setTimeout> | undefined
    let observer: ReturnType<typeof setInterval> | undefined
    const onExit = () => settleFromObservedExit()
    const finish = (error?: Error) => {
      if (settled) return
      settled = true
      if (timer) clearTimeout(timer)
      if (hardKillTimer) clearTimeout(hardKillTimer)
      if (reapTimer) clearTimeout(reapTimer)
      if (observer) clearInterval(observer)
      child.removeListener('exit', onExit)
      child.removeListener('error', onError)
      error ? reject(error) : resolve()
    }
    const settleFromObservedExit = () => {
      if (!exitState()) return false
      if (deadlineExpired) {
        finish(new Error(`${label} exceeded ${deadlineMs}ms; reaped ${exitDescription()}`))
      } else if (child.exitCode === 0 && child.signalCode === null) {
        finish()
      } else {
        finish(new Error(`${label} exited ${exitDescription()}`))
      }
      return true
    }
    const onError = (error: Error) => finish(error)
    const reap = () => {
      if (settleFromObservedExit()) return
      child.kill('SIGTERM')
      hardKillTimer = setTimeout(() => {
        if (!settleFromObservedExit()) child.kill('SIGKILL')
      }, 250)
      reapTimer = setTimeout(() => {
        if (!settleFromObservedExit()) {
          finish(new Error(`${label} exceeded ${deadlineMs}ms and could not be reaped; ${exitDescription()}`))
        }
      }, 2_000)
    }
    child.once('exit', onExit)
    child.once('error', onError)
    // An exit can happen between the preflight check and listener installation. Read the process
    // state again, and keep an independent observer so a missed event cannot wedge the deadline.
    if (settleFromObservedExit()) return
    observer = setInterval(settleFromObservedExit, 25)
    timer = setTimeout(() => {
      deadlineExpired = true
      reap()
    }, deadlineMs)
  })
}
async function selfTestChildDeadline(): Promise<void> {
  const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' })
  let timedOut = false
  try { await awaitChildWithin(child, 'self-test hung child', 50) } catch { timedOut = true }
  if (!timedOut || (child.exitCode === null && child.signalCode === null)) throw new Error('child deadline self-test left a process alive')
  const preExited = spawn(process.execPath, ['-e', 'process.exit(0)'], { stdio: 'ignore' })
  await new Promise<void>((resolve, reject) => {
    preExited.once('exit', () => resolve())
    preExited.once('error', reject)
  })
  await awaitChildWithin(preExited, 'self-test pre-exited child', 50)
  console.log('PASS child-deadline and pre-exited-child self-tests')
}

async function main(): Promise<void> {
  const { postgres18Tools } = await import(join(server, 'scripts/postgres18.ts'))
  const { DurableStreamTestServer, ensureServerBinary } = await import(join(server, 'packages/ds-rust/src/index.ts'))
  const { buildEngine, spawnRawEngine, changesTail, engineChangesOffset, positionReached } = await import(join(server, 'packages/conformance/src/harness.ts'))
  buildEngine()
  const tools = postgres18Tools()
  const dsBinary = ensureServerBinary()
  const identity = runtimeIdentity(tools, dsBinary)
  pgCtl = tools.pgCtl
  pgPort = await freePort()
  enginePort = await freePort()
  dsPort = await freePort()
  const engineURL = `http://127.0.0.1:${enginePort}`
  const dsURL = `http://127.0.0.1:${dsPort}`
  const database = `swift_outage_${process.pid}_${Date.now().toString(36)}`.toLowerCase()
  const adminURL = `postgres://postgres@127.0.0.1:${pgPort}/postgres`
  const pgURL = `postgres://postgres@127.0.0.1:${pgPort}/${database}`
  const slot = `swift_outage_${process.pid}_${Date.now().toString(36)}`.toLowerCase()
  execFileSync(tools.initdb, ['-D', pgData, '-U', 'postgres', '--auth=trust', '--no-sync'])
  appendFileSync(join(pgData, 'postgresql.conf'), `\nlisten_addresses = '127.0.0.1'\nport = ${pgPort}\nwal_level = logical\nmax_replication_slots = 20\nmax_wal_senders = 20\n`)
  execFileSync(pgCtl, ['-D', pgData, '-l', pgLog, '-w', 'start'])
  await sql(adminURL, `CREATE DATABASE ${database}`)
  await sql(pgURL, `
    CREATE TABLE items (id integer PRIMARY KEY, title text NOT NULL);
    ALTER TABLE items REPLICA IDENTITY FULL;
    CREATE TABLE __el_sync (id integer PRIMARY KEY, n bigint NOT NULL);
    INSERT INTO __el_sync (id, n) VALUES (1, 0);`)
  ds = new DurableStreamTestServer({ port: dsPort, dataDir: dsData, longPollTimeout: 500 })
  if (await ds.start() !== dsURL) throw new Error('durable-streams did not bind the owned fixed public URL')
  const startEngine = async () => {
    const raw = spawnRawEngine({
      ELECTRIC_CIRCUITS_DS_URL: dsURL,
      ELECTRIC_CIRCUITS_BIND: `127.0.0.1:${enginePort}`,
      ELECTRIC_CIRCUITS_LOG: 'warn',
      ELECTRIC_CIRCUITS_PG_URL: pgURL,
      ELECTRIC_CIRCUITS_PG_TABLES: 'items',
      ELECTRIC_CIRCUITS_PG_SLOT: slot,
      ELECTRIC_CIRCUITS_PG_POLL_MS: '25',
    }) as RawEngine
    const listening = await raw.waitForListening(20_000)
    if (listening !== engineURL) throw new Error(`engine public Axum URL drifted: ${listening} != ${engineURL}`)
    engine = raw
  }
  const stopEngine = async (signal: NodeJS.Signals = 'SIGTERM') => {
    if (!engine) return
    engine.signal(signal)
    await engine.waitForExit(10_000)
    engine = undefined
  }
  const replicationStatus = async () => {
    const response = await fetch(`${engineURL}/replication/lsn`)
    if (!response.ok) throw new Error(`replication status -> ${response.status}`)
    return await response.json() as { sync: number, pendingFlips?: number }
  }
  const drainThrough = async (marker: number, phaseName: string) => {
    await waitFor(`${phaseName} engine source-marker receipt`, async () => (await replicationStatus()).sync >= marker)
    const tail = await changesTail(dsURL, engineURL)
    if (tail) {
      await waitFor(`${phaseName} sequencer receipt`, async () => {
        const position = await engineChangesOffset(engineURL)
        return position === null || positionReached(position, tail)
      })
    }
    await waitFor(`${phaseName} deferred-work receipt`, async () => ((await replicationStatus()).pendingFlips ?? 0) === 0)
    writePhase(`${phaseName}-server-receipt`, { marker, publicAxumURL: engineURL, receipt: 'source marker -> replication sync -> change-log tail -> deferred work' })
  }
  const commitMutation = async (phaseName: string, statements: string[]) => {
    const client = new pg.Client({ connectionString: pgURL })
    await client.connect()
    try {
      await client.query('BEGIN')
      for (const statement of statements) await client.query(statement)
      // This harness-only source marker is the FINAL logical change in this same transaction.
      const marker = Number((await client.query('UPDATE __el_sync SET n = n + 1 WHERE id = 1 RETURNING n')).rows[0].n)
      await client.query('COMMIT')
      writePhase(`${phaseName}-mutation-committed`, { marker, sourceMarker: '__el_sync.n', finalChangeInTransaction: true })
      return marker
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {})
      throw error
    } finally { await client.end() }
  }
  const phaseEvidence: Record<string, { oracle: unknown[], receipt: { rows: unknown[], cursor: unknown } }> = {}
  const verifyPhaseReceipt = async (phaseName: string) => {
    const receiptPath = join(phase, `${phaseName}-client-provider-receipt.json`)
    const receipt = JSON.parse(readFileSync(receiptPath, 'utf8')) as { rows?: Record<string, any>, cursor?: { offset?: string } }
    if (!receipt.rows || !receipt.cursor?.offset) throw new Error(`${phaseName} Swift receipt lacks rows or durable cursor`)
    const receiptRows = Object.entries(receipt.rows)
      .map(([key, row]: any) => ({ id: Number(key), title: row.title.string ?? row.title }))
      .sort((a, b) => a.id - b.id)
    const oracle = (await sql(pgURL, 'SELECT id, title FROM items ORDER BY id')).rows
    if (JSON.stringify(receiptRows) !== JSON.stringify(oracle)) {
      throw new Error(`${phaseName} SQL oracle mismatch expected=${JSON.stringify(oracle)} actual=${JSON.stringify(receiptRows)}`)
    }
    phaseEvidence[phaseName] = { oracle, receipt: { rows: receiptRows, cursor: receipt.cursor } }
  }

  await startEngine()
  const baselineMarker = await commitMutation('baseline', [
    "INSERT INTO items (id, title) VALUES (1, 'before'), (2, 'delete-me')",
  ])
  await drainThrough(baselineMarker, 'baseline')
  writeFileSync(join(runRoot, 'manifest.json'), JSON.stringify({
    scenario: 'SWF-P1-2-engine-ds-outage-v1', profile: 'NATIVE_CORE_LAYER_B', identity,
    endpoints: { postgres: adminURL, engine: engineURL, durableStreams: dsURL },
    roots: { pgData, durableStreams: dsData, phase }, slot,
  }, null, 2))
  swift = spawn('swift', ['run', 'ElectricCircuitsSwiftEngineDSOutage'], {
    cwd: candidate,
    env: { ...process.env, ECS_OUTAGE_BASE_URL: engineURL, ECS_OUTAGE_PHASE_DIR: phase },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let swiftLog = ''
  swift.stdout?.on('data', chunk => { swiftLog += chunk; appendFileSync(join(runRoot, 'swift.log'), chunk) })
  swift.stderr?.on('data', chunk => { swiftLog += chunk; appendFileSync(join(runRoot, 'swift.log'), chunk) })
  await waitFor('baseline client/provider receipt', () => existsSync(join(phase, 'baseline-client-provider-receipt.json')))
  await verifyPhaseReceipt('baseline')

  // Phase 1: same public Axum URL, streaming claim retained in durable streams, mutation while down.
  await stopEngine('SIGKILL')
  assertNoListener(enginePort, 'engine before restart')
  const engineRestartMarker = await commitMutation('engine-restart', [
    "UPDATE items SET title = 'after-engine' WHERE id = 1",
    "INSERT INTO items (id, title) VALUES (3, 'engine-created')",
  ])
  await startEngine()
  await drainThrough(engineRestartMarker, 'engine-restart')
  await waitFor('engine restart client/provider receipt', () => existsSync(join(phase, 'engine-restart-client-provider-receipt.json')))
  await verifyPhaseReceipt('engine-restart')

  // Phase 2: service is genuinely absent; the same port and writable DS root return unchanged.
  await ds.stop()
  ds = undefined
  assertNoListener(dsPort, 'durable-streams during outage')
  writePhase('durable-streams-outage-started', { port: dsPort, durableDataDirectory: dsData })
  await waitFor('Swift long-poll retry receipt', () => existsSync(join(phase, 'durable-streams-client-retrying')))
  const durableStreamsMarker = await commitMutation('durable-streams', [
    "UPDATE items SET title = 'after-ds' WHERE id = 1",
    'DELETE FROM items WHERE id = 2',
    "INSERT INTO items (id, title) VALUES (4, 'ds-created')",
  ])
  await waitFor('engine durable-stream append retry', () => {
    const log = engine?.stderr() ?? ''
    // Replication calls the retry state "reconnecting" while the sequencer calls it "backing
    // off"; both name an engine-owned retry after a refused durable-stream append.
    return log.includes('replicator: append changes:') && (log.includes('reconnecting') || log.includes('retrying'))
  })
  writePhase('durable-streams-engine-append-retrying', { marker: durableStreamsMarker, evidence: 'engine-owned append retry while durable-streams listener is absent' })
  ds = new DurableStreamTestServer({ port: dsPort, dataDir: dsData, longPollTimeout: 500 })
  if (await ds.start() !== dsURL) throw new Error('durable-streams restart changed its public URL')
  await drainThrough(durableStreamsMarker, 'durable-streams')
  await waitFor('durable-streams client/provider receipt', () => existsSync(join(phase, 'durable-streams-client-provider-receipt.json')))
  await verifyPhaseReceipt('durable-streams')
  await awaitChildWithin(swift, 'Swift outage coordinator', Number(process.env.ECS_OUTAGE_CHILD_DEADLINE_MS ?? '45_000')).catch(error => {
    throw new Error(`${String(error)}\n${swiftLog}`)
  })

  const swiftResult = JSON.parse(readFileSync(join(phase, 'swift-result.json'), 'utf8'))
  const oracle = (await sql(pgURL, 'SELECT id, title FROM items ORDER BY id')).rows
  const actual = Object.entries(swiftResult.rows).map(([key, row]: any) => ({ id: Number(key), title: row.title.string ?? row.title })).sort((a, b) => a.id - b.id)
  if (JSON.stringify(actual) !== JSON.stringify(oracle)) throw new Error(`SQL oracle mismatch expected=${JSON.stringify(oracle)} actual=${JSON.stringify(actual)}`)
  if (!swiftResult.controlHeaderForwarded || !swiftResult.streamHeaderForwarded || !swiftResult.releaseHeaderForwarded) throw new Error('custom auth/header forwarding was not retained across control, stream, and release')
  if (!swiftResult.durableStreamsRetryObserved || !swiftResult.reopenedRowsMatch || !swiftResult.reopenedCursorMatch) throw new Error('Swift retry or file-provider reopen proof missing')
  if (new Set(swiftResult.appliedCursors).size !== swiftResult.appliedCursors.length) throw new Error('Swift provider recorded duplicate checkpoint application')
  writeFileSync(join(runRoot, 'result.json'), JSON.stringify({
    sourceMarkers: { baseline: baselineMarker, engineRestart: engineRestartMarker, durableStreams: durableStreamsMarker },
    publicAxumURL: engineURL, durableStreamsURL: dsURL, phaseEvidence, oracle, actual, swiftResult,
  }, null, 2))
  passed = true
  console.log(`PASS SWF-P1-2-engine-ds-outage-v1 root=${runRoot}`)
}

async function cleanup(): Promise<void> {
  const problems: string[] = []
  try { if (swift?.exitCode === null) swift.kill('SIGKILL') } catch (error) { problems.push(`Swift kill: ${error}`) }
  try {
    if (engine) {
      try {
        engine.signal('SIGTERM')
        await engine.waitForExit(10_000)
      } catch {
        // A durable-stream outage can leave a graceful shutdown party retrying. The runner owns
        // this child, so its external deadline escalates and then verifies no listener survives.
        engine.signal('SIGKILL')
        await engine.waitForExit(2_000)
      }
      engine = undefined
    }
  } catch (error) { problems.push(`engine stop: ${error}`) }
  try { await ds?.stop() } catch (error) { problems.push(`durable-streams stop: ${error}`) }
  try { if (pgCtl) execFileSync(pgCtl, ['-D', pgData, '-m', 'immediate', '-w', 'stop']) } catch (error) { problems.push(`postgres stop: ${error}`) }
  for (const [port, label] of [[enginePort, 'engine'], [dsPort, 'durable-streams'], [pgPort, 'postgres']] as const) {
    try { if (port) assertNoListener(port, label) } catch (error) { problems.push(String(error)) }
  }
  for (const root of [pgData, dsData, phase]) {
    try {
      rmSync(root, { recursive: true, force: true })
      if (existsSync(root)) problems.push(`owned root survived cleanup: ${root}`)
    } catch (error) { problems.push(`root cleanup ${root}: ${error}`) }
  }
  cleanupFailure = problems.length ? problems.join('; ') : undefined
}

async function runQualification(): Promise<void> {
  try { await main() } catch (error) {
    failure = String(error)
    console.error(error)
    process.exitCode = 1
  } finally {
    await cleanup()
    if (cleanupFailure) {
      failure = failure ? `${failure}; cleanup: ${cleanupFailure}` : `cleanup: ${cleanupFailure}`
      process.exitCode = 1
    }
    const record = {
      status: passed && !cleanupFailure ? 'PASS' : 'FAIL', failure,
      manifest: existsSync(join(runRoot, 'manifest.json')) ? JSON.parse(readFileSync(join(runRoot, 'manifest.json'), 'utf8')) : undefined,
      result: existsSync(join(runRoot, 'result.json')) ? JSON.parse(readFileSync(join(runRoot, 'result.json'), 'utf8')) : undefined,
      cleanup: { passed: !cleanupFailure, failure: cleanupFailure, pgDataRemoved: !existsSync(pgData), durableStreamsDataRemoved: !existsSync(dsData), phaseRemoved: !existsSync(phase) },
    }
    writeFileSync(join(retainedEvidence, 'engine-ds-outage-pg18-last-run.json'), JSON.stringify(record, null, 2))
    rmSync(runRoot, { recursive: true, force: true })
  }
}

async function entry(): Promise<void> {
  if (process.argv.includes('--self-test-child-deadline')) {
    try { await selfTestChildDeadline() } finally { rmSync(runRoot, { recursive: true, force: true }) }
    return
  }
  await runQualification()
}
void entry()
