#!/usr/bin/env node
// SWF-P1-1: a real PG18.4 physical standby without synchronised logical failover slots is an
// epoch boundary. This runner owns every mutable process/root and drives the public Swift client
// at one stable Axum URL across restart. The sibling Rust checkout is runtime input only.
import { execFileSync, spawn } from 'node:child_process'
import { createHash } from 'node:crypto'
import { appendFileSync, existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { DurableStreamTestServer } from '@electric-circuits/ds-rust'
import pgpkg from 'pg'

const server = process.env.ELECTRIC_CIRCUITS_SERVER_ROOT ?? '/Users/bozilabs/labs/electric-circuits'
const candidate = dirname(dirname(fileURLToPath(import.meta.url)))
const retainedEvidence = join(candidate, 'Evidence')
const root = mkdtempSync(join(tmpdir(), 'electric-circuits-swift-pg18-failover-v1-'))
const phase = join(root, 'phase')
const primaryData = join(root, 'primary')
const standbyData = join(root, 'standby')
const databasePath = join(root, 'swift.sqlite')
mkdirSync(phase)
mkdirSync(retainedEvidence, { recursive: true })

const ITEM = 'items'
const MARKER = '__el_sync'
const TIMEOUT_MS = 45_000
let pgCtl = ''
let primaryPort = 0
let standbyPort = 0
let enginePort = 0
let dsPort = 0
let ds: DurableStreamTestServer | undefined
let engine: any
let swift: ReturnType<typeof spawn> | undefined
let scenarioCompleted = false
let cleanupCompleted = false
let failure: string | undefined
let swiftLog = ''
const pids = new Set<number>()

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))
const sha256 = (path: string) => createHash('sha256').update(readFileSync(path)).digest('hex')
const command = (bin: string, args: string[], cwd = candidate) => execFileSync(bin, args, { cwd, encoding: 'utf8' }).trim()

class PhaseLedger {
  private index = 0
  readonly values: string[] = []
  constructor(private readonly expected: string[]) {}
  advance(next: string) {
    if (this.expected[this.index] !== next) {
      throw new Error(`invalid causal phase: expected ${this.expected[this.index] ?? 'end'}, got ${next}`)
    }
    this.values.push(next)
    this.index += 1
  }
  complete() { return this.index === this.expected.length }
}

function treeDigest(rootPath: string, ignored: Set<string>) {
  const walk = (relative: string): Array<{ path: string, mode: string, sha256: string }> => {
    const absolute = join(rootPath, relative)
    return readdirSync(absolute, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name)).flatMap(entry => {
      if (ignored.has(entry.name)) return []
      const child = relative ? join(relative, entry.name) : entry.name
      const full = join(rootPath, child)
      if (entry.isDirectory()) return walk(child)
      return entry.isFile() ? [{ path: child, mode: (lstatSync(full).mode & 0o777).toString(8), sha256: sha256(full) }] : []
    })
  }
  return walk('')
}

function identity(tools: { initdb: string, pgCtl: string, pgBasebackup: string }, dsBinary: string) {
  const serverStatus = command('git', ['status', '--porcelain=v1', '--untracked-files=all'], server)
  if (serverStatus && process.env.ECS_ALLOW_DIRTY_SERVER !== '1') {
    throw new Error('server runtime checkout is dirty; set ECS_ALLOW_DIRTY_SERVER=1 to retain its complete overlay identity')
  }
  const serverManifest = treeDigest(server, new Set(['.git', 'node_modules', 'target', '.build', '.derivedData', 'DerivedData', 'Evidence']))
  const candidateManifest = treeDigest(candidate, new Set(['.git', '.build', '.derivedData', 'DerivedData', 'Evidence']))
  return {
    server: {
      root: server, head: command('git', ['rev-parse', 'HEAD'], server), tree: command('git', ['rev-parse', 'HEAD^{tree}'], server),
      statusSha256: createHash('sha256').update(serverStatus).digest('hex'),
      runtimeSourceManifestSha256: createHash('sha256').update(JSON.stringify(serverManifest)).digest('hex'),
    },
    swiftCandidate: { root: candidate, runtimeInputManifestSha256: createHash('sha256').update(JSON.stringify(candidateManifest)).digest('hex') },
    executables: {
      engine: { path: join(server, 'target/debug/electric-circuits-engine'), sha256: sha256(join(server, 'target/debug/electric-circuits-engine')) },
      durableStreams: { path: dsBinary, sha256: sha256(dsBinary) },
      postgres18: {
        initdb: { path: tools.initdb, version: command(tools.initdb, ['--version']), sha256: sha256(tools.initdb) },
        pgCtl: { path: tools.pgCtl, version: command(tools.pgCtl, ['--version']), sha256: sha256(tools.pgCtl) },
        pgBasebackup: { path: tools.pgBasebackup, version: command(tools.pgBasebackup, ['--version']), sha256: sha256(tools.pgBasebackup) },
      },
    },
    toolchain: { node: process.version, swift: command('swift', ['--version']).split('\n')[0], pnpm: command('pnpm', ['--version']) },
  }
}

async function freePort(): Promise<number> {
  return await new Promise((resolve, reject) => {
    const listener = createServer().once('error', reject).listen(0, '127.0.0.1', () => {
      const address = listener.address()
      listener.close(error => error || !address || typeof address === 'string' ? reject(error ?? new Error('no port')) : resolve(address.port))
    })
  })
}

async function waitFor(name: string, predicate: () => Promise<boolean> | boolean, deadlineMs = TIMEOUT_MS): Promise<void> {
  const deadline = Date.now() + deadlineMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await sleep(25)
  }
  throw new Error(`deadline waiting for ${name}; root=${root}`)
}

async function sql(url: string, statement: string, values: unknown[] = []) {
  const client = new pgpkg.Client({ connectionString: url }); await client.connect()
  try { return await client.query(statement, values) } finally { await client.end().catch(() => {}) }
}
async function scalar<T>(url: string, statement: string, values: unknown[] = []): Promise<T> {
  const row = (await sql(url, statement, values)).rows[0]
  if (!row) throw new Error(`missing SQL scalar: ${statement}`)
  return Object.values(row)[0] as T
}
function pg(action: 'start' | 'stop' | 'promote', data: string, log: string) {
  const args = ['-D', data]
  if (action === 'start') args.push('-l', log, '-w', 'start')
  else if (action === 'stop') args.push('-m', 'immediate', '-w', 'stop')
  else args.push('promote', '-w')
  execFileSync(pgCtl, args, { stdio: 'ignore' })
}
function postmasterPid(data: string): number {
  const pid = Number(readFileSync(join(data, 'postmaster.pid'), 'utf8').split('\n', 1)[0])
  if (!Number.isSafeInteger(pid) || pid <= 0) throw new Error(`bad postmaster pid ${data}`)
  return pid
}
function alive(pid: number) { try { process.kill(pid, 0); return true } catch { return false } }
function assertNoListener(port: number, name: string) {
  try {
    const found = execFileSync('lsof', ['-tiTCP:' + port, '-sTCP:LISTEN'], { encoding: 'utf8' }).trim()
    if (found) throw new Error(`${name} listener survived on ${port}: ${found}`)
  } catch (error: any) { if (error.status !== 1) throw error }
}

function engineEnvironment(dsUrl: string, pgUrl: string, slot: string) {
  return {
    ELECTRIC_CIRCUITS_DS_URL: dsUrl,
    ELECTRIC_CIRCUITS_BIND: `127.0.0.1:${enginePort}`,
    ELECTRIC_CIRCUITS_LOG: 'warn',
    ELECTRIC_CIRCUITS_PG_URL: pgUrl,
    ELECTRIC_CIRCUITS_PG_TABLES: `public.${ITEM}`,
    ELECTRIC_CIRCUITS_PG_SLOT: slot,
    ELECTRIC_CIRCUITS_PG_POLL_MS: '25',
    ELECTRIC_STORAGE_DIR: join(root, 'engine-storage'),
  }
}
async function startEngine(spawnRawEngine: any, dsUrl: string, pgUrl: string, slot: string, gate: string) {
  const next = spawnRawEngine(engineEnvironment(dsUrl, pgUrl, slot))
  if (next.proc.pid) pids.add(next.proc.pid)
  const url = await next.waitForListening(TIMEOUT_MS).catch((error: Error) => { next.signal('SIGKILL'); throw error })
  engine = next
  await waitFor(gate, async () => (await fetch(`${url}/ready`)).status === 200)
  return url
}
async function stopEngine() {
  if (!engine) return
  const old = engine; engine = undefined
  old.signal('SIGTERM')
  try { await old.waitForExit(TIMEOUT_MS) } catch { old.signal('SIGKILL'); await old.waitForExit(TIMEOUT_MS) }
}

async function sourceTransaction(url: string, rows: Array<[number, string]>) {
  const client = new pgpkg.Client({ connectionString: url }); await client.connect()
  try {
    await client.query('BEGIN')
    for (const [id, title] of rows) {
      if (title === '__delete__') await client.query(`DELETE FROM ${ITEM} WHERE id = $1`, [id])
      else await client.query(`INSERT INTO ${ITEM} (id, title) VALUES ($1, $2) ON CONFLICT(id) DO UPDATE SET title = excluded.title`, [id, title])
    }
    const marker = Number((await client.query(`UPDATE ${MARKER} SET n = n + 1 WHERE id = 1 RETURNING n`)).rows[0].n)
    await client.query('COMMIT')
    return marker
  } catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error } finally { await client.end().catch(() => {}) }
}
async function awaitEngineReceipt(engineUrl: string, marker: number) {
  await waitFor(`engine receipt ${marker}`, async () => {
    const response = await fetch(`${engineUrl}/replication/lsn`)
    if (!response.ok) return false
    return Number((await response.json() as { sync: number }).sync) >= marker
  })
}
async function oracle(url: string) {
  return (await sql(url, `SELECT id, title FROM ${ITEM} ORDER BY id`)).rows.map((row: any) => ({ id: Number(row.id), title: String(row.title) }))
}

async function awaitChildWithin(child: ReturnType<typeof spawn>, label: string, deadlineMs: number): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    let done = false; let expired = false
    let timer: ReturnType<typeof setTimeout> | undefined
    const finish = (error?: Error) => { if (done) return; done = true; clearTimeout(timer); error ? reject(error) : resolve() }
    const exited = () => {
      if (child.exitCode === null && child.signalCode === null) return false
      child.exitCode === 0 ? finish() : finish(new Error(`${label} exited ${child.exitCode ?? child.signalCode}`))
      return true
    }
    if (exited()) return
    timer = setTimeout(() => { expired = true; child.kill('SIGTERM'); setTimeout(() => child.kill('SIGKILL'), 250).unref() }, deadlineMs)
    child.once('exit', code => expired ? finish(new Error(`${label} exceeded ${deadlineMs}ms`)) : code === 0 ? finish() : finish(new Error(`${label} exited ${code}`)))
    child.once('error', error => finish(error))
    exited()
  })
}
async function selfTestChildDeadline() {
  const hung = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' })
  let timedOut = false; try { await awaitChildWithin(hung, 'hung self-test', 50) } catch { timedOut = true }
  if (!timedOut || (hung.exitCode === null && hung.signalCode === null)) throw new Error('hung child was not reaped')
  const exited = spawn(process.execPath, ['-e', 'process.exit(0)'], { stdio: 'ignore' })
  await new Promise<void>((resolve, reject) => exited.once('exit', code => code === 0 ? resolve() : reject(new Error('pre-exited child failed'))))
  await awaitChildWithin(exited, 'already-exited self-test', 50)
  console.log('PASS child deadline self-test')
}
function selfTestPhaseOrder() {
  const phases = ['a', 'b']; const ledger = new PhaseLedger(phases)
  ledger.advance('a'); ledger.advance('b')
  if (!ledger.complete()) throw new Error('valid phase order did not complete')
  let rejected = false; try { new PhaseLedger(phases).advance('b') } catch { rejected = true }
  if (!rejected) throw new Error('invalid phase order was accepted')
  console.log('PASS causal phase-order self-test')
}
function qualificationStatus(scenario: boolean, cleanup: boolean) {
  return scenario && cleanup ? 'pass' : 'fail'
}
function selfTestCleanupStatus() {
  if (qualificationStatus(true, true) !== 'pass' || qualificationStatus(true, false) !== 'fail'
    || qualificationStatus(false, true) !== 'fail') {
    throw new Error('qualification status did not fail closed on cleanup')
  }
  console.log('PASS cleanup-status self-test')
}

async function main() {
  if (!existsSync(join(server, 'target/debug/electric-circuits-engine'))) throw new Error('prebuilt server engine is missing')
  const { postgres18Tools } = await import(join(server, 'scripts/postgres18.ts'))
  const { ensureServerBinary } = await import(join(server, 'packages/ds-rust/src/index.ts'))
  const { spawnRawEngine } = await import(join(server, 'packages/conformance/src/harness.ts'))
  const tools = postgres18Tools()
  const runtime = identity(tools, ensureServerBinary())
  if (!runtime.executables.postgres18.initdb.version.includes('18.4')) throw new Error(`PG18.4 required, got ${runtime.executables.postgres18.initdb.version}`)
  pgCtl = tools.pgCtl
  primaryPort = await freePort(); standbyPort = await freePort(); enginePort = await freePort()
  const nonce = `${process.pid}_${Date.now().toString(36)}`.toLowerCase()
  const databaseName = `swift_p1_${nonce}`
  const slot = `swift_p1_slot_${nonce}`
  const primaryURL = `postgres://postgres@127.0.0.1:${primaryPort}/${databaseName}`
  const standbyURL = `postgres://postgres@127.0.0.1:${standbyPort}/${databaseName}`
  execFileSync(tools.initdb, ['-D', primaryData, '-U', 'postgres', '--auth=trust', '--no-sync'])
  appendFileSync(join(primaryData, 'postgresql.conf'), `\nlisten_addresses = '127.0.0.1'\nport = ${primaryPort}\nwal_level = logical\nmax_wal_senders = 10\nmax_replication_slots = 10\n`)
  appendFileSync(join(primaryData, 'pg_hba.conf'), '\nhost replication all 127.0.0.1/32 trust\nhost all all 127.0.0.1/32 trust\n')
  pg('start', primaryData, join(root, 'primary.log')); pids.add(postmasterPid(primaryData))
  await sql(`postgres://postgres@127.0.0.1:${primaryPort}/postgres`, 'CREATE ROLE swift_p1_repl WITH REPLICATION LOGIN')
  await sql(`postgres://postgres@127.0.0.1:${primaryPort}/postgres`, `CREATE DATABASE ${databaseName}`)
  await sql(primaryURL, `CREATE TABLE ${ITEM} (id integer PRIMARY KEY, title text NOT NULL); CREATE TABLE ${MARKER} (id integer PRIMARY KEY, n bigint NOT NULL); INSERT INTO ${MARKER} (id, n) VALUES (1, 0);`)
  execFileSync(tools.pgBasebackup, ['-D', standbyData, '-h', '127.0.0.1', '-p', String(primaryPort), '-U', 'swift_p1_repl', '-R', '-X', 'stream', '--checkpoint=fast'])
  appendFileSync(join(standbyData, 'postgresql.conf'), `\nlisten_addresses = '127.0.0.1'\nport = ${standbyPort}\nhot_standby = on\n`)
  pg('start', standbyData, join(root, 'standby.log')); pids.add(postmasterPid(standbyData))
  ds = new DurableStreamTestServer({ port: 0, dataDir: join(root, 'durable-streams') })
  const dsURL = await ds.start(); dsPort = Number(new URL(dsURL).port); if (ds.pid) pids.add(ds.pid)
  const stableEngineURL = await startEngine(spawnRawEngine, dsURL, primaryURL, slot, 'primary engine ready')
  const expected = { baseline: [{ id: 1, title: 'before' }, { id: 2, title: 'delete-me' }], promoted: [{ id: 1, title: 'after' }, { id: 3, title: 'promoted' }] }
  writeFileSync(join(phase, 'expected.json'), JSON.stringify(expected))
  writeFileSync(join(root, 'manifest.json'), JSON.stringify({ scenario: 'SWF-P1-1', profile: 'PG18_4_UNSYNCHRONIZED_SLOT', runtime, stableEngineURL, ports: { primaryPort, standbyPort, enginePort, dsPort }, slot }, null, 2))
  const scratch = mkdtempSync(join(tmpdir(), 'electric-circuits-swift-pg18-failover-build-'))
  swift = spawn('swift', ['run', '--scratch-path', scratch, 'LinearLitePG18Failover'], {
    cwd: join(candidate, 'Examples/LinearLite'),
    env: { ...process.env, ECS_PG18_FAILOVER_BASE_URL: stableEngineURL, ECS_PG18_FAILOVER_DIR: phase, ECS_PG18_FAILOVER_DATABASE: databasePath },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  swift.stdout?.on('data', data => { swiftLog += data; writeFileSync(join(root, 'swift.log'), swiftLog) })
  swift.stderr?.on('data', data => { swiftLog += data; writeFileSync(join(root, 'swift.log'), swiftLog) })
  const phases = new PhaseLedger(['baseline_source_commit', 'baseline_server_receipt', 'baseline_client_receipt', 'standby_caught_up', 'primary_isolated', 'standby_promoted', 'engine_repointed', 'old_terminal', 'promoted_source_commit', 'promoted_server_receipt', 'recovery_complete'])
  const baselineMarker = await sourceTransaction(primaryURL, [[1, 'before'], [2, 'delete-me']]); phases.advance('baseline_source_commit')
  await awaitEngineReceipt(stableEngineURL, baselineMarker); phases.advance('baseline_server_receipt')
  await waitFor('Swift baseline receipt', () => existsSync(join(phase, 'swift-baseline-ready')), 180_000); phases.advance('baseline_client_receipt')
  const targetLSN = await scalar<string>(primaryURL, 'SELECT pg_current_wal_lsn()::text')
  await waitFor('standby catches baseline', () => scalar<boolean>(standbyURL, 'SELECT pg_is_in_recovery() AND pg_wal_lsn_diff(pg_last_wal_replay_lsn(), $1::pg_lsn) >= 0', [targetLSN])); phases.advance('standby_caught_up')
  pg('stop', primaryData, join(root, 'primary.log')); phases.advance('primary_isolated')
  await stopEngine()
  pg('promote', standbyData, join(root, 'standby.log'))
  await waitFor('standby promotion', async () => !(await scalar<boolean>(standbyURL, 'SELECT pg_is_in_recovery()'))); phases.advance('standby_promoted')
  const absent = await scalar<number>(standbyURL, 'SELECT count(*)::int FROM pg_replication_slots WHERE slot_name = $1', [slot])
  if (absent !== 0 || await scalar<string>(standbyURL, 'SHOW sync_replication_slots') !== 'off') throw new Error('fixture unexpectedly has a synchronized logical failover slot')
  await startEngine(spawnRawEngine, dsURL, standbyURL, slot, 'promoted engine ready')
  writeFileSync(join(phase, 'engine-repointed'), ''); phases.advance('engine_repointed')
  await waitFor('Swift old terminal', () => existsSync(join(phase, 'old-terminal'))); phases.advance('old_terminal')
  const promotedMarker = await sourceTransaction(standbyURL, [[1, 'after'], [2, '__delete__'], [3, 'promoted']]); phases.advance('promoted_source_commit')
  await awaitEngineReceipt(stableEngineURL, promotedMarker); phases.advance('promoted_server_receipt')
  const promotedOracle = await oracle(standbyURL)
  if (JSON.stringify(promotedOracle) !== JSON.stringify(expected.promoted)) throw new Error(`independent promoted SQL oracle mismatch ${JSON.stringify(promotedOracle)}`)
  writeFileSync(join(phase, 'promoted-mutation-drained'), JSON.stringify({ sourceCommitID: promotedMarker, oracle: promotedOracle }))
  if (!swift) throw new Error('Swift child missing')
  await awaitChildWithin(swift, 'Swift PG18 failover client', 180_000).catch(error => { throw new Error(`${error.message}\n${swiftLog}`) })
  const result = JSON.parse(readFileSync(join(phase, 'swift-result.json'), 'utf8'))
  if (JSON.stringify(result.recovered) !== JSON.stringify(promotedOracle) || JSON.stringify(result.reopened) !== JSON.stringify(promotedOracle)) throw new Error('Swift GRDB recovery differs from promoted SQL oracle')
  if (JSON.stringify(result.oldAfterPromotion) !== JSON.stringify(expected.baseline)) throw new Error('old reader accepted promoted rows')
  const visible = result.visibleObservations
  if (!Array.isArray(visible) || visible.length !== 2
    || visible[0].stage !== 'old_complete_before_activation' || visible[0].scope !== result.oldScope
    || JSON.stringify(visible[0].rows) !== JSON.stringify(expected.baseline)
    || visible[1].stage !== 'new_complete_after_activation' || visible[1].scope !== result.newScope
    || JSON.stringify(visible[1].rows) !== JSON.stringify(promotedOracle)) {
    throw new Error('visible scope transition admitted a mixed-generation GRDB view')
  }
  if (result.oldHandle === result.newHandle || result.oldSubscription === result.newSubscription || result.oldScope === result.newScope) throw new Error('recovery generation identity was reused')
  if (!result.headers.control || !result.headers.stream || result.headers.successfulReleases < 2) throw new Error('custom-header propagation or named release evidence missing')
  phases.advance('recovery_complete')
  if (!phases.complete()) throw new Error(`incomplete phase order ${phases.values.join(',')}`)
  writeFileSync(join(root, 'result.json'), JSON.stringify({ phases: phases.values, baselineMarker, promotedMarker, oracle: promotedOracle, swift: result }, null, 2))
  scenarioCompleted = true
  console.log(`PASS SWF-P1-1 root=${root}`)
}

async function cleanup() {
  swift?.kill('SIGKILL')
  await stopEngine().catch(() => {})
  try { if (pgCtl) pg('stop', standbyData, join(root, 'standby.log')) } catch {}
  try { if (pgCtl) pg('stop', primaryData, join(root, 'primary.log')) } catch {}
  await ds?.stop().catch(() => {})
  if (enginePort) assertNoListener(enginePort, 'engine')
  if (dsPort) assertNoListener(dsPort, 'durable-streams')
  if (primaryPort) assertNoListener(primaryPort, 'primary postgres')
  if (standbyPort) assertNoListener(standbyPort, 'standby postgres')
  const survivors = [...pids].filter(alive)
  if (survivors.length) throw new Error(`owned children survived cleanup: ${survivors.join(',')}`)
  rmSync(root, { recursive: true, force: true })
  if (existsSync(root)) throw new Error('owned fixture root survived cleanup')
  cleanupCompleted = true
}

async function entry() {
  if (process.argv.includes('--self-test-phase-order')) { try { selfTestPhaseOrder() } finally { rmSync(root, { recursive: true, force: true }) }; return }
  if (process.argv.includes('--self-test-child-deadline')) { try { await selfTestChildDeadline() } finally { rmSync(root, { recursive: true, force: true }) }; return }
  if (process.argv.includes('--self-test-cleanup-status')) { try { selfTestCleanupStatus() } finally { rmSync(root, { recursive: true, force: true }) }; return }
  try { await main() } catch (error) { failure = String(error); console.error(error); process.exitCode = 1 }
  // Preserve only content-addressed identity and public-contract observations; all writable
  // process/data roots remain private to `root` and are removed by cleanup below.
  const manifest = existsSync(join(root, 'manifest.json')) ? JSON.parse(readFileSync(join(root, 'manifest.json'), 'utf8')) : undefined
  const result = existsSync(join(root, 'result.json')) ? JSON.parse(readFileSync(join(root, 'result.json'), 'utf8')) : undefined
  try { await cleanup() } catch (error) { failure = `${failure ?? ''}\ncleanup: ${String(error)}`; console.error(error); process.exitCode = 1 }
  writeFileSync(join(retainedEvidence, 'pg18-failover-last-run.json'), JSON.stringify({ status: qualificationStatus(scenarioCompleted, cleanupCompleted), failure, manifest, result, swiftLog }, null, 2))
}

void entry()
