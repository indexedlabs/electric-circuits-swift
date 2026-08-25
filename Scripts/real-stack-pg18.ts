#!/usr/bin/env node
// Explicit Layer-B qualification runner. It reuses the server's real PG/replication harness but
// never edits that checkout; this file owns the PostgreSQL root, DB/slot, DS WAL root, engine child,
// Swift store, process group, and evidence directory for one run.
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
const runRoot = mkdtempSync(join(tmpdir(), 'electric-circuits-swift-real-stack-v1-'))
const pgData = join(runRoot, 'postgres')
const pgLog = join(runRoot, 'postgres.log')
const evidence = join(runRoot, 'evidence')
const phase = join(runRoot, 'phase')
mkdirSync(evidence)
mkdirSync(phase)
const nonce = `${process.pid}_${Date.now().toString(36)}`
let pgCtl = ''
let pgPort = 0
let h: any
let swift: ReturnType<typeof spawn> | undefined
let passed = false
let failure: string | undefined

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
function digestRuntimeInputs(root: string, inputs: string[]): Array<{ path: string, mode: string, sha256: string }> {
  const walk = (relative: string): Array<{ path: string, mode: string, sha256: string }> => {
    const absolute = join(root, relative)
    const stat = lstatSync(absolute)
    if (stat.isFile()) return [{ path: relative, mode: (stat.mode & 0o777).toString(8), sha256: sha256(absolute) }]
    if (!stat.isDirectory()) return []
    return readdirSync(absolute, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name)).flatMap(entry =>
      walk(join(relative, entry.name)))
  }
  return inputs.slice().sort().flatMap(walk)
}
function runtimeIdentity(tools: { initdb: string, pgCtl: string }, dsBinary: string) {
  const status = command('git', ['status', '--porcelain=v1', '--untracked-files=all'])
  if (status && process.env.ECS_ALLOW_DIRTY_SERVER !== '1') {
    throw new Error('server checkout is dirty; use a clean checkout or set ECS_ALLOW_DIRTY_SERVER=1 to retain a complete dirty-overlay manifest')
  }
  const tree = digestTree(server)
  const manifest = JSON.stringify(tree)
  const candidateTree = digestRuntimeInputs(candidate, ['Package.swift', 'Scripts', 'Sources'])
  const candidateManifest = JSON.stringify(candidateTree)
  return {
    server: {
      root: server, head: command('git', ['rev-parse', 'HEAD']), tree: command('git', ['rev-parse', 'HEAD^{tree}']),
      statusPorcelainV1: status, dirtyOverlaySha256: createHash('sha256').update(status).digest('hex'),
      completeRuntimeSourceManifest: tree, completeRuntimeSourceManifestSha256: createHash('sha256').update(manifest).digest('hex'),
    },
    swiftCandidate: {
      root: candidate, completeRuntimeInputManifest: candidateTree,
      completeRuntimeInputManifestSha256: createHash('sha256').update(candidateManifest).digest('hex'),
    },
    runtimeExecutables: {
      engine: { path: join(server, 'target/debug/electric-circuits-engine'), sha256: sha256(join(server, 'target/debug/electric-circuits-engine')) },
      durableStreams: { path: dsBinary, sha256: sha256(dsBinary) },
      postgres18: {
        initdb: { path: tools.initdb, version: command(tools.initdb, ['--version'], candidate), sha256: sha256(tools.initdb) },
        pgCtl: { path: tools.pgCtl, version: command(tools.pgCtl, ['--version'], candidate), sha256: sha256(tools.pgCtl) },
      },
    },
    toolchain: { node: process.version, pnpm: command('pnpm', ['--version'], candidate) },
  }
}
async function awaitChildWithin(child: ReturnType<typeof spawn>, label: string, deadlineMs: number): Promise<void> {
  if (child.exitCode !== null) {
    if (child.exitCode === 0) return
    throw new Error(`${label} exited ${child.exitCode}`)
  }
  await new Promise<void>((resolve, reject) => {
    let done = false
    let deadlineExceeded = false
    const finish = (error?: Error) => { if (done) return; done = true; clearTimeout(timer); error ? reject(error) : resolve() }
    const timer = setTimeout(() => {
      deadlineExceeded = true
      child.kill('SIGTERM')
      setTimeout(() => child.kill('SIGKILL'), 250).unref()
    }, deadlineMs)
    child.once('exit', code => deadlineExceeded
      ? finish(new Error(`${label} exceeded ${deadlineMs}ms; terminated child pid=${child.pid}`))
      : code === 0 ? finish() : finish(new Error(`${label} exited ${code}`)))
    child.once('error', error => finish(error))
  })
}
async function selfTestChildDeadline() {
  const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' })
  let timedOut = false
  try { await awaitChildWithin(child, 'self-test hung child', 50) } catch { timedOut = true }
  if (!timedOut || (child.exitCode === null && child.signalCode === null)) {
    throw new Error('self-test did not timeout and reap hung child')
  }
  console.log('PASS child-deadline self-test')
}
function selfTestIdentity() {
  const root = mkdtempSync(join(tmpdir(), 'electric-circuits-swift-identity-'))
  try {
    writeFileSync(join(root, 'input.ts'), 'export const value = 1\n')
    const first = createHash('sha256').update(JSON.stringify(digestTree(root))).digest('hex')
    writeFileSync(join(root, 'input.ts'), 'export const value = 2\n')
    const second = createHash('sha256').update(JSON.stringify(digestTree(root))).digest('hex')
    if (first === second) throw new Error('identity manifest did not change after a runtime input changed')
    console.log('PASS runtime-identity self-test')
  } finally { rmSync(root, { recursive: true, force: true }) }
}
async function freePort(): Promise<number> {
  const net = await import('node:net')
  return await new Promise((resolve, reject) => {
    const s = net.createServer().once('error', reject).listen(0, '127.0.0.1', () => {
      const a = s.address(); s.close(() => typeof a === 'object' && a ? resolve(a.port) : reject(new Error('no port')))
    })
  })
}
async function waitFor(name: string, predicate: () => Promise<boolean> | boolean): Promise<void> {
  const deadline = Date.now() + 30_000
  while (Date.now() < deadline) { if (await predicate()) return; await sleep(25) }
  throw new Error(`deadline waiting for ${name}; root=${runRoot}`)
}
function portOf(url: string): number { return Number(new URL(url).port) }
function assertNoListener(port: number, label: string) {
  try {
    const pids = execFileSync('lsof', ['-tiTCP:' + port, '-sTCP:LISTEN'], { encoding: 'utf8' }).trim()
    if (pids) throw new Error(`${label} listener survived on ${port}: ${pids}`)
  } catch (e: any) { if (e.status === 1) return; throw e }
}
async function sql(url: string, query: string, values: unknown[] = []) {
  const c = new pg.Client({ connectionString: url }); await c.connect()
  try { return await c.query(query, values) } finally { await c.end() }
}

async function main() {
  if (!existsSync(join(server, 'target/debug/electric-circuits-engine'))) throw new Error('prebuilt engine missing; build the server candidate before qualification')
  const { postgres18Tools } = await import(join(server, 'scripts/postgres18.ts'))
  const { bootHarness, drainEngine } = await import(join(server, 'packages/conformance/src/harness.ts'))
  const { ensureServerBinary } = await import(join(server, 'packages/ds-rust/src/index.ts'))
  const tools = postgres18Tools()
  const identity = runtimeIdentity(tools, ensureServerBinary())
  pgCtl = tools.pgCtl
  pgPort = await freePort()
  execFileSync(tools.initdb, ['-D', pgData, '-U', 'postgres', '--auth=trust', '--no-sync'])
  appendFileSync(join(pgData, 'postgresql.conf'), `\nlisten_addresses = '127.0.0.1'\nport = ${pgPort}\nwal_level = logical\nmax_replication_slots = 20\nmax_wal_senders = 20\n`)
  execFileSync(pgCtl, ['-D', pgData, '-l', pgLog, '-w', 'start'])
  const admin = `postgres://postgres@127.0.0.1:${pgPort}/postgres`
  process.env.ELECTRIC_CIRCUITS_TEST_PG_URL = admin
  process.env.ELECTRIC_CIRCUITS_ENGINE_PREBUILT = '1'
  const schema = { tables: {
    items: { columns: { id: { type: 'int' }, title: { type: 'text' } }, primaryKey: 'id' },
    causal_markers: { columns: { id: { type: 'int' }, marker: { type: 'int' } }, primaryKey: 'id' },
  } }
  h = await bootHarness(schema, { ddl: `
    CREATE TABLE items (id integer PRIMARY KEY, title text NOT NULL);
    ALTER TABLE items REPLICA IDENTITY FULL;
    CREATE TABLE causal_markers (id integer PRIMARY KEY, marker bigint NOT NULL);
    ALTER TABLE causal_markers REPLICA IDENTITY FULL;
    INSERT INTO causal_markers (id, marker) VALUES (1, 0);` })
  const pgClient = new pg.Client({ connectionString: h.pgUrl }); await pgClient.connect()
  try {
    await pgClient.query('BEGIN')
    await pgClient.query("INSERT INTO items (id, title) VALUES (1, 'before'), (2, 'delete-me')")
    const baseline = Number((await pgClient.query('UPDATE causal_markers SET marker = marker + 1 WHERE id = 1 RETURNING marker')).rows[0].marker)
    await pgClient.query('COMMIT')
    await drainEngine(h)
    writeFileSync(join(phase, 'baseline-source.json'), JSON.stringify({ marker: baseline }))
  } finally { await pgClient.end() }
  writeFileSync(join(runRoot, 'manifest.json'), JSON.stringify({
    scenario: 'SWF-P0-7-v2', profile: 'NATIVE_CORE_LAYER_B', identity,
    swiftToolchain: execFileSync('swift', ['--version'], { encoding: 'utf8' }).split('\n')[0],
    endpoints: { postgresPort: pgPort, engine: h.engineUrl, durableStreams: h.dsUrl }, namespace: nonce,
  }, null, 2))
  swift = spawn('swift', ['run', 'ElectricCircuitsSwiftRealStack'], { cwd: candidate, env: { ...process.env, ECS_REAL_STACK_BASE_URL: h.engineUrl, ECS_REAL_STACK_DIR: phase }, stdio: ['ignore', 'pipe', 'pipe'] })
  let swiftLog = ''; swift.stdout?.on('data', d => { swiftLog += d; appendFileSync(join(runRoot, 'swift.log'), d) }); swift.stderr?.on('data', d => { swiftLog += d; appendFileSync(join(runRoot, 'swift.log'), d) })
  await waitFor('Swift baseline receipt', () => {
    if (swift?.exitCode !== null && swift?.exitCode !== undefined) throw new Error(`Swift runner exited ${swift.exitCode}\\n${swiftLog}`)
    return existsSync(join(phase, 'baseline-ready'))
  })
  const writer = new pg.Client({ connectionString: h.pgUrl }); await writer.connect()
  let marker: number
  try {
    await writer.query('BEGIN')
    await writer.query("INSERT INTO items (id, title) VALUES (3, 'created')")
    await writer.query("UPDATE items SET title = 'after' WHERE id = 1")
    await writer.query('DELETE FROM items WHERE id = 2')
    marker = Number((await writer.query('UPDATE causal_markers SET marker = marker + 1 WHERE id = 1 RETURNING marker')).rows[0].marker)
    await writer.query('COMMIT')
  } catch (error) { await writer.query('ROLLBACK').catch(() => {}); throw error } finally { await writer.end() }
  writeFileSync(join(phase, 'mutation-committed'), JSON.stringify({ marker }))
  // Existing bootHarness establishes a named source->engine drain after the same-transaction marker.
  await drainEngine(h)
  writeFileSync(join(phase, 'server-drained'), JSON.stringify({ marker, receipt: 'harness drainEngine after marker' }))
  const childDeadlineMs = Number(process.env.ECS_REAL_STACK_CHILD_DEADLINE_MS ?? '20000')
  await awaitChildWithin(swift!, 'post-drain Swift sequence', childDeadlineMs).catch(error => {
    throw new Error(`${String(error)}\n${swiftLog}`)
  })
  const swiftResult = JSON.parse(readFileSync(join(phase, 'swift-result.json'), 'utf8'))
  const oracle = (await sql(h.pgUrl, 'SELECT id, title FROM items ORDER BY id')).rows
  const actual = Object.entries(swiftResult.rows).map(([key, row]: any) => ({ id: Number(key), title: row.title.string ?? row.title })).sort((a, b) => a.id - b.id)
  if (JSON.stringify(actual) !== JSON.stringify(oracle)) throw new Error(`SQL oracle mismatch expected=${JSON.stringify(oracle)} actual=${JSON.stringify(actual)}`)
  if (!swiftResult.cursor?.offset || !swiftResult.controlHeaderInjected || !swiftResult.streamHeaderInjected) throw new Error('missing durable cursor or header injection proof')
  if (swiftResult.replayRequestedAt !== swiftResult.preTerminalCursor?.offset) throw new Error('replay did not request from the saved pre-terminal cursor')
  if (swiftResult.preTerminalCursor.offset === swiftResult.cursor.offset) throw new Error('pre-terminal and final cursor unexpectedly match')
  if (!(swiftResult.replayAppliedBatches > 0) || !(swiftResult.replayAppliedEvents > 0)) throw new Error('replay did not apply a committed mutation batch')
  writeFileSync(join(runRoot, 'result.json'), JSON.stringify({
    marker, oracle, actual, finalCursor: swiftResult.cursor, preTerminalCursor: swiftResult.preTerminalCursor,
    replayRequestedAt: swiftResult.replayRequestedAt,
    replayAppliedBatches: swiftResult.replayAppliedBatches, replayAppliedEvents: swiftResult.replayAppliedEvents,
    headers: { control: true, stream: true },
  }, null, 2))
  passed = true
  console.log(`PASS SWF-P0-7-v2 root=${runRoot}`)
}

async function runQualification() { await main().catch(error => {
  failure = String(error)
  console.error(error)
  process.exitCode = 1
}).finally(async () => {
  swift?.kill('SIGKILL')
  const enginePort = h?.engineUrl ? portOf(h.engineUrl) : undefined
  const dsPort = h?.dsUrl ? portOf(h.dsUrl) : undefined
  await h?.shutdown().catch(() => {})
  if (pgCtl) try { execFileSync(pgCtl, ['-D', pgData, '-m', 'immediate', '-w', 'stop']) } catch {}
  if (enginePort) await sleep(100).then(() => assertNoListener(enginePort, 'engine'))
  if (dsPort) assertNoListener(dsPort, 'durable-streams')
  if (pgPort) assertNoListener(pgPort, 'postgres')
  // Keep redacted evidence/results, but remove the only writable PG/DS roots and prove the cleanup above.
  rmSync(pgData, { recursive: true, force: true })
  writeFileSync(join(runRoot, 'cleanup.json'), JSON.stringify({ engine: 'gone', durableStreams: 'gone', postgres: 'gone', pgDataRemoved: !existsSync(pgData) }))
  const record = {
    status: passed ? 'pass' : 'fail',
    failure,
    manifest: existsSync(join(runRoot, 'manifest.json')) ? JSON.parse(readFileSync(join(runRoot, 'manifest.json'), 'utf8')) : undefined,
    result: existsSync(join(runRoot, 'result.json')) ? JSON.parse(readFileSync(join(runRoot, 'result.json'), 'utf8')) : undefined,
    cleanup: JSON.parse(readFileSync(join(runRoot, 'cleanup.json'), 'utf8')),
  }
  const evidenceName = process.env.ECS_REAL_STACK_EVIDENCE_NAME ?? 'real-stack-pg18-last-run.json'
  writeFileSync(join(retainedEvidence, evidenceName), JSON.stringify(record, null, 2))
  rmSync(runRoot, { recursive: true, force: true })
}) }

async function entry() {
  if (process.argv.includes('--self-test-child-deadline')) {
    try { await selfTestChildDeadline() } finally { rmSync(runRoot, { recursive: true, force: true }) }
    return
  }
  if (process.argv.includes('--self-test-runtime-identity')) {
    try { selfTestIdentity() } finally { rmSync(runRoot, { recursive: true, force: true }) }
    return
  }
  await runQualification()
}
void entry()
