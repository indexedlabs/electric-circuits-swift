#!/usr/bin/env node
// Real PG18.4 / Axum / durable-stream qualification for two concurrent filtered recent windows.
import { execFileSync, spawn } from 'node:child_process'
import { createHash } from 'node:crypto'
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import pg from 'pg'

const server = process.env.ELECTRIC_CIRCUITS_SERVER_ROOT ?? '/Users/bozilabs/labs/electric-circuits'
const candidate = dirname(dirname(fileURLToPath(import.meta.url)))
const evidence = join(candidate, 'Evidence')
const root = mkdtempSync(join(tmpdir(), 'electric-circuits-swift-filtered-windows-v1-'))
const phase = join(root, 'phase')
const pgData = join(root, 'postgres')
const pgLog = join(root, 'postgres.log')
const sqlite = join(root, 'linearlite.sqlite')
mkdirSync(phase)

let h: any; let swift: ReturnType<typeof spawn> | undefined; let pgCtl = ''; let pgPort = 0; let swiftLog = ''; let scenarioPassed = false; let failure = ''
const scratchRoots: string[] = []
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))
const command = (bin: string, args: string[], cwd = candidate) => execFileSync(bin, args, { cwd, encoding: 'utf8' }).trim()
const sha256 = (path: string) => createHash('sha256').update(readFileSync(path)).digest('hex')

async function waitFor(name: string, predicate: () => Promise<boolean> | boolean, timeout = 45_000) {
  const deadline = Date.now() + timeout
  while (Date.now() < deadline) {
    if (await predicate()) return
    await sleep(25)
  }
  throw new Error(`deadline waiting for ${name}; root=${root}`)
}
async function waitForFile(name: string) { await waitFor(name, () => existsSync(join(phase, name))) }
async function freePort(): Promise<number> {
  const net = await import('node:net')
  return await new Promise((resolve, reject) => {
    const listener = net.createServer().once('error', reject).listen(0, '127.0.0.1', () => {
      const address = listener.address(); listener.close(() => typeof address === 'object' && address ? resolve(address.port) : reject(new Error('no port')))
    })
  })
}
async function sql(url: string, text: string, values: unknown[] = []) {
  const client = new pg.Client({ connectionString: url }); await client.connect()
  try { return await client.query(text, values) } finally { await client.end() }
}
async function sourceTransaction(name: string, statements: Array<{ text: string, values?: unknown[] }>, pgUrl: string, drain: (harness: any) => Promise<void>) {
  const client = new pg.Client({ connectionString: pgUrl }); await client.connect(); let marker = 0
  try {
    await client.query('BEGIN')
    for (const statement of statements) await client.query(statement.text, statement.values)
    marker = Number((await client.query('UPDATE causal_markers SET marker = marker + 1 WHERE id = 1 RETURNING marker')).rows[0].marker)
    await client.query('COMMIT')
  } catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error } finally { await client.end() }
  await drain(h)
  writeFileSync(join(phase, `${name}-source-marker.json`), JSON.stringify({ sourceCommitID: marker, sameTransaction: true }, null, 2))
  writeFileSync(join(phase, `${name}-server-drained`), JSON.stringify({ sourceCommitID: marker, receipt: 'drainEngine after same-transaction marker' }, null, 2))
  return marker
}
type Row = { id: number, modified: number, username: string, title: string }
const sameRows = (left: Row[], right: Row[]) => JSON.stringify(left.map(row => [row.id, row.modified, row.username, row.title])) === JSON.stringify(right.map(row => [row.id, row.modified, row.username, row.title]))
async function oracle(pgUrl: string, username: string): Promise<Row[]> {
  return (await sql(pgUrl, 'SELECT id, modified, username, title FROM issues WHERE username = $1 ORDER BY modified DESC, id DESC LIMIT 10', [username])).rows
    .map((r: any) => ({ id: Number(r.id), modified: Number(r.modified), username: String(r.username), title: String(r.title) }))
}
function assertWindow(name: string, expected: Row[], observed: any) {
  if (!sameRows(observed.rows, expected) || !sameRows(observed.sessionRows, expected)) {
    throw new Error(`${name}: independent SQL window mismatch`)
  }
  if (expected.length !== 10 || observed.membershipCount !== 10 || new Set(observed.rows.map((row: Row) => row.id)).size !== 10 || !observed.cursor?.offset) {
    throw new Error(`${name}: not exactly ten unique rows with a durable isolated cursor`)
  }
}
function assertObservation(name: string, expectedA: Row[], expectedB: Row[], observed: any) {
  assertWindow(`${name}/A`, expectedA, observed.a); assertWindow(`${name}/B`, expectedB, observed.b)
  const ids = [...new Set([...expectedA, ...expectedB].map(row => row.id))].sort((a, b) => a - b)
  if (observed.canonicalIssueCount !== ids.length || JSON.stringify(observed.canonicalIssueIDs) !== JSON.stringify(ids)) {
    throw new Error(`${name}: canonical issue rows are not exactly the union of isolated view memberships`)
  }
}
function assertChildAlive(stage: string) {
  if (swift?.exitCode !== null && swift?.exitCode !== undefined) throw new Error(`Swift child exited before ${stage}: ${swift.exitCode}\n${swiftLog}`)
}
function qualificationStatus(scenario: boolean, cleanup: { swiftReaped: boolean, listenersGone: boolean, pgDataRemoved: boolean, scratchClean: boolean, rootRemoved: boolean }) {
  return scenario && cleanup.swiftReaped && cleanup.listenersGone && cleanup.pgDataRemoved && cleanup.scratchClean && cleanup.rootRemoved ? 'pass' : 'fail'
}
function assertNoListener(port: number, label: string) {
  try { if (execFileSync('lsof', ['-tiTCP:' + port, '-sTCP:LISTEN'], { encoding: 'utf8' }).trim()) throw new Error(`${label} listener survived`) }
  catch (error: any) { if (error.status !== 1) throw error }
}
async function awaitChildWithin(child: ReturnType<typeof spawn>, label: string, timeout: number) {
  await new Promise<void>((resolve, reject) => {
    let done = false; let expired = false; let timer: ReturnType<typeof setTimeout> | undefined
    const finish = (error?: Error) => { if (done) return; done = true; if (timer) clearTimeout(timer); error ? reject(error) : resolve() }
    const terminal = () => {
      if (child.exitCode === null && child.signalCode === null) return false
      if (child.exitCode === 0) finish(); else finish(new Error(`${label} exit=${child.exitCode ?? child.signalCode}`))
      return true
    }
    if (terminal()) return
    timer = setTimeout(() => {
      expired = true; child.kill('SIGTERM'); setTimeout(() => child.kill('SIGKILL'), 250).unref()
    }, timeout)
    child.once('exit', code => expired ? finish(new Error(`${label} exceeded ${timeout}ms`)) : code === 0 ? finish() : finish(new Error(`${label} exit=${code ?? child.signalCode}`)))
    child.once('error', error => finish(error))
    terminal()
  })
}
async function reapChild(child: ReturnType<typeof spawn> | undefined) {
  if (!child || (child.exitCode !== null || child.signalCode !== null)) return true
  child.kill('SIGTERM')
  try { await awaitChildWithin(child, 'cleanup Swift child', 1_000); return true }
  catch { return child.exitCode !== null || child.signalCode !== null }
}
function selfTestOracle() {
  const rows = [{ id: 1, modified: 2, username: 'ada', title: 'one' }]
  let rejected = false
  try { assertWindow('self', rows, { rows, sessionRows: rows, membershipCount: 2, cursor: { offset: '1' } }) } catch { rejected = true }
  if (!rejected) throw new Error('self test accepted duplicate membership')
  rejected = false
  try { assertObservation('self', rows, rows, { a: { rows, sessionRows: rows, membershipCount: 1, cursor: { offset: '1' } }, b: { rows, sessionRows: rows, membershipCount: 1, cursor: { offset: '1' } }, canonicalIssueCount: 2, canonicalIssueIDs: [1, 1] }) } catch { rejected = true }
  if (!rejected) throw new Error('self test accepted duplicated canonical rows')
  console.log('PASS filtered-window oracle self-test')
}
function selfTestCleanupStatus() {
  const clean = { swiftReaped: true, listenersGone: true, pgDataRemoved: true, scratchClean: true, rootRemoved: true }
  if (qualificationStatus(true, clean) !== 'pass') throw new Error('cleanup status rejected complete cleanup')
  if (qualificationStatus(true, { ...clean, rootRemoved: false }) !== 'fail') throw new Error('cleanup status accepted surviving run root')
  if (qualificationStatus(true, { ...clean, pgDataRemoved: false }) !== 'fail') throw new Error('cleanup status accepted surviving PostgreSQL data root')
  if (qualificationStatus(true, { ...clean, scratchClean: false }) !== 'fail') throw new Error('cleanup status accepted a nonempty Swift scratch root')
  if (qualificationStatus(true, { ...clean, swiftReaped: false }) !== 'fail') throw new Error('cleanup status accepted a live Swift child')
  console.log('PASS filtered-window cleanup-status self-test')
}
async function selfTestChildBoundary() {
  const preExited = spawn(process.execPath, ['-e', 'process.exit(0)'], { stdio: 'ignore' })
  await new Promise<void>((resolve, reject) => preExited.once('exit', code => code === 0 ? resolve() : reject(new Error('pre-exited child failed'))))
  await awaitChildWithin(preExited, 'pre-exited self-test child', 50)
  const hung = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' })
  let rejected = false
  try { await awaitChildWithin(hung, 'hung self-test child', 50) } catch { rejected = true }
  if (!rejected || (hung.exitCode === null && hung.signalCode === null)) throw new Error('child deadline did not reap the hung child')
  console.log('PASS filtered-window child-boundary self-test')
}

async function main() {
  if (!existsSync(join(server, 'target/debug/electric-circuits-engine'))) throw new Error('prebuilt engine missing')
  const { postgres18Tools } = await import(join(server, 'scripts/postgres18.ts'))
  const { bootHarness, drainEngine } = await import(join(server, 'packages/conformance/src/harness.ts'))
  const { ensureServerBinary } = await import(join(server, 'packages/ds-rust/src/index.ts'))
  const tools = postgres18Tools(); pgCtl = tools.pgCtl
  if (!command(tools.initdb, ['--version']).includes('18.4')) throw new Error(`expected PostgreSQL 18.4, got ${command(tools.initdb, ['--version'])}`)
  pgPort = await freePort()
  execFileSync(tools.initdb, ['-D', pgData, '-U', 'postgres', '--auth=trust', '--no-sync'])
  writeFileSync(join(pgData, 'postgresql.conf'), `\nlisten_addresses = '127.0.0.1'\nport = ${pgPort}\nwal_level = logical\nmax_replication_slots = 20\nmax_wal_senders = 20\n`, { flag: 'a' })
  execFileSync(pgCtl, ['-D', pgData, '-l', pgLog, '-w', 'start'])
  process.env.ELECTRIC_CIRCUITS_TEST_PG_URL = `postgres://postgres@127.0.0.1:${pgPort}/postgres`
  process.env.ELECTRIC_CIRCUITS_ENGINE_PREBUILT = '1'
  const schema = { tables: { issues: { columns: { id: { type: 'int' }, client_id: { type: 'text' }, title: { type: 'text' }, description: { type: 'text' }, status: { type: 'text' }, priority: { type: 'text' }, username: { type: 'text' }, project_id: { type: 'int' }, created: { type: 'int' }, modified: { type: 'int' }, kanbanorder: { type: 'float' } }, primaryKey: 'id' }, causal_markers: { columns: { id: { type: 'int' }, marker: { type: 'int' } }, primaryKey: 'id' } } }
  h = await bootHarness(schema, { ddl: `
    CREATE TABLE issues (id integer PRIMARY KEY, client_id text, title text NOT NULL, description text NOT NULL, status text NOT NULL, priority text NOT NULL, username text NOT NULL, project_id integer NOT NULL, created bigint NOT NULL, modified bigint NOT NULL, kanbanorder double precision NOT NULL);
    ALTER TABLE issues REPLICA IDENTITY FULL; CREATE TABLE causal_markers (id integer PRIMARY KEY, marker bigint NOT NULL); ALTER TABLE causal_markers REPLICA IDENTITY FULL; INSERT INTO causal_markers VALUES (1, 0);` })
  const seeded = [...Array.from({ length: 12 }, (_, index) => [index + 1, 'ada']), ...Array.from({ length: 12 }, (_, index) => [index + 101, 'bob'])]
  await sourceTransaction('seed', [{ text: `INSERT INTO issues (id, client_id, title, description, status, priority, username, project_id, created, modified, kanbanorder) VALUES ${seeded.map((_, i) => `($${i * 7 + 1}, $${i * 7 + 2}, $${i * 7 + 3}, 'seed', 'todo', 'high', $${i * 7 + 4}, 7, $${i * 7 + 5}, $${i * 7 + 6}, $${i * 7 + 7})`).join(', ')}`,
    values: seeded.flatMap(([id, username]) => [id, `00000000-0000-4000-8000-${String(id).padStart(12, '0')}`, `seed-${id}`, username, id, 1000 + id, id]) }], h.pgUrl, drainEngine)
  const dsBinary = ensureServerBinary()
  writeFileSync(join(root, 'runtime-identity.json'), JSON.stringify({ scenario: 'SWF-P0-4-filtered-windows', postgres: { initdb: command(tools.initdb, ['--version']), initdbSha256: sha256(tools.initdb) }, engine: { path: join(server, 'target/debug/electric-circuits-engine'), sha256: sha256(join(server, 'target/debug/electric-circuits-engine')), head: command('git', ['rev-parse', 'HEAD'], server), tree: command('git', ['rev-parse', 'HEAD^{tree}'], server) }, durableStreams: { path: dsBinary, sha256: sha256(dsBinary) }, swift: command('swift', ['--version']).split('\n')[0], endpoints: { engine: h.engineUrl, durableStreams: h.dsUrl } }, null, 2))

  const scratch = mkdtempSync(join(tmpdir(), 'electric-circuits-swift-filtered-windows-build-')); scratchRoots.push(scratch)
  swift = spawn('swift', ['run', '--scratch-path', scratch, 'LinearLiteRealFilteredWindows'], { cwd: join(candidate, 'Examples/LinearLite'), env: { ...process.env, ECS_FILTERED_WINDOWS_BASE_URL: h.engineUrl, ECS_FILTERED_WINDOWS_DIR: phase, ECS_FILTERED_WINDOWS_DATABASE: sqlite }, stdio: ['ignore', 'pipe', 'pipe'] })
  swift.stdout?.on('data', data => { swiftLog += data; writeFileSync(join(root, 'swift.log'), swiftLog) }); swift.stderr?.on('data', data => { swiftLog += data; writeFileSync(join(root, 'swift.log'), swiftLog) })
  await waitFor('initial-pair-queries-entered', () => {
    assertChildAlive('initial pair gate')
    return existsSync(join(phase, 'initial-pair-queries-entered'))
  }, Number(process.env.ECS_FILTERED_WINDOWS_STARTUP_DEADLINE_MS ?? '180000'))
  const gate = JSON.parse(readFileSync(join(phase, 'initial-pair-gate.json'), 'utf8'))
  if (gate.heads !== 2 || gate.initialQueries !== 2) throw new Error('feed-before-snapshot gate did not observe both durable heads and queries')
  await sourceTransaction('snapshot-overlap', [{ text: "INSERT INTO issues VALUES (50, '00000000-0000-4000-8000-000000000050', 'overlap-a', 'overlap', 'todo', 'high', 'ada', 7, 50, 5000, 50)" }], h.pgUrl, drainEngine)
  writeFileSync(join(phase, 'initial-pair-query-release'), '')
  await waitForFile('baseline-client-ready'); assertChildAlive('baseline')
  let expectedA = await oracle(h.pgUrl, 'ada'); let expectedB = await oracle(h.pgUrl, 'bob')
  assertObservation('baseline', expectedA, expectedB, JSON.parse(readFileSync(join(phase, 'baseline-client-ready.json'), 'utf8')))

  await waitForFile('a-insert-client-ready')
  await sourceTransaction('a-insert', [{ text: "INSERT INTO issues VALUES (51, '00000000-0000-4000-8000-000000000051', 'newest-a', 'insert A only', 'todo', 'high', 'ada', 7, 51, 6000, 51)" }], h.pgUrl, drainEngine)
  await waitForFile('a-insert-observed'); expectedA = await oracle(h.pgUrl, 'ada'); expectedB = await oracle(h.pgUrl, 'bob'); assertObservation('a-insert', expectedA, expectedB, JSON.parse(readFileSync(join(phase, 'a-insert-observed.json'), 'utf8')))

  await waitForFile('reassignment-client-ready')
  await sourceTransaction('reassignment', [{ text: "UPDATE issues SET username = 'bob', modified = 7000, title = 'moved-a-to-b' WHERE id = 12" }], h.pgUrl, drainEngine)
  await waitForFile('reassignment-observed'); expectedA = await oracle(h.pgUrl, 'ada'); expectedB = await oracle(h.pgUrl, 'bob'); assertObservation('reassignment', expectedA, expectedB, JSON.parse(readFileSync(join(phase, 'reassignment-observed.json'), 'utf8')))

  await waitForFile('neutral-update-client-ready')
  await sourceTransaction('neutral-update', [{ text: "UPDATE issues SET title = 'neutral-b-update' WHERE id = 12" }], h.pgUrl, drainEngine)
  await waitForFile('neutral-update-observed'); expectedA = await oracle(h.pgUrl, 'ada'); expectedB = await oracle(h.pgUrl, 'bob'); assertObservation('neutral-update', expectedA, expectedB, JSON.parse(readFileSync(join(phase, 'neutral-update-observed.json'), 'utf8')))

  await waitForFile('a-closed'); await waitForFile('b-after-a-close-client-ready')
  await sourceTransaction('b-after-a-close', [{ text: "UPDATE issues SET title = 'b-after-a-close' WHERE id = 12" }], h.pgUrl, drainEngine)
  await waitForFile('b-after-a-close-observed'); expectedA = await oracle(h.pgUrl, 'ada'); expectedB = await oracle(h.pgUrl, 'bob'); assertObservation('b-after-a-close', expectedA, expectedB, JSON.parse(readFileSync(join(phase, 'b-after-a-close-observed.json'), 'utf8')))
  if (!sameRows(expectedA, JSON.parse(readFileSync(join(phase, 'neutral-update-observed.json'), 'utf8')).a.rows)) throw new Error('A changed after its release')
  await awaitChildWithin(swift, 'Swift filtered-window child', 60_000)
  const result = JSON.parse(readFileSync(join(phase, 'swift-filtered-windows-result.json'), 'utf8'))
  assertObservation('reopened', expectedA, expectedB, result.reopened)
  if (result.headers.feeds < 2 || result.headers.queries < 2 || result.headers.streams < 2 || result.headers.releases !== 2) throw new Error('custom sentinel header/release facts incomplete')
  writeFileSync(join(root, 'result.json'), JSON.stringify({ expectedA, expectedB, swift: result }, null, 2)); scenarioPassed = true
}

async function entry() {
  if (process.argv.includes('--self-test-oracle')) { try { selfTestOracle() } finally { rmSync(root, { recursive: true, force: true }) }; return }
  if (process.argv.includes('--self-test-cleanup-status')) { try { selfTestCleanupStatus() } finally { rmSync(root, { recursive: true, force: true }) }; return }
  if (process.argv.includes('--self-test-child-boundary')) { try { await selfTestChildBoundary() } finally { rmSync(root, { recursive: true, force: true }) }; return }
  try { await main() } catch (error) { failure = String(error); console.error(error); process.exitCode = 1 } finally {
    const enginePort = h?.engineUrl ? Number(new URL(h.engineUrl).port) : 0; const dsPort = h?.dsUrl ? Number(new URL(h.dsUrl).port) : 0
    const phaseFiles = ['initial-pair-gate.json', 'baseline-client-ready.json', 'a-insert-observed.json', 'reassignment-observed.json', 'neutral-update-observed.json', 'b-after-a-close-observed.json']
    const phaseEvidence = Object.fromEntries(phaseFiles.filter(file => existsSync(join(phase, file))).map(file => [file, JSON.parse(readFileSync(join(phase, file), 'utf8'))]))
    const cleanup = { swiftReaped: false, listenersGone: false, pgDataRemoved: false, scratchClean: false, rootRemoved: false, error: '' }
    try {
      cleanup.swiftReaped = await reapChild(swift)
      if (!cleanup.swiftReaped) throw new Error('Swift child was not reaped during cleanup')
      await h?.shutdown(); if (pgCtl) try { execFileSync(pgCtl, ['-D', pgData, '-m', 'immediate', '-w', 'stop']) } catch {}
      if (enginePort) { await sleep(100); assertNoListener(enginePort, 'engine') }; if (dsPort) assertNoListener(dsPort, 'durable-streams'); if (pgPort) assertNoListener(pgPort, 'postgres')
      cleanup.listenersGone = true; rmSync(pgData, { recursive: true, force: true }); cleanup.pgDataRemoved = !existsSync(pgData)
      for (const scratch of scratchRoots) execFileSync('swift', ['package', 'clean', '--scratch-path', scratch], { cwd: join(candidate, 'Examples/LinearLite'), stdio: 'ignore' })
      cleanup.scratchClean = true
    } catch (error) { cleanup.error = String(error); failure ||= cleanup.error }
    const retained = { runtime: existsSync(join(root, 'runtime-identity.json')) ? JSON.parse(readFileSync(join(root, 'runtime-identity.json'), 'utf8')) : undefined, result: existsSync(join(root, 'result.json')) ? JSON.parse(readFileSync(join(root, 'result.json'), 'utf8')) : undefined, phaseEvidence, swiftLog }
    rmSync(root, { recursive: true, force: true }); cleanup.rootRemoved = !existsSync(root)
    const status = qualificationStatus(scenarioPassed, cleanup)
    if (status !== 'pass') process.exitCode = 1
    mkdirSync(evidence, { recursive: true }); writeFileSync(join(evidence, 'real-filtered-windows-pg18-last-run.json'), JSON.stringify({ status, failure, ...retained, cleanup }, null, 2))
    if (status === 'pass') console.log('PASS SWF-P0-4-filtered-windows cleanup-qualified')
  }
}
void entry()
