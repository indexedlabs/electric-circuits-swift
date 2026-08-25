#!/usr/bin/env node
// Layer-B qualification for the real LinearLite recent-subset path. It reuses the reviewed
// PG18 harness's actual Postgres -> replication -> durable-streams -> native Axum topology; this
// file only adds the independently-authored top-10 oracle and explicit source/transport phases.
import { execFileSync, spawn } from 'node:child_process'
import { createHash } from 'node:crypto'
import { existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import pg from 'pg'

const server = process.env.ELECTRIC_CIRCUITS_SERVER_ROOT ?? '/Users/bozilabs/labs/electric-circuits'
const candidate = dirname(dirname(fileURLToPath(import.meta.url)))
const retainedEvidence = join(candidate, 'Evidence')
mkdirSync(retainedEvidence, { recursive: true })
const runRoot = mkdtempSync(join(tmpdir(), 'electric-circuits-swift-real-top10-v1-'))
const phaseDirectory = join(runRoot, 'phase')
const databasePath = join(runRoot, 'linearlite.sqlite')
const pgData = join(runRoot, 'postgres')
const pgLog = join(runRoot, 'postgres.log')
mkdirSync(phaseDirectory)

let h: any
let swift: ReturnType<typeof spawn> | undefined
let passed = false
let failure: string | undefined
let pgCtl = ''
let pgPort = 0
let swiftLog = ''

const sleep = (milliseconds: number) => new Promise(resolve => setTimeout(resolve, milliseconds))
const sha256 = (path: string) => createHash('sha256').update(readFileSync(path)).digest('hex')
const command = (executable: string, args: string[], cwd = candidate) =>
  execFileSync(executable, args, { cwd, encoding: 'utf8' }).trim()

function digestTree(root: string, ignored: Set<string>): Array<{ path: string, mode: string, sha256: string }> {
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
  const serverStatus = command('git', ['status', '--porcelain=v1', '--untracked-files=all'], server)
  if (serverStatus && process.env.ECS_ALLOW_DIRTY_SERVER !== '1') {
    throw new Error('server checkout is dirty; set ECS_ALLOW_DIRTY_SERVER=1 only to retain its complete overlay manifest')
  }
  const serverManifest = digestTree(server, new Set(['.git', 'node_modules', 'target', '.build', '.derivedData', 'DerivedData', 'Evidence']))
  const candidateManifest = digestTree(candidate, new Set(['.git', '.build', '.derivedData', 'DerivedData', 'Evidence']))
  return {
    server: {
      root: server, head: command('git', ['rev-parse', 'HEAD'], server), tree: command('git', ['rev-parse', 'HEAD^{tree}'], server),
      statusPorcelainV1: serverStatus,
      completeRuntimeSourceManifestSha256: createHash('sha256').update(JSON.stringify(serverManifest)).digest('hex'),
    },
    swiftCandidate: {
      root: candidate,
      completeRuntimeInputManifestSha256: createHash('sha256').update(JSON.stringify(candidateManifest)).digest('hex'),
    },
    runtimeExecutables: {
      engine: { path: join(server, 'target/debug/electric-circuits-engine'), sha256: sha256(join(server, 'target/debug/electric-circuits-engine')) },
      durableStreams: { path: dsBinary, sha256: sha256(dsBinary) },
      postgres18: {
        initdb: { path: tools.initdb, version: command(tools.initdb, ['--version']), sha256: sha256(tools.initdb) },
        pgCtl: { path: tools.pgCtl, version: command(tools.pgCtl, ['--version']), sha256: sha256(tools.pgCtl) },
      },
    },
    toolchain: { node: process.version, swift: command('swift', ['--version']).split('\n')[0], pnpm: command('pnpm', ['--version']) },
  }
}

async function waitFor(name: string, predicate: () => Promise<boolean> | boolean, deadlineMs = 30_000): Promise<void> {
  const deadline = Date.now() + deadlineMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await sleep(25)
  }
  throw new Error(`deadline waiting for ${name}; root=${runRoot}`)
}

async function waitForFile(name: string): Promise<void> {
  await waitFor(name, () => existsSync(join(phaseDirectory, name)))
}

async function freePort(): Promise<number> {
  const net = await import('node:net')
  return await new Promise((resolve, reject) => {
    const listener = net.createServer().once('error', reject).listen(0, '127.0.0.1', () => {
      const address = listener.address()
      listener.close(() => typeof address === 'object' && address ? resolve(address.port) : reject(new Error('no free port')))
    })
  })
}

async function sql(url: string, query: string, values: unknown[] = []) {
  const client = new pg.Client({ connectionString: url })
  await client.connect()
  try { return await client.query(query, values) } finally { await client.end() }
}

function sourceRecord(name: string, marker: number) {
  writeFileSync(join(phaseDirectory, `${name}-source-committed.json`), JSON.stringify({ sourceCommitID: marker }, null, 2))
}

async function sourceTransaction(
  name: string, statements: Array<{ text: string, values?: unknown[] }>, pgUrl: string, drainEngine: (harness: any) => Promise<void>,
): Promise<number> {
  const client = new pg.Client({ connectionString: pgUrl })
  await client.connect()
  let marker: number
  try {
    await client.query('BEGIN')
    for (const statement of statements) await client.query(statement.text, statement.values)
    marker = Number((await client.query(
      'UPDATE causal_markers SET marker = marker + 1 WHERE id = 1 RETURNING marker')).rows[0].marker)
    await client.query('COMMIT')
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  } finally { await client.end() }
  sourceRecord(name, marker!)
  await drainEngine(h)
  writeFileSync(join(phaseDirectory, `${name}-server-drained`), JSON.stringify({
    sourceCommitID: marker, receipt: 'harness drainEngine after same-transaction causal marker',
  }, null, 2))
  return marker!
}

async function captureFeedFanout(name: string) {
  const feed = JSON.parse(readFileSync(join(phaseDirectory, 'feed-handle.json'), 'utf8')) as {
    id: string, streamPath: string
  }
  const [tableOffset, replication, head] = await Promise.all([
    fetch(`${h.engineUrl}/tables/public.issues/offset`).then(async response => ({
      status: response.status, body: await response.json().catch(() => null),
    })),
    fetch(`${h.engineUrl}/replication/lsn`).then(async response => ({
      status: response.status, body: await response.json().catch(() => null),
    })),
    fetch(new URL(feed.streamPath, `${h.dsUrl}/`), { method: 'HEAD' }).then(response => ({
      status: response.status, nextOffset: response.headers.get('stream-next-offset'),
      closed: response.headers.get('stream-closed'),
    })),
  ])
  writeFileSync(join(phaseDirectory, `${name}-fanout-state.json`), JSON.stringify({
    feed, tableOffset, replication, durableShapeHead: head,
  }, null, 2))
}

async function oracle(pgUrl: string): Promise<Array<{ id: number, modified: number, title: string }>> {
  return (await sql(
    pgUrl, 'SELECT id, modified, title FROM issues ORDER BY modified DESC, id DESC LIMIT 10')).rows
    .map((row: any) => ({ id: Number(row.id), modified: Number(row.modified), title: String(row.title) }))
}

function assertPhase(name: string, expected: unknown, observed: any) {
  const actual = observed.grdbRows
  if (JSON.stringify(actual) !== JSON.stringify(expected) || JSON.stringify(observed.sessionRows) !== JSON.stringify(expected)) {
    throw new Error(`${name}: SQL top-10 oracle mismatch expected=${JSON.stringify(expected)} grdb=${JSON.stringify(actual)} session=${JSON.stringify(observed.sessionRows)}`)
  }
  if (!observed.cursor?.offset || observed.membershipCount !== actual.length || new Set(actual.map((row: any) => row.id)).size !== actual.length) {
    throw new Error(`${name}: GRDB cursor/membership is not atomically exact and duplicate-free`)
  }
}

function assertNoListener(port: number, label: string) {
  try {
    const pids = execFileSync('lsof', ['-tiTCP:' + port, '-sTCP:LISTEN'], { encoding: 'utf8' }).trim()
    if (pids) throw new Error(`${label} listener survived on ${port}: ${pids}`)
  } catch (error: any) {
    if (error.status === 1) return
    throw error
  }
}

async function awaitChildWithin(child: ReturnType<typeof spawn>, label: string, deadlineMs: number): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    let done = false
    let expired = false
    let timer: ReturnType<typeof setTimeout> | undefined
    const finish = (error?: Error) => {
      if (done) return
      done = true
      if (timer) clearTimeout(timer)
      error ? reject(error) : resolve()
    }
    const finishIfAlreadyExited = () => {
      if (child.exitCode === null && child.signalCode === null) return false
      if (child.exitCode === 0) finish()
      else finish(new Error(`${label} exited ${child.exitCode ?? child.signalCode}`))
      return true
    }
    // A phase marker can be observed in the same turn that the child writes its final
    // evidence and exits. ChildProcess does not replay an already-emitted `exit` event.
    if (finishIfAlreadyExited()) return
    timer = setTimeout(() => {
      expired = true
      child.kill('SIGTERM')
      setTimeout(() => child.kill('SIGKILL'), 250).unref()
    }, deadlineMs)
    child.once('exit', code => expired
      ? finish(new Error(`${label} exceeded ${deadlineMs}ms; terminated child pid=${child.pid}`))
      : code === 0 ? finish() : finish(new Error(`${label} exited ${code}`)))
    child.once('error', error => finish(error))
    // Keep the same fail-closed behavior if it exited between the first check and
    // listener installation.
    finishIfAlreadyExited()
  })
}

async function selfTestChildDeadline() {
  const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' })
  let timedOut = false
  try { await awaitChildWithin(child, 'self-test hung child', 50) } catch { timedOut = true }
  if (!timedOut || (child.exitCode === null && child.signalCode === null)) {
    throw new Error('child-deadline self-test did not reap its hung process')
  }
  const alreadyExited = spawn(process.execPath, ['-e', 'process.exit(0)'], { stdio: 'ignore' })
  await new Promise<void>((resolve, reject) => alreadyExited.once('exit', code => code === 0 ? resolve() : reject(new Error('pre-exited self-test child failed'))))
  await awaitChildWithin(alreadyExited, 'self-test pre-exited child', 50)
  console.log('PASS child-deadline self-test')
}

function selfTestPhaseOracle() {
  const expected = [{ id: 2, modified: 20, title: 'two' }]
  assertPhase('self-test', expected, {
    grdbRows: expected, sessionRows: expected, cursor: { offset: '2' }, membershipCount: 1,
  })
  let rejectedDuplicate = false
  try {
    assertPhase('self-test-duplicate', expected, {
      grdbRows: expected, sessionRows: expected, cursor: { offset: '2' }, membershipCount: 2,
    })
  } catch { rejectedDuplicate = true }
  if (!rejectedDuplicate) throw new Error('phase-oracle self-test accepted duplicate membership')
  console.log('PASS phase-oracle self-test')
}

async function main() {
  if (!existsSync(join(server, 'target/debug/electric-circuits-engine'))) {
    throw new Error('prebuilt engine missing; build the server candidate before qualification')
  }
  const { postgres18Tools } = await import(join(server, 'scripts/postgres18.ts'))
  const { bootHarness, drainEngine } = await import(join(server, 'packages/conformance/src/harness.ts'))
  const { ensureServerBinary } = await import(join(server, 'packages/ds-rust/src/index.ts'))
  const tools = postgres18Tools()
  const identity = runtimeIdentity(tools, ensureServerBinary())
  if (!identity.runtimeExecutables.postgres18.initdb.version.includes('18.4')) {
    throw new Error(`this scenario is pinned to PostgreSQL 18.4, got ${identity.runtimeExecutables.postgres18.initdb.version}`)
  }
  pgCtl = tools.pgCtl
  pgPort = await freePort()
  execFileSync(tools.initdb, ['-D', pgData, '-U', 'postgres', '--auth=trust', '--no-sync'])
  writeFileSync(
    join(pgData, 'postgresql.conf'),
    `\nlisten_addresses = '127.0.0.1'\nport = ${pgPort}\nwal_level = logical\nmax_replication_slots = 20\nmax_wal_senders = 20\n`,
    { flag: 'a' })
  execFileSync(pgCtl, ['-D', pgData, '-l', pgLog, '-w', 'start'])
  process.env.ELECTRIC_CIRCUITS_TEST_PG_URL = `postgres://postgres@127.0.0.1:${pgPort}/postgres`
  process.env.ELECTRIC_CIRCUITS_ENGINE_PREBUILT = '1'
  const schema = { tables: {
    issues: { columns: {
      id: { type: 'int' }, client_id: { type: 'text' }, title: { type: 'text' }, description: { type: 'text' },
      status: { type: 'text' }, priority: { type: 'text' }, username: { type: 'text' }, project_id: { type: 'int' },
      created: { type: 'int' }, modified: { type: 'int' }, kanbanorder: { type: 'float' },
    }, primaryKey: 'id' },
    causal_markers: { columns: { id: { type: 'int' }, marker: { type: 'int' } }, primaryKey: 'id' },
  } }
  h = await bootHarness(schema, { ddl: `
    CREATE TABLE issues (
      id integer PRIMARY KEY, client_id text, title text NOT NULL, description text NOT NULL,
      status text NOT NULL, priority text NOT NULL, username text NOT NULL, project_id integer NOT NULL,
      created bigint NOT NULL, modified bigint NOT NULL, kanbanorder double precision NOT NULL
    );
    ALTER TABLE issues REPLICA IDENTITY FULL;
    CREATE TABLE causal_markers (id integer PRIMARY KEY, marker bigint NOT NULL);
    ALTER TABLE causal_markers REPLICA IDENTITY FULL;
    INSERT INTO causal_markers (id, marker) VALUES (1, 0);`,
  })
  // This is deliberately a live Axum document, not a copied fixture: the real corpus refuses
  // to exercise a native surface whose published OpenAPI no longer matches the Swift v1 contract.
  execFileSync(
    process.execPath,
    [join(candidate, 'Scripts', 'check-native-v1-contract.mjs'), '--openapi', `${h.engineUrl}/v1/openapi.json`],
    { cwd: candidate, stdio: 'inherit' },
  )
  const seed = [
    [15, 1500], [14, 1400], [13, 1300], [12, 1200], [11, 1100], [10, 1000], [9, 900], [8, 800],
    [7, 700], [6, 700], [5, 600], [4, 500], [3, 400], [2, 300], [1, 200],
  ]
  await sourceTransaction('seed', [{
    text: `INSERT INTO issues (id, client_id, title, description, status, priority, username, project_id, created, modified, kanbanorder)
      VALUES ${seed.map(([id], index) => `($${index * 6 + 1}, '00000000-0000-4000-8000-${String(id).padStart(12, '0')}', $${index * 6 + 2}, $${index * 6 + 3}, 'todo', 'high', 'ada', 7, $${index * 6 + 4}, $${index * 6 + 5}, $${index * 6 + 6})`).join(', ')}`,
    values: seed.flatMap(([id, modified]) => [id, `seed-${id}`, `description-${id}`, id, modified, id]),
  }], h.pgUrl, drainEngine)

  writeFileSync(join(runRoot, 'manifest.json'), JSON.stringify({
    scenario: 'SWF-P0-8-real-top10', profile: 'NATIVE_CORE_LAYER_B_PG18_4', identity,
    endpoints: { engine: h.engineUrl, durableStreams: h.dsUrl }, phaseDirectory,
  }, null, 2))
  const scratch = mkdtempSync(join(tmpdir(), 'electric-circuits-swift-top10-build-'))
  swift = spawn('swift', [
    'run', '--scratch-path', scratch, 'LinearLiteRealTopTen',
  ], {
    cwd: join(candidate, 'Examples/LinearLite'),
    env: {
      ...process.env, ECS_REAL_TOP10_BASE_URL: h.engineUrl, ECS_REAL_TOP10_DIR: phaseDirectory,
      ECS_REAL_TOP10_DATABASE: databasePath,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  swift.stdout?.on('data', data => { swiftLog += data; writeFileSync(join(runRoot, 'swift.log'), swiftLog) })
  swift.stderr?.on('data', data => { swiftLog += data; writeFileSync(join(runRoot, 'swift.log'), swiftLog) })

  const startupDeadlineMs = Number(process.env.ECS_REAL_TOP10_STARTUP_DEADLINE_MS ?? '180000')
  const startupDeadline = Date.now() + startupDeadlineMs
  const remainingStartup = () => Math.max(1, startupDeadline - Date.now())
  await waitFor('feed-cursor-captured', () => {
    if (existsSync(join(phaseDirectory, 'feed-cursor-captured'))) return true
    if (swift?.exitCode !== null && swift?.exitCode !== undefined) {
      throw new Error(`Swift top-10 child exited ${swift.exitCode} before feed cursor\n${swiftLog}`)
    }
    return false
  }, remainingStartup())
  await waitFor('subset-query-entered', () => existsSync(join(phaseDirectory, 'subset-query-entered')), remainingStartup())
  // The feed and frontier already exist. This source transaction is held against the real subset
  // request, then the actual Axum query runs after its marker has drained.
  await sourceTransaction('snapshot-boundary', [{
    text: `INSERT INTO issues (id, client_id, title, description, status, priority, username, project_id, created, modified, kanbanorder)
      VALUES (100, '00000000-0000-4000-8000-000000000100', 'boundary-newest', 'overlaps feed/snapshot fence', 'todo', 'high', 'ada', 7, 100, 2000, 100)`,
  }], h.pgUrl, drainEngine)
  writeFileSync(join(phaseDirectory, 'subset-query-release'), '')
  await waitFor('baseline-observed', () => {
    if (existsSync(join(phaseDirectory, 'baseline-observed'))) return true
    if (swift?.exitCode !== null && swift?.exitCode !== undefined) {
      throw new Error(`Swift top-10 child exited ${swift.exitCode} before baseline observation\n${swiftLog}`)
    }
    return false
  }, 45_000)
  assertPhase('snapshot-boundary', await oracle(h.pgUrl), JSON.parse(readFileSync(join(phaseDirectory, 'baseline-observed.json'), 'utf8')))

  const phases: Array<[string, Array<{ text: string }>]> = [
    ['newest-insert', [{ text: `INSERT INTO issues (id, client_id, title, description, status, priority, username, project_id, created, modified, kanbanorder)
      VALUES (101, '00000000-0000-4000-8000-000000000101', 'newest', 'enters and evicts the boundary', 'todo', 'high', 'ada', 7, 101, 3000, 101)` }]],
    ['outside-update', [{ text: "UPDATE issues SET title = 'outside-now-newest', modified = 4000 WHERE id = 5" }]],
    ['move-below-boundary', [{ text: "UPDATE issues SET title = 'moved-below', modified = 0 WHERE id = 100" }]],
    ['delete-promotes', [{ text: 'DELETE FROM issues WHERE id = 5' }]],
  ]
  const phaseEvidence: Array<{ name: string, sourceCommitID: number, oracle: unknown }> = []
  for (const [name, statements] of phases) {
    await waitForFile(`${name}-client-ready`)
    const sourceCommitID = await sourceTransaction(name, statements, h.pgUrl, drainEngine)
    await captureFeedFanout(name)
    await waitFor(`${name}-observed`, () => {
      if (existsSync(join(phaseDirectory, `${name}-observed`))) return true
      if (swift?.exitCode !== null && swift?.exitCode !== undefined) {
        throw new Error(`Swift top-10 child exited ${swift.exitCode} before ${name} observation\n${swiftLog}`)
      }
      return false
    }, 45_000)
    const expected = await oracle(h.pgUrl)
    const observed = JSON.parse(readFileSync(join(phaseDirectory, `${name}-observed.json`), 'utf8'))
    assertPhase(name, expected, observed)
    phaseEvidence.push({ name, sourceCommitID, oracle: expected })
  }
  const childDeadlineMs = Number(process.env.ECS_REAL_TOP10_CHILD_DEADLINE_MS ?? '30000')
  if (!swift) throw new Error('Swift top-10 child was not started')
  await awaitChildWithin(swift, 'Swift top-10 child', childDeadlineMs)
    .catch(error => { throw new Error(`${error.message}\n${swiftLog}`) })
  const result = JSON.parse(readFileSync(join(phaseDirectory, 'swift-result.json'), 'utf8'))
  const finalOracle = await oracle(h.pgUrl)
  if (JSON.stringify(result.reopenedRows) !== JSON.stringify(finalOracle) || result.reopenedMembershipCount !== finalOracle.length) {
    throw new Error('reopened GRDB provider did not expose the final SQL top-10/cursor exactly once')
  }
  if (!result.headers.nativeControl || !result.headers.subsetQuery || !result.headers.durableStream || !result.headers.release) {
    throw new Error('configured qualification header did not reach all real native request classes')
  }
  writeFileSync(join(runRoot, 'result.json'), JSON.stringify({ phaseEvidence, finalOracle, swift: result }, null, 2))
  passed = true
  console.log(`PASS SWF-P0-8-real-top10 root=${runRoot}`)
}

async function runQualification() {
  await main().catch(error => { failure = String(error); console.error(error); process.exitCode = 1 }).finally(async () => {
    swift?.kill('SIGKILL')
    const enginePort = h?.engineUrl ? Number(new URL(h.engineUrl).port) : undefined
    const dsPort = h?.dsUrl ? Number(new URL(h.dsUrl).port) : undefined
    await h?.shutdown().catch(() => {})
    if (pgCtl) try { execFileSync(pgCtl, ['-D', pgData, '-m', 'immediate', '-w', 'stop']) } catch {}
    if (enginePort) await sleep(100).then(() => assertNoListener(enginePort, 'engine'))
    if (dsPort) assertNoListener(dsPort, 'durable-streams')
    if (pgPort) assertNoListener(pgPort, 'postgres')
    rmSync(pgData, { recursive: true, force: true })
    const record = {
      status: passed ? 'pass' : 'fail', failure,
      manifest: existsSync(join(runRoot, 'manifest.json')) ? JSON.parse(readFileSync(join(runRoot, 'manifest.json'), 'utf8')) : undefined,
      result: existsSync(join(runRoot, 'result.json')) ? JSON.parse(readFileSync(join(runRoot, 'result.json'), 'utf8')) : undefined,
      initialDebug: existsSync(join(phaseDirectory, 'initial-debug.json'))
        ? JSON.parse(readFileSync(join(phaseDirectory, 'initial-debug.json'), 'utf8')) : undefined,
      phaseDebug: Object.fromEntries(
        ['newest-insert', 'outside-update', 'move-below-boundary', 'delete-promotes']
          .filter(name => existsSync(join(phaseDirectory, `${name}-debug.json`)))
          .map(name => [name, JSON.parse(readFileSync(join(phaseDirectory, `${name}-debug.json`), 'utf8'))])),
      fanoutState: Object.fromEntries(
        ['newest-insert', 'outside-update', 'move-below-boundary', 'delete-promotes']
          .filter(name => existsSync(join(phaseDirectory, `${name}-fanout-state.json`)))
          .map(name => [name, JSON.parse(readFileSync(join(phaseDirectory, `${name}-fanout-state.json`), 'utf8'))])),
      swiftLog,
      cleanup: { engine: 'gone', durableStreams: 'gone', postgres: 'gone', pgDataRemoved: !existsSync(pgData) },
    }
    writeFileSync(join(retainedEvidence, 'real-top10-pg18-last-run.json'), JSON.stringify(record, null, 2))
    rmSync(runRoot, { recursive: true, force: true })
  })
}

async function entry() {
  if (process.argv.includes('--self-test-child-deadline')) {
    try { await selfTestChildDeadline() } finally { rmSync(runRoot, { recursive: true, force: true }) }
    return
  }
  if (process.argv.includes('--self-test-phase-oracle')) {
    try { selfTestPhaseOracle() } finally { rmSync(runRoot, { recursive: true, force: true }) }
    return
  }
  await runQualification()
}

void entry()
