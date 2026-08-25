#!/usr/bin/env node
// Dependency-free drift gate for the Rust/Axum native-v1 OpenAPI document.
import { readFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'

const scriptDirectory = new URL('.', import.meta.url)
const defaultContract = new URL('../Contracts/native-v1-contract-v1.json', scriptDirectory)

export class ContractDriftError extends Error {}

function fail(message) { throw new ContractDriftError(message) }
function responseSchema(response) {
  const content = response?.content?.['application/json']
  return content?.schema?.$ref?.split('/').at(-1) ?? null
}

export function checkNativeV1Contract(contract, document) {
  if (document.openapi !== contract.rustOpenAPI.openapi) {
    fail(`OpenAPI version drift: expected ${contract.rustOpenAPI.openapi}, got ${document.openapi ?? '<missing>'}`)
  }
  if (document.info?.version !== contract.rustOpenAPI.infoVersion) {
    fail(`native API version drift: expected ${contract.rustOpenAPI.infoVersion}, got ${document.info?.version ?? '<missing>'}`)
  }
  for (const expected of contract.paths) {
    const operation = document.paths?.[expected.path]?.[expected.method]
    if (!operation) fail(`missing operation ${expected.method.toUpperCase()} ${expected.path}`)
    if (expected.request) {
      const actual = operation.requestBody?.content?.['application/json']?.schema?.$ref?.split('/').at(-1)
      if (actual !== expected.request) {
        fail(`${expected.method.toUpperCase()} ${expected.path} request schema drift: expected ${expected.request}, got ${actual ?? '<missing>'}`)
      }
    }
    for (const [status, expectedSchema] of Object.entries(expected.responses)) {
      const response = operation.responses?.[status]
      if (!response) fail(`${expected.method.toUpperCase()} ${expected.path} missing documented status ${status}`)
      const actual = responseSchema(response)
      if (actual !== expectedSchema) {
        fail(`${expected.method.toUpperCase()} ${expected.path} ${status} schema drift: expected ${expectedSchema ?? '<empty>'}, got ${actual ?? '<empty>'}`)
      }
    }
  }
  for (const [name, fields] of Object.entries(contract.schemas)) {
    const schema = document.components?.schemas?.[name]
    if (!schema) fail(`missing schema ${name}`)
    for (const field of fields) {
      if (field === 'oneOf') {
        if (!Array.isArray(schema.oneOf)) fail(`${name} must retain its oneOf predicate grammar`)
      } else if (!Object.hasOwn(schema.properties ?? {}, field)) {
        fail(`${name} missing property ${field}`)
      }
    }
  }
}

async function loadJSON(location) {
  if (/^https?:\/\//.test(location)) {
    const response = await fetch(location, { headers: { Accept: 'application/json' } })
    if (!response.ok) throw new Error(`OpenAPI fetch ${location} returned ${response.status}`)
    return response.json()
  }
  return JSON.parse(await readFile(location, 'utf8'))
}

function selfTest() {
  const aggregate = {
    path: '/v1/aggregates', method: 'post', request: 'OpenApiAggregateRequest',
    responses: { 200: 'OpenApiShapeResponse', 400: 'OpenApiError' },
  }
  const contract = {
    rustOpenAPI: { openapi: '3.0.3', infoVersion: '0.1.0' }, paths: [aggregate],
    schemas: {
      OpenApiAggregateRequest: ['table', 'where', 'fn', 'col', 'subscription'],
      OpenApiShapeResponse: ['shapeId'], OpenApiError: ['error'],
    },
  }
  const aggregateOperation = {
    requestBody: { content: { 'application/json': { schema: { $ref: '#/components/schemas/OpenApiAggregateRequest' } } } },
    responses: {
      200: { content: { 'application/json': { schema: { $ref: '#/components/schemas/OpenApiShapeResponse' } } } },
      400: { content: { 'application/json': { schema: { $ref: '#/components/schemas/OpenApiError' } } } },
    },
  }
  const document = {
    openapi: '3.0.3', info: { version: '0.1.0' },
    paths: { '/v1/aggregates': { post: aggregateOperation } },
    components: {
      schemas: {
        OpenApiAggregateRequest: { properties: { table: {}, where: {}, fn: {}, col: {}, subscription: {} } },
        OpenApiShapeResponse: { properties: { shapeId: {} } }, OpenApiError: { properties: { error: {} } },
      },
    },
  }
  checkNativeV1Contract(contract, document)
  // Additive paths are allowed by v1.
  checkNativeV1Contract(contract, { ...document, paths: { ...document.paths, '/v1/future': { get: {} } } })
  let aggregateRemovalRejected = false
  try { checkNativeV1Contract(contract, { ...document, paths: {} }) } catch (error) {
    aggregateRemovalRejected = error instanceof ContractDriftError
      && error.message === 'missing operation POST /v1/aggregates'
  }
  if (!aggregateRemovalRejected) throw new Error('self-test accepted removal of POST /v1/aggregates')
  let versionRejected = false
  try { checkNativeV1Contract(contract, { ...document, info: { version: '9.9.9' } }) } catch (error) {
    versionRejected = error instanceof ContractDriftError
  }
  if (!versionRejected) throw new Error('self-test accepted a changed native API version')
  console.log('PASS native-v1 contract checker self-test')
}

async function main() {
  const args = process.argv.slice(2)
  if (args[0] === '--self-test') return selfTest()
  const location = args[0] === '--openapi' ? args[1] : process.env.ECS_NATIVE_V1_OPENAPI_URL
  if (!location) throw new Error('usage: check-native-v1-contract.mjs --openapi <URL-or-file>')
  const contract = JSON.parse(await readFile(fileURLToPath(defaultContract), 'utf8'))
  const document = await loadJSON(location)
  checkNativeV1Contract(contract, document)
  console.log(`PASS native-v1 contract v${contract.contractVersion}: ${location}`)
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(error => { console.error(`FAIL native-v1 contract: ${error.message}`); process.exitCode = 1 })
}
