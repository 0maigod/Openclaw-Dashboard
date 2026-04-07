'use strict';

/**
 * ingestionService.js — Background SSH ingestion job.
 *
 * Responsibilities:
 *  - For each node in config/machines.json:
 *      1. Open an SFTP connection
 *      2. Load openclaw.json to resolve agent identities for that node
 *      3. Scan agents/{id}/sessions/*.jsonl for new token events
 *      4. INSERT new events into SQLite with the correct node_id
 *  - Node failures are isolated: if one node is offline, others continue.
 *  - Idempotent: events have a deterministic ID and use INSERT OR IGNORE.
 */

const path     = require('path');
const readline = require('readline');
const crypto   = require('crypto');
const ssh      = require('./sshService');

// ── Agent role map (display labels) ──────────────────────────────────────────
const AGENT_ROLES = {
  'main':                        { displayName: 'VonClo',    subtitle: 'Agente Orquestador',            role: 'Orquestador' },
  'agente-fotografo':            { displayName: 'Farol',     subtitle: 'Agente Fotografía',             role: 'Fotografía' },
  'agentedesarrolladordeideas':  { displayName: 'Adi',       subtitle: 'Agente Desarrollador de Ideas', role: 'Desarrollador de Ideas' },
  'agenteshowrunner':            { displayName: 'Showy',     subtitle: 'Agente Showrunner',             role: 'Showrunner' },
  'agenteworldbuilding':         { displayName: 'Wildy',     subtitle: 'Agente Worldbuilding',          role: 'Worldbuilding' },
  'agentepersonajes':            { displayName: 'Peña',      subtitle: 'Agente Personajes',             role: 'Personajes' },
  'agenteestructuradramatica':   { displayName: 'Bartis',    subtitle: 'Agente Estructura Dramática',   role: 'Estructura Dramática' },
  'agenteguionliterario':        { displayName: 'Rimbaud',   subtitle: 'Agente Guion Literario',        role: 'Guion Literario' },
  'agentescriptdoctor':          { displayName: 'Doc',       subtitle: 'Agente Script Doctor',          role: 'Script Doctor' },
  'agentecontinuidad':           { displayName: 'Conti',     subtitle: 'Agente Continuidad',            role: 'Continuidad' },
  'agentebielinsky':             { displayName: 'Bielinsky', subtitle: 'Agente Guion Técnico',          role: 'Guion Técnico' },
  'agente-guion-tecnico':        { displayName: 'Bielinsky', subtitle: 'Agente Guion Técnico',          role: 'Guion Técnico' },
  'rimbaud':                     { displayName: 'Rimbaud',   subtitle: 'Agente Guion Literario',        role: 'Guion Literario' },
  'haus':                        { displayName: 'Haus',      subtitle: 'Agente Fotografía',             role: 'Fotografía' },
};

// ── Config loader ─────────────────────────────────────────────────────────────

function loadMachinesConfig() {
  const configPath = path.join(__dirname, '..', 'config', 'machines.json');
  try {
    return JSON.parse(require('fs').readFileSync(configPath, 'utf8'));
  } catch (err) {
    console.error('[ingestion] Cannot load config/machines.json:', err.message);
    return [];
  }
}

// ── Remote identity loader ────────────────────────────────────────────────────

/**
 * Reads openclaw.json from a remote node and returns a map of agentId → identity.
 * Returns an empty object if the file is missing or malformed.
 *
 * @param {object} sftp
 * @param {string} openclawPath - Root path of ~/.openclaw on the remote node
 * @returns {Promise<object>}
 */
async function loadRemoteIdentities(sftp, openclawPath) {
  const identities = {};
  const remoteCfgPath = openclawPath + '/openclaw.json';

  const raw = await ssh.readFileAsString(sftp, remoteCfgPath);
  if (!raw) return identities;

  let cfg;
  try { cfg = JSON.parse(raw); } catch { return identities; }

  for (const agent of (cfg.agents?.list || [])) {
    const role = AGENT_ROLES[agent.id] || {};
    identities[agent.id] = {
      name:     agent.identity?.name || role.displayName || agent.name || agent.id,
      emoji:    agent.identity?.emoji || '🤖',
      theme:    agent.identity?.theme || 'default',
      subtitle: role.subtitle || ('Agente ' + agent.id),
      role:     role.role    || agent.id,
      model:    agent.model  || cfg.agents?.defaults?.model?.primary || null,
    };
  }
  return identities;
}

// ── Subagent session map ──────────────────────────────────────────────────────

/**
 * Builds a map sessionId → { label } by scanning the main agent's session JSONL files
 * for completion events that mention a subagent runId or childSessionKey.
 *
 * @param {object} sftp
 * @param {string} agentsDir - Remote path to the agents directory
 * @param {string} runsFilePath - Remote path to subagents/runs.json
 * @returns {Promise<object>}
 */
async function buildRemoteSubagentMap(sftp, agentsDir, runsFilePath) {
  const map = {};

  const runsRaw = await ssh.readFileAsString(sftp, runsFilePath);
  if (!runsRaw) return map;

  let runs;
  try { runs = JSON.parse(runsRaw).runs || {}; } catch { return map; }

  const runIdToLabel = {};
  for (const [runId, run] of Object.entries(runs)) {
    if (run.label) runIdToLabel[runId] = run.label;
  }

  const mainSessionsPath = agentsDir + '/main/sessions';
  const sessionFiles = await ssh.listDir(sftp, mainSessionsPath);

  for (const entry of sessionFiles) {
    if (!entry.filename.includes('.jsonl')) continue;
    const content = await ssh.readFileAsString(sftp, mainSessionsPath + '/' + entry.filename);
    if (!content) continue;

    for (const line of content.split('\n')) {
      if (!line.includes('subagent task') || !line.includes('session_id')) continue;
      try {
        const obj = JSON.parse(line);
        if (obj.message?.role !== 'user') continue;
        const text     = JSON.stringify(obj.message.content);
        const sessionId = text.match(/session_id: ([a-f0-9-]{36})/)?.[1];
        const runId     = text.match(/"runId":"([^"]+)"/)?.[1]
                       || text.match(/runId: ([a-f0-9-]{36})/)?.[1];
        const childKey  = text.match(/agent:main:subagent:([a-f0-9-]{36})/)?.[1];

        let label = runId ? runIdToLabel[runId] : null;
        if (!label && childKey) {
          for (const run of Object.values(runs)) {
            if (run.childSessionKey?.includes(childKey)) { label = run.label; break; }
          }
        }
        if (sessionId && label) map[sessionId] = { label };
      } catch { continue; }
    }
  }

  return map;
}

// ── JSONL Parser (stream-based) ───────────────────────────────────────────────

/**
 * Parses a remote JSONL session file and returns token events.
 * Reads line by line via a Readable stream to avoid large memory allocations.
 *
 * @param {object} sftp
 * @param {string} remotePath
 * @param {string} nodeId
 * @param {string} agentId
 * @param {string} sessionId
 * @returns {Promise<Array>}
 */
async function parseRemoteSession(sftp, remotePath, nodeId, agentId, sessionId) {
  const events  = [];
  const stream  = ssh.createReadStream(sftp, remotePath);
  const rl      = readline.createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
    if (!line.trim()) continue;
    let obj;
    try { obj = JSON.parse(line); } catch { continue; }

    if (obj.type !== 'message') continue;
    const msg = obj.message;
    if (!msg?.usage) continue;

    const usage   = msg.usage;
    const cost    = usage.cost?.total ?? 0;
    const ts      = obj.timestamp ? new Date(obj.timestamp).getTime() : (obj.ts ?? 0);
    const rawId   = obj.id || crypto.createHash('sha1').update(remotePath + line).digest('hex');
    const eventId = `${nodeId}::${sessionId}::${rawId}`;

    events.push({
      id:                 eventId,
      node_id:            nodeId,
      agent_id:           agentId,
      session_id:         sessionId,
      ts,
      model:              msg.model || null,
      input_tokens:       usage.input        || 0,
      output_tokens:      usage.output       || 0,
      cache_read_tokens:  usage.cacheRead    || 0,
      cache_write_tokens: usage.cacheWrite   || 0,
      cost,
    });
  }

  return events;
}

// ── Node ingestion ────────────────────────────────────────────────────────────

/**
 * Connects to a single node, scans its .openclaw directory,
 * and inserts new token events into the local SQLite database.
 *
 * @param {object} db - node:sqlite DatabaseSync instance
 * @param {object} nodeConfig - Single entry from machines.json
 */
async function ingestNode(db, nodeConfig) {
  const { node_id, openclawPath } = nodeConfig;
  const agentsDir  = openclawPath + '/agents';
  const runsFile   = openclawPath + '/subagents/runs.json';

  let client, sftp;
  try {
    ({ client, sftp } = await ssh.connect(nodeConfig));
  } catch (err) {
    console.warn(`[ingestion] Node "${node_id}" is unreachable: ${err.message}`);
    return; // Isolated failure — other nodes continue
  }

  try {
    // Load identities and subagent map for this node
    const subagentMap = await buildRemoteSubagentMap(sftp, agentsDir, runsFile);
    const mapSize = Object.keys(subagentMap).length;
    if (mapSize > 0) {
      const sample = Object.entries(subagentMap).slice(0, 3).map(([k,v]) => `${k.slice(0,8)}→${v.label}`).join(', ');
      console.log(`[ingestion] Node "${node_id}": subagent map has ${mapSize} sessions (${sample}…)`);
    } else {
      // Disabled missing runs.json warning as we identified subagents have their own folders in this setup
      // console.log(`[ingestion] Node "${node_id}": no subagent map found (subagents/runs.json missing or empty)`);
    }

    // Prepare insert statement
    const insertEvent = db.prepare(`
      INSERT OR IGNORE INTO token_events
        (id, node_id, agent_id, session_id, ts, model, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, cost)
      VALUES
        (@id, @node_id, @agent_id, @session_id, @ts, @model, @input_tokens, @output_tokens, @cache_read_tokens, @cache_write_tokens, @cost)
    `);

    // Scan agent dirs
    const agentDirs = await ssh.listDir(sftp, agentsDir);
    let totalNew = 0;

    for (const entry of agentDirs) {
      if (!entry.attrs.isDirectory()) continue;
      const agentId      = entry.filename;
      const sessionsPath = agentsDir + '/' + agentId + '/sessions';
      const sessionFiles = await ssh.listDir(sftp, sessionsPath);

      for (const sf of sessionFiles) {
        // Include .deleted and .reset variants — real tokens were consumed in those sessions.
        // Only skip boot-* files (initialization events with no LLM usage).
        if (!sf.filename.includes('.jsonl')) continue;
        if (sf.filename.startsWith('boot-')) continue;
        const sessionId        = sf.filename.split('.')[0];
        const effectiveAgentId = subagentMap[sessionId]?.label || agentId;
        const remotePath       = sessionsPath + '/' + sf.filename;

        try {
          const events = await parseRemoteSession(sftp, remotePath, node_id, effectiveAgentId, sessionId);
          // node:sqlite uses manual BEGIN/COMMIT — no .transaction() helper
          db.exec('BEGIN DEFERRED TRANSACTION');
          try {
            for (const evt of events) {
              const info = insertEvent.run(evt);
              if (info.changes > 0) totalNew++;
            }
            db.exec('COMMIT');
          } catch (txErr) {
            db.exec('ROLLBACK');
            throw txErr;
          }
        } catch (parseErr) {
          console.warn(`[ingestion] Error parsing ${remotePath}:`, parseErr.message);
        }
      }
    }

    // Re-attribute subagent sessions previously stored as 'main'
    const updateAgent = db.prepare(
      'UPDATE token_events SET agent_id = ? WHERE node_id = ? AND session_id = ? AND agent_id != ?'
    );
    db.exec('BEGIN DEFERRED TRANSACTION');
    try {
      for (const [sessionId, { label }] of Object.entries(subagentMap)) {
        updateAgent.run(label, node_id, sessionId, label);
      }
      db.exec('COMMIT');
    } catch (reattrErr) {
      db.exec('ROLLBACK');
      console.warn(`[ingestion] Re-attribution failed for node "${node_id}": ${reattrErr.message}`);
    }

    if (totalNew > 0) {
      console.log(`[ingestion] Node "${node_id}": +${totalNew} new token events.`);
    }
  } catch (err) {
    console.error(`[ingestion] Error during ingestion of node "${node_id}":`, err.message);
  } finally {
    ssh.disconnect(client);
  }
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Runs a single ingestion cycle across all configured nodes.
 * Nodes are processed sequentially to avoid overwhelming the network.
 *
 * @param {object} db - node:sqlite DatabaseSync instance
 */
async function runIngestionNow(db) {
  const machines = loadMachinesConfig();
  if (machines.length === 0) {
    console.warn('[ingestion] No nodes configured in config/machines.json');
    return;
  }

  console.log(`[ingestion] Starting sync cycle for ${machines.length} node(s)…`);
  for (const nodeConfig of machines) {
    await ingestNode(db, nodeConfig);
  }
  console.log('[ingestion] Sync cycle complete.');
}

/**
 * Starts the background ingestion job.
 * Runs immediately on startup, then on the given interval.
 *
 * @param {object} db - node:sqlite DatabaseSync instance
 * @param {number} intervalMs - Milliseconds between sync cycles (default: 10 min)
 * @returns {NodeJS.Timeout} - Timer handle (can be cleared if needed)
 */
function startIngestionJob(db, intervalMs = 10 * 60 * 1000) {
  console.log(`[ingestion] Background job started (interval: ${intervalMs / 60_000} min)`);

  // First run immediately, without blocking server startup
  setImmediate(() => runIngestionNow(db).catch(console.error));

  return setInterval(() => runIngestionNow(db).catch(console.error), intervalMs);
}

/**
 * Loads the list of configured nodes (without sensitive data) for the API.
 * @returns {Array<{ node_id, label, host }>}
 */
function getNodeList() {
  return loadMachinesConfig().map(({ node_id, label, host }) => ({ node_id, label, host }));
}

/**
 * Loads remote agent identities for a given node (used by the stats API).
 * Opens and closes its own SFTP connection.
 *
 * @param {object} nodeConfig
 * @returns {Promise<object>} agentId → identity map
 */
async function loadNodeIdentities(nodeConfig) {
  let client, sftp;
  try {
    ({ client, sftp } = await ssh.connect(nodeConfig));
    const identities = await loadRemoteIdentities(sftp, nodeConfig.openclawPath);
    return identities;
  } catch (err) {
    console.warn(`[ingestion] Could not load identities for "${nodeConfig.node_id}":`, err.message);
    return {};
  } finally {
    if (client) ssh.disconnect(client);
  }
}

module.exports = { startIngestionJob, runIngestionNow, getNodeList, loadNodeIdentities, loadMachinesConfig };
