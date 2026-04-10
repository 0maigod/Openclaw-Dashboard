'use strict';

const { DatabaseSync }  = require('node:sqlite');
const express           = require('express');
const fs                = require('fs');
const path              = require('path');
const crypto            = require('crypto');
const ssh               = require('./src/sshService');
const ingestion         = require('./src/ingestionService');

// ── Config ────────────────────────────────────────────────────────────────────
const DB_FILE     = path.join(__dirname, 'db', 'openclaw-dashboard.sqlite');
const SCHEMA_FILE = path.join(__dirname, 'db', 'schema.sql');
const PORT        = 3131;

// ── Database setup ────────────────────────────────────────────────────────────
const db = new DatabaseSync(DB_FILE);
db.exec(fs.readFileSync(SCHEMA_FILE, 'utf8'));

// Prepared statements
const insertEvent = db.prepare(`
  INSERT OR IGNORE INTO token_events
    (id, node_id, agent_id, session_id, ts, model,
     input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, cost)
  VALUES
    (@id, @node_id, @agent_id, @session_id, @ts, @model,
     @input_tokens, @output_tokens, @cache_read_tokens, @cache_write_tokens, @cost)
`);

// ── Stats query ───────────────────────────────────────────────────────────────
/**
 * Returns consolidated stats from SQLite.
 * All SSH ingestion is done by the background job; this only reads local data.
 *
 * @param {object} opts
 * @param {string|null} opts.nodeId  - 'all' or a specific node_id
 * @param {number|null} opts.year
 * @param {number|null} opts.month
 * @param {number|null} opts.tsStart - Unix ms (overrides year/month)
 * @param {number|null} opts.tsEnd   - Unix ms (overrides year/month)
 */
function queryStats({ nodeId = 'all', year, month, tsStart, tsEnd } = {}) {
  // Resolve time range
  let start, end;
  if (tsStart && tsEnd) {
    start = parseInt(tsStart);
    end   = parseInt(tsEnd);
  } else if (year && month) {
    start = new Date(year, month - 1, 1).getTime();
    end   = new Date(year, month, 1).getTime();
  } else {
    const now = new Date();
    start = new Date(now.getFullYear(), now.getMonth(), 1).getTime();
    end   = new Date(now.getFullYear(), now.getMonth() + 1, 1).getTime();
  }

  // node filter clause
  const nodeFilter = nodeId && nodeId !== 'all' ? 'AND node_id = ?' : '';
  const nodeArgs   = nodeId && nodeId !== 'all' ? [nodeId] : [];

  const agentStats = db.prepare(`
    SELECT
      agent_id,
      COUNT(DISTINCT session_id) AS session_count,
      SUM(input_tokens)          AS input_tokens,
      SUM(output_tokens)         AS output_tokens,
      SUM(cache_read_tokens)     AS cache_read_tokens,
      SUM(input_tokens + output_tokens + cache_read_tokens + cache_write_tokens) AS total_tokens,
      SUM(cost)                  AS total_cost,
      MAX(ts)                    AS last_seen
    FROM token_events
    WHERE ts >= ? AND ts < ? ${nodeFilter}
    GROUP BY agent_id
    ORDER BY total_tokens DESC
  `).all(start, end, ...nodeArgs);

  const agentAllTime = db.prepare(`
    SELECT
      agent_id,
      SUM(input_tokens + output_tokens + cache_read_tokens + cache_write_tokens) AS total_tokens,
      SUM(cost) AS total_cost
    FROM token_events
    WHERE 1=1 ${nodeFilter}
    GROUP BY agent_id
  `).all(...nodeArgs);

  const agentModels = db.prepare(`
    SELECT agent_id, model, COUNT(*) AS cnt
    FROM token_events
    WHERE ts >= ? AND ts < ? AND model IS NOT NULL ${nodeFilter}
    GROUP BY agent_id, model
    ORDER BY cnt DESC
  `).all(start, end, ...nodeArgs);

  const modelByAgent = {};
  for (const row of agentModels) {
    if (!modelByAgent[row.agent_id]) modelByAgent[row.agent_id] = row.model;
  }

  const monthlyRows = db.prepare(`
    SELECT
      strftime('%Y-%m', datetime(ts/1000, 'unixepoch')) AS month,
      agent_id,
      SUM(input_tokens + output_tokens + cache_read_tokens) AS total_tokens,
      SUM(cost) AS total_cost
    FROM token_events
    WHERE ts >= ? ${nodeFilter}
    GROUP BY month, agent_id
    ORDER BY month ASC
  `).all(Date.now() - 6 * 30 * 24 * 3600 * 1000, ...nodeArgs);

  const dailyRows = db.prepare(`
    SELECT
      strftime('%Y-%m-%d', datetime(ts/1000, 'unixepoch')) AS day,
      agent_id,
      SUM(input_tokens + output_tokens + cache_read_tokens) AS total_tokens,
      SUM(cost) AS total_cost
    FROM token_events
    WHERE ts >= ? AND ts < ? ${nodeFilter}
    GROUP BY day, agent_id
    ORDER BY day ASC
  `).all(start, end, ...nodeArgs);

  const recentSessions = db.prepare(`
    SELECT
      node_id,
      agent_id,
      session_id,
      MIN(ts) AS started_at,
      MAX(ts) AS last_ts,
      COUNT(*) AS message_count,
      SUM(input_tokens + output_tokens) AS tokens,
      SUM(cost) AS cost
    FROM token_events
    WHERE 1=1 ${nodeFilter}
    GROUP BY session_id
    ORDER BY last_ts DESC
    LIMIT 20
  `).all(...nodeArgs);

  const availableMonths = db.prepare(`
    SELECT DISTINCT strftime('%Y-%m', datetime(ts/1000, 'unixepoch')) AS month
    FROM token_events
    WHERE 1=1 ${nodeFilter}
    ORDER BY month DESC
  `).all(...nodeArgs).map(r => r.month);

  const allTimeMap = {};
  for (const row of agentAllTime) allTimeMap[row.agent_id] = row;

  return {
    period: {
      year:  year  || new Date().getFullYear(),
      month: month || new Date().getMonth() + 1,
    },
    agents: agentStats.map(r => ({
      ...r,
      model:           modelByAgent[r.agent_id] || null,
      all_time_tokens: allTimeMap[r.agent_id]?.total_tokens || 0,
      all_time_cost:   allTimeMap[r.agent_id]?.total_cost   || 0,
    })),
    monthly:         monthlyRows,
    timeline:        dailyRows,
    recent_sessions: recentSessions,
    available_months: availableMonths,
  };
}

// ── Express app ───────────────────────────────────────────────────────────────
const rootApp = express();
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── GET /api/debug/agents ─────────────────────────────────────────────────────
// Temporary: shows raw agent list from a node's openclaw.json (no DB involved)
app.get('/api/debug/agents', async (req, res) => {
  const { node: nodeId = 'Frame02' } = req.query;
  const machines  = ingestion.loadMachinesConfig();
  const nodeConfig = machines.find(m => m.node_id === nodeId);
  if (!nodeConfig) return res.status(404).json({ error: 'Node not found' });

  let client, sftp;
  try {
    ({ client, sftp } = await ssh.connect(nodeConfig));
    const raw = await ssh.readFileAsString(sftp, nodeConfig.openclawPath + '/openclaw.json');
    const cfg = JSON.parse(raw);
    const agents = (cfg.agents?.list || []).map(a => ({
      id:       a.id,
      name:     a.name,
      identity: a.identity || null,
    }));
    res.json({ node: nodeId, count: agents.length, agents });
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    if (client) ssh.disconnect(client);
  }
});

// ── GET /api/nodes ─────────────────────────────────────────────────────────────
// Returns the list of configured nodes (without sensitive data).
app.get('/api/nodes', (_req, res) => {
  try {
    res.json({ nodes: ingestion.getNodeList() });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/stats ─────────────────────────────────────────────────────────────
// Serves pre-ingested stats directly from SQLite. No SSH calls here.
app.get('/api/stats', async (req, res) => {
  try {
    const { node = 'all', year, month, tsStart, tsEnd } = req.query;

    const stats = queryStats({
      nodeId:  node,
      year:    year   ? parseInt(year)   : null,
      month:   month  ? parseInt(month)  : null,
      tsStart: tsStart || null,
      tsEnd:   tsEnd   || null,
    });

    // Load identities from all relevant nodes for display enrichment
    const machines  = ingestion.loadMachinesConfig();
    const targetNodes = node === 'all'
      ? machines
      : machines.filter(m => m.node_id === node);

    const allIdentities = {};
    for (const nodeConfig of targetNodes) {
      const ids = await ingestion.loadNodeIdentities(nodeConfig);
      Object.assign(allIdentities, ids);
    }

    stats.agents = stats.agents.map(a => ({
      ...a,
      identity: allIdentities[a.agent_id] || { name: a.agent_id, emoji: '🤖' },
      // Prefer model recorded in actual DB events (ground truth) over openclaw.json config model
      model: a.model || allIdentities[a.agent_id]?.model || null,
    }));
    stats.recent_sessions = stats.recent_sessions.map(s => ({
      ...s,
      identity: allIdentities[s.agent_id] || { name: s.agent_id, emoji: '🤖' },
    }));

    res.json(stats);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/sync ─────────────────────────────────────────────────────────────
// Manual trigger for the ingestion job (for the "Refresh" button in UI).
app.post('/api/sync', async (_req, res) => {
  try {
    ingestion.runIngestionNow(db).catch(console.error);
    res.json({ ok: true, message: 'Sync started in background.' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /proyectos ─────────────────────────────────────────────────────────────
app.get('/proyectos', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'proyectos.html'));
});

// ── GET /api/projects ──────────────────────────────────────────────────────────
// Lists projects from a remote node via SFTP.
app.get('/api/projects', async (req, res) => {
  const { node: nodeId, project } = req.query;

  if (!nodeId) return res.status(400).json({ error: 'Missing ?node= parameter' });

  const machines  = ingestion.loadMachinesConfig();
  const nodeConfig = machines.find(m => m.node_id === nodeId);
  if (!nodeConfig) return res.status(404).json({ error: `Node "${nodeId}" not found in config` });

  const workspacePath = nodeConfig.openclawPath + '/workspace/proyectos';

  let client, sftp;
  try {
    ({ client, sftp } = await ssh.connect(nodeConfig));

    if (!project) {
      // Return list of project directories
      const entries = await ssh.listDir(sftp, workspacePath);
      const projects = entries
        .filter(e => e.attrs.isDirectory())
        .map(e => e.filename)
        .sort();
      return res.json({ projects });
    }

    // Return files for a specific project, grouped by agent
    const projectPath  = `${workspacePath}/${project}`;
    const filesByAgent = {};

    async function scanDir(remotePath, relStage, isBackup) {
      const entries = await ssh.listDir(sftp, remotePath);
      for (const entry of entries) {
        if (entry.filename.startsWith('.')) continue;
        const fullRemote = remotePath + '/' + entry.filename;

        if (entry.attrs.isDirectory()) {
          const enterBackup = isBackup || entry.filename === '00_backup';
          await scanDir(fullRemote, entry.filename, enterBackup);
        } else {
          const match = entry.filename.match(/^([^_]+)_/);
          const agent = match ? match[1].toLowerCase() : 'general';
          if (!filesByAgent[agent]) filesByAgent[agent] = [];
          filesByAgent[agent].push({
            name:   entry.filename,
            stage:  relStage || 'raiz',
            path:   project + (relStage ? '/' + relStage : '') + '/' + entry.filename,
            size:   entry.attrs.size,
            mtime:  entry.attrs.mtime * 1000,
            backup: isBackup || false,
          });
        }
      }
    }

    await scanDir(projectPath, null, false);

    for (const agent of Object.keys(filesByAgent)) {
      filesByAgent[agent].sort((a, b) => b.mtime - a.mtime);
    }

    res.json({ project, files: filesByAgent });
  } catch (err) {
    console.error('/api/projects error:', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    if (client) ssh.disconnect(client);
  }
});

// ── GET /api/file-content ──────────────────────────────────────────────────────
// Returns the raw text content of a remote project file for in-browser preview.
app.get('/api/file-content', async (req, res) => {
  const { node: nodeId, path: relPath } = req.query;

  if (!nodeId)  return res.status(400).json({ error: 'Missing ?node= parameter' });
  if (!relPath) return res.status(400).json({ error: 'Missing ?path= parameter' });

  const machines  = ingestion.loadMachinesConfig();
  const nodeConfig = machines.find(m => m.node_id === nodeId);
  if (!nodeConfig) return res.status(404).json({ error: `Node "${nodeId}" not found in config` });

  const workspacePath = nodeConfig.openclawPath + '/workspace/proyectos';
  const ext           = path.extname(relPath).toLowerCase();
  const ALLOWED_EXTS  = ['.md', '.txt', '.fountain', '.json'];

  if (!ALLOWED_EXTS.includes(ext)) {
    return res.status(415).json({ error: 'File type not supported for preview' });
  }

  // Security: prevent path traversal outside workspace/proyectos
  const normalized = path.posix.normalize(relPath);
  if (normalized.startsWith('..') || normalized.startsWith('/')) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  const remotePath = workspacePath + '/' + normalized;

  let client, sftp;
  try {
    ({ client, sftp } = await ssh.connect(nodeConfig));
    const content = await ssh.readFileAsString(sftp, remotePath);
    if (content === null) return res.status(404).json({ error: 'File not found' });
    res.json({ content, name: path.basename(relPath), ext });
  } catch (err) {
    console.error('/api/file-content error:', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    if (client) ssh.disconnect(client);
  }
});

// ── POST /api/file-content ─────────────────────────────────────────────────────
// Updates the remote project file via SFTP.
app.post('/api/file-content', async (req, res) => {
  const { node: nodeId, path: relPath } = req.query;
  const { content } = req.body;

  if (!nodeId)  return res.status(400).json({ error: 'Missing ?node= parameter' });
  if (!relPath) return res.status(400).json({ error: 'Missing ?path= parameter' });
  if (typeof content !== 'string') return res.status(400).json({ error: 'Missing content in body' });

  const machines  = ingestion.loadMachinesConfig();
  const nodeConfig = machines.find(m => m.node_id === nodeId);
  if (!nodeConfig) return res.status(404).json({ error: `Node "${nodeId}" not found in config` });

  const workspacePath = nodeConfig.openclawPath + '/workspace/proyectos';
  const ext           = path.extname(relPath).toLowerCase();
  const ALLOWED_EXTS  = ['.md', '.txt', '.fountain', '.json'];

  if (!ALLOWED_EXTS.includes(ext)) {
    return res.status(415).json({ error: 'File type not supported for saving' });
  }

  const normalized = path.posix.normalize(relPath);
  if (normalized.startsWith('..') || normalized.startsWith('/')) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  const remotePath = workspacePath + '/' + normalized;

  let client, sftp;
  try {
    ({ client, sftp } = await ssh.connect(nodeConfig));
    await ssh.writeFileAsString(sftp, remotePath, content);
    res.json({ ok: true });
  } catch (err) {
    console.error('/api/file-content POST error:', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    if (client) ssh.disconnect(client);
  }
});

// ── PUT /api/file-content ──────────────────────────────────────────────────────
// Renames a remote project file via SFTP.
app.put('/api/file-content', async (req, res) => {
  const { node: nodeId, path: relPath, newName } = req.query;

  if (!nodeId)  return res.status(400).json({ error: 'Missing ?node= parameter' });
  if (!relPath) return res.status(400).json({ error: 'Missing ?path= parameter' });
  if (!newName) return res.status(400).json({ error: 'Missing ?newName= parameter' });

  const machines  = ingestion.loadMachinesConfig();
  const nodeConfig = machines.find(m => m.node_id === nodeId);
  if (!nodeConfig) return res.status(404).json({ error: `Node "${nodeId}" not found in config` });

  const workspacePath = nodeConfig.openclawPath + '/workspace/proyectos';

  // Security: prevent path traversal
  const normalized = path.posix.normalize(relPath);
  if (normalized.startsWith('..') || normalized.startsWith('/')) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  // Create new rel path using the newName
  const dirName = path.dirname(normalized);
  const safeNewName = path.basename(newName); // Strip any subdirectories from newName to prevent trickery
  const newNormalized = dirName === '.' ? safeNewName : dirName + '/' + safeNewName;

  const oldRemotePath = workspacePath + '/' + normalized;
  const newRemotePath = workspacePath + '/' + newNormalized;

  let client, sftp;
  try {
    ({ client, sftp } = await ssh.connect(nodeConfig));
    await ssh.renameFile(sftp, oldRemotePath, newRemotePath);
    res.json({ ok: true, newPath: newNormalized });
  } catch (err) {
    console.error('/api/file-content PUT error:', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    if (client) ssh.disconnect(client);
  }
});

// ── DELETE /api/file-content ───────────────────────────────────────────────────
// Deletes a remote project file via SFTP.
app.delete('/api/file-content', async (req, res) => {
  const { node: nodeId, path: relPath } = req.query;

  if (!nodeId)  return res.status(400).json({ error: 'Missing ?node= parameter' });
  if (!relPath) return res.status(400).json({ error: 'Missing ?path= parameter' });

  const machines  = ingestion.loadMachinesConfig();
  const nodeConfig = machines.find(m => m.node_id === nodeId);
  if (!nodeConfig) return res.status(404).json({ error: `Node "${nodeId}" not found in config` });

  const workspacePath = nodeConfig.openclawPath + '/workspace/proyectos';

  // Security: prevent path traversal
  const normalized = path.posix.normalize(relPath);
  if (normalized.startsWith('..') || normalized.startsWith('/')) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  const remotePath = workspacePath + '/' + normalized;

  let client, sftp;
  try {
    ({ client, sftp } = await ssh.connect(nodeConfig));
    await ssh.deleteFile(sftp, remotePath);
    res.json({ ok: true });
  } catch (err) {
    console.error('/api/file-content DELETE error:', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    if (client) ssh.disconnect(client);
  }
});

// ── GET /api/agent-sessions ────────────────────────────────────────────────────
// Lists session JSONL files from a remote node via SFTP.
app.get('/api/agent-sessions', async (req, res) => {
  const { node: nodeId } = req.query;
  if (!nodeId) return res.status(400).json({ error: 'Missing ?node= parameter' });

  const machines  = ingestion.loadMachinesConfig();
  const nodeConfig = machines.find(m => m.node_id === nodeId);
  if (!nodeConfig) return res.status(404).json({ error: `Node "${nodeId}" not found in config` });

  const sessionsPath = nodeConfig.openclawPath + '/agents/main/sessions';

  let client, sftp;
  try {
    ({ client, sftp } = await ssh.connect(nodeConfig));
    const entries = await ssh.listDir(sftp, sessionsPath);
    const sessions = entries
      .filter(e => e.filename.endsWith('.jsonl'))
      .sort((a, b) => b.attrs.mtime - a.attrs.mtime)
      .map(e => e.filename);
    res.json({ sessions });
  } catch (err) {
    console.error('/api/agent-sessions error:', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    if (client) ssh.disconnect(client);
  }
});

// ── GET /api/agent-sessions/content ────────────────────────────────────────────
// Loads a remote JSONL session and maps it to the basic structures.
app.get('/api/agent-sessions/content', async (req, res) => {
  const { node: nodeId, file } = req.query;
  if (!nodeId || !file) return res.status(400).json({ error: 'Missing node or file' });

  const machines  = ingestion.loadMachinesConfig();
  const nodeConfig = machines.find(m => m.node_id === nodeId);
  if (!nodeConfig) return res.status(404).json({ error: 'Node not found' });

  const safeFile = path.basename(file);
  const remotePath = nodeConfig.openclawPath + '/agents/main/sessions/' + safeFile;

  let client, sftp;
  try {
    ({ client, sftp } = await ssh.connect(nodeConfig));
    const raw = await ssh.readFileAsString(sftp, remotePath);
    if (!raw) return res.status(404).json({ error: 'Session file not found' });
    
    const lines = raw.split('\n').filter(l => l.trim().length > 0);
    const messages = [];
    for (const line of lines) {
      try {
        const obj = JSON.parse(line);
        if (obj.type === 'message' && obj.message) {
          let textContent = '';
          let tool_calls = obj.message.tool_calls || [];
          
          if (Array.isArray(obj.message.content)) {
            const textParts = [];
            for (const part of obj.message.content) {
              if (part.type === 'text' && part.text) textParts.push(part.text);
              if (part.type === 'toolCall') {
                tool_calls.push({ function: { name: part.name, arguments: JSON.stringify(part.arguments) } });
              }
            }
            textContent = textParts.join('\n');
          } else {
            textContent = obj.message.content || '';
          }

          messages.push({
            ts: obj.timestamp || obj.ts || 0,
            role: obj.message.role,
            content: textContent,
            name: obj.message.name,
            tool_calls: tool_calls,
            cost: obj.message.usage?.cost?.total || 0,
            tokens: (obj.message.usage?.input || 0) + (obj.message.usage?.output || 0)
          });
        }
      } catch (e) { /* ignore parse error */ }
    }
    res.json({ messages });
  } catch (err) {
    console.error('/api/agent-sessions/content error:', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    if (client) ssh.disconnect(client);
  }
});

// ── POST /api/projects (legacy placeholder) ───────────────────────────────────
app.post('/api/projects', (req, res) => {
  const { title, description, status, agent_id } = req.body;
  const now = Date.now();
  const id  = crypto.randomUUID();
  db.prepare(
    `INSERT INTO projects (id, title, description, status, agent_id, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)`
  ).run(id, title, description || null, status || 'backlog', agent_id || null, now, now);
  res.json({ id });
});

// ── Bootstrap ─────────────────────────────────────────────────────────────────
rootApp.use('/openclaw-dashboard', app);
rootApp.get('/', (req, res) => res.redirect('/openclaw-dashboard/'));

rootApp.listen(PORT, () => {
  console.log(`✅ Dashboard running at http://localhost:${PORT}/openclaw-dashboard/`);
  // Start background SSH ingestion job (non-blocking)
  ingestion.startIngestionJob(db, 10 * 60 * 1000);
});
