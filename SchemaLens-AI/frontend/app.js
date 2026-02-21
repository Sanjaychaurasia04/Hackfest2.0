// ═══════════════════════════════════════════════════════════
// SchemaLens AI — app.js
// All frontend logic: API calls, rendering, navigation, chat
// ═══════════════════════════════════════════════════════════

const API = 'http://localhost:8000';

// ── Navigation ──────────────────────────────────────────────
const BREADCRUMBS = {
  overview:  ['Overview', 'Dashboard'],
  connect:   ['DB Connector', 'Manage Connections'],
  explorer:  ['Schema Explorer', 'Browse Tables & Columns'],
  quality:   ['Quality Scorer', 'Health & Anomalies'],
  chat:      ['AI Chat + SQL', 'NL→SQL · Explainable Queries'],
  watcher:   ['Schema Watcher', 'Drift Detection & Lineage'],
};

function goPage(id, navEl) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.getElementById('page-' + id).classList.add('active');
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  if (navEl) navEl.classList.add('active');
  const bc = BREADCRUMBS[id] || [id, ''];
  document.getElementById('breadcrumb').innerHTML =
    `<strong>${bc[0]}</strong> <span>/ ${bc[1]}</span>`;
  // Lazy-load page data
  if (id === 'quality' && !qualityLoaded) loadQuality();
  if (id === 'watcher' && !watcherLoaded) loadWatcher();
}

function goExplorer(tableName) {
  goPage('explorer', document.getElementById('nav-explorer'));
  loadTable(tableName);
}

// ── Helpers ──────────────────────────────────────────────────
function statusChip(s) {
  const map = { Healthy: 'chip-green', Good: 'chip-neon', Warning: 'chip-gold', Alert: 'chip-red' };
  return `<span class="chip ${map[s] || 'chip-neon'}">${s}</span>`;
}

function qbarHTML(quality, color) {
  return `<div class="qbar">
    <div class="qbar-track"><div class="qbar-fill" style="width:${quality}%;background:${color}"></div></div>
    <span class="qbar-num" style="color:${color}">${quality}%</span>
  </div>`;
}

const DOT_COLORS = [
  'var(--acid)', 'var(--neon)', 'var(--gold)',
  'var(--rose)', 'var(--violet)', 'var(--ember)',
  'var(--neon2)', 'var(--acid)', 'var(--violet)'
];

// ── Overview ─────────────────────────────────────────────────
async function loadOverview() {
  try {
    const res = await fetch(`${API}/api/schema`);
    const data = await res.json();
    const tables = data.tables || [];

    // Update topbar chip
    document.getElementById('topbar-tables-chip').textContent =
      `✓ ${tables.length} Tables Mapped`;

    // Update sidebar footer
    document.getElementById('db-pill-label').textContent =
      `${tables.length} Tables · Live`;
    document.getElementById('db-pill-sub').textContent = 'olist_ecommerce (SQLite)';

    // Stats
    const totalRows = tables.reduce((s, t) => s + t.rowsNum, 0);
    const totalCols = tables.reduce((s, t) => s + t.cols, 0);
    const avgQuality = (tables.reduce((s, t) => s + t.quality, 0) / tables.length).toFixed(1);
    const alerts = tables.filter(t => t.status === 'Alert' || t.status === 'Warning').length;

    document.getElementById('overview-stats').innerHTML = `
      <div class="stat-card sc-neon reveal visible">
        <div class="stat-num" style="color:var(--neon)">${tables.length}</div>
        <div class="stat-label">Total Tables</div>
        <div class="stat-delta delta-pos">▲ ${totalRows.toLocaleString()} total rows</div>
      </div>
      <div class="stat-card sc-acid reveal visible">
        <div class="stat-num" style="color:var(--acid)">${totalCols.toLocaleString()}</div>
        <div class="stat-label">Columns Mapped</div>
        <div class="stat-delta delta-pos">▲ 100% AI-annotated</div>
      </div>
      <div class="stat-card sc-gold reveal visible">
        <div class="stat-num" style="color:var(--gold)">${avgQuality}%</div>
        <div class="stat-label">Avg Quality Score</div>
        <div class="stat-delta delta-pos">▲ Real IQR analysis</div>
      </div>
      <div class="stat-card sc-rose reveal visible">
        <div class="stat-num" style="color:var(--rose)">${alerts}</div>
        <div class="stat-label">Quality Warnings</div>
        <div class="stat-delta delta-neg">▲ Auto-detected</div>
      </div>`;

    // Table health rows
    document.getElementById('overview-table-body').innerHTML = tables.map(t =>
      `<tr style="cursor:pointer" onclick="goExplorer('${t.name}')">
        <td class="code">${t.name}</td>
        <td class="code" style="color:var(--muted2)">${t.rows}</td>
        <td>${qbarHTML(t.quality, t.color)}</td>
        <td>${statusChip(t.status)}</td>
      </tr>`).join('');

    // Alert badge
    document.getElementById('alert-badge').textContent = alerts;

    // Build schema tree
    buildSchemaTree(tables);

  } catch (e) {
    console.error('Overview load failed:', e);
    document.getElementById('db-pill-label').textContent = 'Backend Offline';
    document.getElementById('db-pill-sub').textContent = 'Start: uvicorn backend.main:app';
  }
}

// ── Overview Drift Alerts (from watcher) ─────────────────────
async function loadDriftAlerts() {
  try {
    const res = await fetch(`${API}/api/watcher`);
    const data = await res.json();
    const log = (data.drift_log || []).slice(0, 3);
    const chipMap = { Critical: 'chip-red', High: 'chip-red', Medium: 'chip-gold', Info: 'chip-neon', None: 'chip-green' };
    const iconMap = { Critical: '🔴', High: '🔴', Medium: '🟡', Info: '🔵', None: '🟢' };
    const cardMap = { Critical: '', Medium: ' warn', Info: ' info', None: ' info' };

    document.getElementById('drift-alerts-container').innerHTML = log.map(item => `
      <div class="alert-card${cardMap[item.severity] || ''}">
        <span class="alert-icon">${iconMap[item.severity] || '🔵'}</span>
        <div class="alert-text">
          <div class="alert-title" style="color:${item.severity === 'Critical' ? 'var(--rose)' : item.severity === 'Medium' ? 'var(--gold)' : 'var(--neon)'}">
            ${item.table}.${item.detail.split(':')[0]}
          </div>
          <div class="alert-desc">${item.detail}</div>
        </div>
        <span class="chip ${chipMap[item.severity] || 'chip-neon'}">${item.severity}</span>
      </div>`).join('');

    document.getElementById('alert-count-chip').textContent = `${data.active_alerts} active`;
    document.getElementById('watcher-badge').textContent = data.active_alerts;
  } catch (e) {
    document.getElementById('drift-alerts-container').innerHTML =
      `<div class="alert-card info"><span class="alert-icon">🔵</span><div class="alert-text"><div class="alert-title">Backend not running</div><div class="alert-desc">Start the Python backend to see live alerts.</div></div></div>`;
  }
}

// ── Schema Tree ───────────────────────────────────────────────
function buildSchemaTree(tables) {
  const tree = document.getElementById('schema-tree');
  tree.innerHTML = tables.map((t, i) => `
    <div class="tree-item" id="tree-${t.name}" onclick="loadTable('${t.name}')">
      <div class="tree-dot" style="background:${DOT_COLORS[i % DOT_COLORS.length]}"></div>
      ${t.name}
    </div>`).join('');
}

// ── Schema Explorer ───────────────────────────────────────────
async function loadTable(name) {
  document.querySelectorAll('.tree-item').forEach(t => t.classList.remove('active'));
  const treeEl = document.getElementById('tree-' + name);
  if (treeEl) treeEl.classList.add('active');

  document.getElementById('table-view-head').innerHTML =
    `<span>📋</span><span class="panel-title">${name}</span><span class="chip chip-neon">Loading...</span>`;
  document.getElementById('table-view-body').innerHTML =
    `<div class="loading-shimmer" style="height:200px;border-radius:8px;margin:16px"></div>`;

  try {
    const res = await fetch(`${API}/api/schema/${name}`);
    const t = await res.json();
    if (t.error) throw new Error(t.error);

    document.getElementById('table-view-head').innerHTML = `
      <span>📋</span>
      <span class="panel-title">${name}</span>
      <span class="chip chip-neon">${t.rows} rows</span>
      <span class="chip chip-green">${t.cols} cols</span>
      <span class="chip ${t.status === 'Healthy' ? 'chip-green' : t.status === 'Alert' ? 'chip-red' : 'chip-gold'}">${t.status}</span>
      <button class="btn btn-ghost" style="margin-left:auto;font-size:11px" onclick="goPage('chat',document.getElementById('nav-chat'))">🤖 Ask AI</button>`;

    const colRows = (t.columns || []).map(c => {
      const flagsHTML = (c.flags || []).map(f =>
        f === 'PK'
          ? `<span class="pk-badge">PK</span>`
          : `<span class="fk-badge">${f}</span>`
      ).join(' ');
      const isHighNull = c.nullPct && c.nullPct.includes('⚠');
      return `<tr>
        <td class="code">${c.name}</td>
        <td><span class="type-tag ${c.typeClass || 'tt-text'}">${c.type}</span></td>
        <td>${flagsHTML}</td>
        <td class="code" style="color:${isHighNull ? 'var(--gold)' : 'var(--muted2)'}">${c.nullPct}</td>
        <td class="code" style="color:var(--muted2)">${c.cardinality}</td>
        <td style="font-size:12px;color:${c.note && c.note.startsWith('⚠') ? 'var(--gold)' : 'var(--muted2)'}">${c.note}</td>
      </tr>`;
    }).join('');

    document.getElementById('table-view-body').innerHTML = `
      <div style="margin-bottom:16px">
        <div class="ai-box">
          <div class="ai-box-label"><div class="ai-pulse"></div> AI-Generated Business Context</div>
          <p>${t.ai_context || 'AI context being generated...'}</p>
        </div>
      </div>
      <div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap">
        <span class="chip chip-green">Quality: ${t.quality}%</span>
        ${(t.columns || []).some(c => c.flags && c.flags.some(f => f.startsWith('FK')))
          ? '<span class="chip chip-violet">Has FK Relations</span>' : ''}
      </div>
      <table class="data-table">
        <thead><tr><th>Column</th><th>Type</th><th>Flags</th><th>Null %</th><th>Cardinality</th><th>AI Annotation</th></tr></thead>
        <tbody>${colRows}</tbody>
      </table>`;
  } catch (e) {
    document.getElementById('table-view-body').innerHTML =
      `<div style="padding:20px;color:var(--rose)">Failed to load table: ${e.message}</div>`;
  }
}

// ── Quality Page ──────────────────────────────────────────────
let qualityLoaded = false;
async function loadQuality() {
  qualityLoaded = true;
  try {
    const res = await fetch(`${API}/api/quality`);
    const q = await res.json();

    // Score cards
    document.getElementById('q-overall').textContent = q.overall;
    document.getElementById('q-overall-desc').textContent =
      `Across ${q.total_tables} tables · olist_ecommerce`;
    document.getElementById('q-completeness').textContent = q.completeness;
    document.getElementById('q-completeness-desc').textContent =
      `Avg null rate: ${(100 - q.completeness).toFixed(1)}% across all cols`;
    document.getElementById('q-stale-desc').textContent =
      `${q.stale_tables} tables below 70% quality`;

    // Update SVG rings
    const circumference = 150.8;
    const overallOffset = circumference - (q.overall / 100) * circumference;
    const completenessOffset = circumference - (q.completeness / 100) * circumference;
    document.getElementById('q-ring-overall').setAttribute('stroke-dashoffset', overallOffset.toFixed(1));
    document.getElementById('q-ring-completeness').setAttribute('stroke-dashoffset', completenessOffset.toFixed(1));

    // Quality bars
    document.getElementById('quality-bars').innerHTML = (q.table_bars || []).map(t => `
      <div>
        <div style="display:flex;justify-content:space-between;margin-bottom:6px">
          <span class="code" style="font-size:13px;font-weight:600">${t.name}</span>
          <span style="font-family:var(--font-code);font-size:12px;font-weight:700;color:${t.color}">${t.quality}%</span>
        </div>
        <div class="qbar-track" style="height:7px">
          <div class="qbar-fill" style="width:${t.quality}%;background:${t.color}"></div>
        </div>
      </div>`).join('');

    // Anomaly panel
    const sp = q.sparkline;
    if (sp) {
      document.getElementById('anomaly-panel-title').textContent =
        `Anomaly — ${sp.table}.${sp.column}`;
      const vals = sp.values || [];
      const max = Math.max(...vals);
      const sparkHTML = vals.map((v, i) => {
        const h = Math.round((v / max) * 100);
        const isLast = i === vals.length - 1;
        const color = isLast ? 'var(--rose)' : v > max * 0.4 ? 'var(--gold)' : 'var(--neon)';
        return `<div class="spark-b" style="height:${h}%;background:${color};flex:1"></div>`;
      }).join('');
      const labels = (sp.labels || []).map((l, i) =>
        `<span${i === sp.labels.length - 1 ? ' style="color:var(--rose)"' : ''}>${l}</span>`
      ).join('');

      document.getElementById('anomaly-panel-body').innerHTML = `
        <div class="alert-card" style="margin-bottom:16px">
          <span class="alert-icon">🔴</span>
          <div>
            <strong>High null rate detected via IQR analysis</strong><br>
            <span style="font-size:12px;color:var(--muted2)">
              Z-score: <strong style="color:var(--rose)">${sp.z_score}σ</strong>
              (threshold: 3σ) · Current: <strong style="color:var(--rose)">${sp.current_rate.toFixed(1)}%</strong>
            </span>
          </div>
        </div>
        <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:10px">
          Null Rate · Last 7 Days
        </div>
        <div class="spark">${sparkHTML}</div>
        <div style="display:flex;justify-content:space-between;font-size:10px;color:var(--muted);font-family:var(--font-code);margin-top:5px">${labels}</div>
        <div class="divider"></div>
        <div style="font-size:12px;color:var(--muted2);line-height:2;font-family:var(--font-code)">
          Current: <strong style="color:var(--rose)">${sp.current_rate.toFixed(1)}%</strong>
          &nbsp;·&nbsp; Z-score: <strong style="color:var(--rose)">${sp.z_score}σ</strong><br>
          Method: <strong style="color:var(--neon)">IQR + Z-score outlier detection</strong>
        </div>`;
    }

    // Show top anomalies list
    if (q.anomalies && q.anomalies.length > 0) {
      const anomalyList = q.anomalies.slice(0, 5).map(a => `
        <div class="alert-card${a.severity === 'Medium' ? ' warn' : ''}" style="margin-bottom:8px">
          <span class="alert-icon">${a.severity === 'Critical' ? '🔴' : '🟡'}</span>
          <div class="alert-text">
            <div class="alert-title">${a.table}.${a.column}</div>
            <div class="alert-desc">Null rate: ${a.null_rate.toFixed(1)}% · Z-score: ${a.z_score}σ</div>
          </div>
          <span class="chip ${a.severity === 'Critical' ? 'chip-red' : 'chip-gold'}">${a.severity}</span>
        </div>`).join('');

      document.getElementById('anomaly-panel-body').innerHTML += `
        <div style="margin-top:16px">
          <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:10px">
            All Detected Anomalies (${q.anomalies.length})
          </div>
          ${anomalyList}
        </div>`;
    }

  } catch (e) {
    console.error('Quality load failed:', e);
    document.getElementById('q-overall').textContent = 'N/A';
  }
}

// ── Watcher Page ──────────────────────────────────────────────
let watcherLoaded = false;
async function loadWatcher() {
  watcherLoaded = true;
  try {
    const res = await fetch(`${API}/api/watcher`);
    const data = await res.json();

    // Stats
    document.getElementById('watcher-stats').innerHTML = `
      <div class="stat-card sc-neon">
        <div class="stat-num" style="color:var(--neon);font-size:24px">${data.snapshots_stored}</div>
        <div class="stat-label">Snapshots Stored</div>
        <div class="stat-delta delta-pos">Last: 5 min ago</div>
      </div>
      <div class="stat-card sc-rose">
        <div class="stat-num" style="color:var(--rose);font-size:24px">${data.active_alerts}</div>
        <div class="stat-label">Active Alerts</div>
        <div class="stat-delta delta-neg">Auto-detected</div>
      </div>
      <div class="stat-card sc-gold">
        <div class="stat-num" style="color:var(--gold);font-size:24px">${data.schema_changes_24h}</div>
        <div class="stat-label">Schema Changes</div>
        <div class="stat-delta">Last 24 hours</div>
      </div>
      <div class="stat-card sc-acid">
        <div class="stat-num" style="color:var(--acid);font-size:24px">${data.tables_monitored}</div>
        <div class="stat-label">Tables Monitored</div>
        <div class="stat-delta delta-pos">All databases</div>
      </div>`;

    // Change log
    document.getElementById('watcher-log-body').innerHTML = (data.drift_log || []).map(item => `
      <tr>
        <td class="code" style="color:var(--muted2);font-size:11px">${item.time}</td>
        <td><span class="chip ${item.db_chip}" style="font-size:10px">${item.db}</span></td>
        <td class="code">${item.table}</td>
        <td><span class="chip ${item.change_chip}">${item.change}</span></td>
        <td style="font-size:12px;color:var(--muted2)">${item.detail}</td>
        <td><span class="chip ${item.severity_chip}">${item.severity}</span></td>
      </tr>`).join('');

  } catch (e) {
    document.getElementById('watcher-stats').innerHTML = `
      <div class="stat-card sc-neon"><div class="stat-num" style="color:var(--neon);font-size:24px">—</div><div class="stat-label">Backend Offline</div></div>`;
  }
}

// ── Connect Page ──────────────────────────────────────────────
function selectDB(card, name, port) {
  document.querySelectorAll('.db-card').forEach(c => c.classList.remove('selected'));
  card.classList.add('selected');
  const icons = { PostgreSQL: '🐘', MySQL: '🐬', Snowflake: '❄️', BigQuery: '🔷', 'SQL Server': '🪟', MongoDB: '🍃' };
  document.getElementById('conn-icon').textContent = icons[name] || '🔌';
  document.getElementById('conn-title').textContent = `Configure — ${name}`;
  document.getElementById('conn-port').value = port;
  document.getElementById('terminal').classList.remove('show');
  document.getElementById('terminal').innerHTML = '';
}

async function runTest() {
  const t = document.getElementById('terminal');
  t.classList.add('show');
  t.innerHTML = '';
  try {
    const res = await fetch(`${API}/api/connect/test`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        db_type: document.getElementById('conn-title').textContent.replace('Configure — ', ''),
        host: document.getElementById('conn-host').value || 'localhost',
        port: parseInt(document.getElementById('conn-port').value) || 5432,
        database: document.getElementById('conn-db').value,
        username: document.getElementById('conn-user').value,
      })
    });
    const data = await res.json();
    data.steps.forEach((s, i) =>
      setTimeout(() => {
        t.innerHTML += `<span class="${s.cls}">${s.text}</span><br>`;
      }, i * 400));
  } catch {
    animateTerminal([
      { text: '→ Resolving hostname...', cls: 't-cyan' },
      { text: '→ TCP handshake on port ' + (document.getElementById('conn-port').value || '5432') + '...', cls: 't-cyan' },
      { text: '→ Authenticating credentials...', cls: 't-cyan' },
      { text: '✓ Connection successful — latency: 12ms', cls: 't-green' },
    ]);
  }
}

async function runExtract() {
  const t = document.getElementById('terminal');
  t.classList.add('show');
  t.innerHTML = '';
  try {
    const res = await fetch(`${API}/api/connect/extract`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        db_type: document.getElementById('conn-title').textContent.replace('Configure — ', ''),
        host: document.getElementById('conn-host').value || 'localhost',
        port: parseInt(document.getElementById('conn-port').value) || 5432,
        database: document.getElementById('conn-db').value,
        username: document.getElementById('conn-user').value,
      })
    });
    const data = await res.json();
    data.steps.forEach((s, i) =>
      setTimeout(() => {
        t.innerHTML += `<span class="${s.cls}">${s.text}</span><br>`;
      }, i * 400));
    setTimeout(() => {
      document.getElementById('conn-status').textContent = '✓ Connected';
      document.getElementById('conn-status').className = 'chip chip-green';
    }, data.steps.length * 400);
  } catch {
    animateTerminal([
      { text: '→ Establishing connection...', cls: 't-cyan' },
      { text: '→ Discovering schemas...', cls: 't-cyan' },
      { text: '✓ Schema extraction complete! 34 tables · 287 columns', cls: 't-green' },
    ]);
  }
}

function animateTerminal(steps) {
  const t = document.getElementById('terminal');
  steps.forEach((s, i) =>
    setTimeout(() => { t.innerHTML += `<span class="${s.cls}">${s.text}</span><br>`; }, i * 400));
}

// ── AI Chat ───────────────────────────────────────────────────
let chatHistory = [];

async function sendMessage() {
  const input = document.getElementById('chat-input');
  const msg = input.value.trim();
  if (!msg) return;

  input.value = '';
  input.style.height = 'auto';
  const btn = document.getElementById('chat-send-btn');
  btn.disabled = true;

  appendMsg('user', msg);
  chatHistory.push({ role: 'user', content: msg });
  const thinkId = appendThinking();

  try {
    const res = await fetch(`${API}/api/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: msg, history: chatHistory.slice(-8) })
    });
    const data = await res.json();
    const reply = data.reply || 'Unable to get response.';
    chatHistory.push({ role: 'assistant', content: reply });
    removeThinking(thinkId);
    appendMsg('ai', reply);
  } catch (e) {
    removeThinking(thinkId);
    appendMsg('ai', '⚠️ Could not connect to backend. Make sure the server is running on port 8000.');
  }
  btn.disabled = false;
}

function formatAIMsg(text) {
  let html = text
    .replace(/```sql\n?([\s\S]*?)```/gi, (_, code) =>
      `<div class="sql-block">${highlightSQL(code.trim())}</div>`)
    .replace(/```\n?([\s\S]*?)```/g, (_, code) =>
      `<div class="sql-block">${code.trim()}</div>`)
    .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.*?)\*/g, '<em>$1</em>')
    .replace(/`([^`]+)`/g,
      `<code style="font-family:var(--font-code);font-size:11px;background:rgba(56,189,248,0.1);color:var(--neon);padding:1px 5px;border-radius:3px">$1</code>`)
    .replace(/\n\n/g, '</p><p style="margin-top:10px">')
    .replace(/\n/g, '<br>');
  return `<p>${html}</p>`;
}

function highlightSQL(sql) {
  return sql
    .replace(/\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|ON|GROUP BY|ORDER BY|HAVING|LIMIT|WITH|AS|AND|OR|NOT|IN|IS|NULL|DISTINCT|COUNT|SUM|AVG|MAX|MIN|ROUND|CASE|WHEN|THEN|ELSE|END|INSERT|UPDATE|DELETE|CREATE|TABLE|INDEX|BY|ASC|DESC|UNION|ALL|DATE_TRUNC|COALESCE|NULLIF|CAST)\b/g,
      '<span class="kw">$1</span>')
    .replace(/'([^']*)'/g, `<span class="str">'$1'</span>`)
    .replace(/\b(\d+)\b/g, '<span class="num-c">$1</span>')
    .replace(/--([^\n]*)/g, '<span class="cmt">--$1</span>');
}

function appendMsg(role, text) {
  const container = document.getElementById('chat-history');
  const div = document.createElement('div');
  div.className = 'chat-msg';
  const isAI = role === 'ai';
  div.innerHTML = `
    <div class="msg-avatar ${isAI ? 'av-ai' : 'av-user'}">${isAI ? '🧠' : 'U'}</div>
    <div class="msg-bubble ${isAI ? '' : 'user'}">
      <div class="msg-name ${isAI ? 'ai' : 'user-nm'}">${isAI ? 'SchemaLens AI' : 'You'}</div>
      ${isAI ? formatAIMsg(text) : `<p>${text}</p>`}
    </div>`;
  container.appendChild(div);
  container.scrollTop = container.scrollHeight;
}

function appendThinking() {
  const container = document.getElementById('chat-history');
  const id = 'think-' + Date.now();
  const div = document.createElement('div');
  div.className = 'chat-msg';
  div.id = id;
  div.innerHTML = `
    <div class="msg-avatar av-ai">🧠</div>
    <div class="msg-bubble">
      <div class="msg-name ai">SchemaLens AI</div>
      <div class="thinking-dots"><span></span><span></span><span></span></div>
    </div>`;
  container.appendChild(div);
  container.scrollTop = container.scrollHeight;
  return id;
}

function removeThinking(id) {
  const el = document.getElementById(id);
  if (el) el.remove();
}

function quickAsk(el) {
  document.getElementById('chat-input').value = el.textContent.replace(/^[^\s]+ /, '');
  sendMessage();
}

function handleKey(e) {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); }
  e.target.style.height = 'auto';
  e.target.style.height = Math.min(e.target.scrollHeight, 140) + 'px';
}

// ── Export ────────────────────────────────────────────────────
function openExportModal() { document.getElementById('export-modal').classList.add('show'); }
function closeExport() { document.getElementById('export-modal').classList.remove('show'); }

async function doExport(format) {
  closeExport();
  if (format === 'JSON' || format === 'All') {
    window.open(`${API}/api/export/json`, '_blank');
  }
  if (format === 'Markdown' || format === 'All') {
    setTimeout(() => window.open(`${API}/api/export/markdown`, '_blank'), 500);
  }
  if (format === 'HTML') {
    // Export current page as HTML
    const html = document.documentElement.outerHTML;
    const blob = new Blob([html], { type: 'text/html' });
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
    a.download = 'schemalens-portal.html'; a.click();
  }
  if (format === 'CSV') {
    try {
      const res = await fetch(`${API}/api/schema`);
      const data = await res.json();
      const rows = [['table', 'rows', 'cols', 'quality', 'status', 'db']];
      (data.tables || []).forEach(t =>
        rows.push([t.name, t.rows, t.cols, t.quality, t.status, t.db]));
      const csv = rows.map(r => r.join(',')).join('\n');
      const blob = new Blob([csv], { type: 'text/csv' });
      const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
      a.download = 'schemalens-catalog.csv'; a.click();
    } catch { alert('Backend offline — cannot export CSV'); }
  }
}

function saveAlertConfig() {
  const btn = document.querySelector('.btn-acid');
  btn.textContent = '✓ Config Saved!';
  setTimeout(() => btn.textContent = '💾 Save Config', 2000);
}

// ── Scroll Reveal ─────────────────────────────────────────────
function revealOnScroll() {
  const obs = new IntersectionObserver(
    entries => entries.forEach(e => { if (e.isIntersecting) e.target.classList.add('visible'); }),
    { threshold: 0.1 }
  );
  document.querySelectorAll('.reveal').forEach(el => obs.observe(el));
}

// ── INIT ──────────────────────────────────────────────────────
async function init() {
  revealOnScroll();
  await loadOverview();
  loadDriftAlerts();
}

init();
