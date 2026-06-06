require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const fetch = require('node-fetch');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const db = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const ADMIN_PATH = process.env.ADMIN_PATH || 'admin-secret';
const GTFS_RT_URL = process.env.GTFS_RT_URL;
const ROUTE_ID = '379-4799';

// 379 stop sequence (index 0-19, game ends at 19)
const STOPS = [
  "Royal Pde, Ashgrove",
  "Waterworks Rd / Monoplane St",
  "Glory St, West Ashgrove",
  "Waterworks Rd / Myagh Rd",
  "Waterworks Rd / Girraween Gr",
  "Waterworks Rd / Hibiscus Ave",
  "Waterworks Rd / Elimatta Dr",
  "Waterworks Rd, Ashgrove",
  "Waterworks Rd / St Finbarr's",
  "Waterworks Rd / Woodland St",
  "Waterworks Rd / Boon St",
  "Waterworks Rd / Mossvale St",
  "Waterworks Rd / Whitta St",
  "Waterworks Rd / Glenrosa Rd",
  "Waterworks Rd / Cairns St",
  "Musgrave Rd, Red Hill Shops",
  "Musgrave Rd / Hammond St",
  "Musgrave Rd / Upper Clifton Tce",
  "Musgrave Rd, Normanby Fiveways",
  "Musgrave Rd / Normanby Hotel"
];

// Stop IDs from GTFS for the 379 inbound
const STOP_IDS = [
  1518, 1519, 1515, 1517, 1505, 1499, 1490, 10160,
  1487, 1486, 1965, 2038, 2036, 2033, 2032, 2028,
  838, 836, 888, 868
];

const CARD_RANKS = ['2','3','4','5','6','7','8','9','10','J','Q','K','A'];
const CARD_SUITS = ['♠','♥','♦','♣'];
const MAX_PLAYERS = 52;

// ─── In-memory game state ─────────────────────────────────────────────────────

let gameState = {
  sessionId: null,
  status: 'waiting',       // waiting | active | complete
  currentStopIndex: -1,
  playerCount: 0,
  aliveCount: 0,
  winProbability: 50,
  elimRules: [],
  lastEvent: null
};

// ─── Utility ──────────────────────────────────────────────────────────────────

function dealCard(usedCards) {
  const deck = [];
  for (const r of CARD_RANKS)
    for (const s of CARD_SUITS)
      deck.push({ rank: r, suit: s });

  const available = deck.filter(c =>
    !usedCards.some(u => u.rank === c.rank && u.suit === c.suit)
  );

  if (available.length === 0) return { rank: 'A', suit: '♠' };
  return available[Math.floor(Math.random() * available.length)];
}

function cardKilled(rank, suit, rule) {
  if (rule.kills_suit && rule.kills_suit === suit) return true;
  if (rule.kills_ranks) {
    const ranks = rule.kills_ranks.split(',');
    if (ranks.includes(rank)) return true;
  }
  return false;
}

// ─── GTFS-RT Stop Detection ───────────────────────────────────────────────────

let lastProcessedStop = -1;

async function pollGTFS() {
  if (!gameState.sessionId || gameState.status !== 'active') return;

  try {
    const GtfsRT = require('gtfs-realtime-bindings');
    const res = await fetch(GTFS_RT_URL);
    if (!res.ok) return;
    const buffer = await res.arrayBuffer();
    const feed = GtfsRT.transit_realtime.FeedMessage.decode(new Uint8Array(buffer));

    // Find 379 vehicles
    for (const entity of feed.entity) {
      if (!entity.vehicle) continue;
      const routeId = entity.vehicle.trip && entity.vehicle.trip.routeId;
      if (routeId !== ROUTE_ID) continue;

      const stopSeq = entity.vehicle.currentStopSequence;
      // GTFS stop_sequence is 1-based, our index is 0-based
      const stopIndex = stopSeq - 2; // adjust for trip starting at seq 2

      if (stopIndex > lastProcessedStop && stopIndex < STOPS.length) {
        lastProcessedStop = stopIndex;
        await processStopEvent(stopIndex);
      }
    }
  } catch (err) {
    console.error('GTFS poll error:', err.message);
  }
}

// ─── Stop Event Processing ────────────────────────────────────────────────────

async function processStopEvent(stopIndex) {
  console.log(`Stop event: ${stopIndex} — ${STOPS[stopIndex]}`);

  gameState.currentStopIndex = stopIndex;

  // Record stop event
  await db.query(
    'INSERT INTO stop_events (session_id, stop_index, stop_name) VALUES ($1, $2, $3)',
    [gameState.sessionId, stopIndex, STOPS[stopIndex]]
  );

  // Find applicable elimination rule
  const rule = gameState.elimRules.find(r => r.stop_index === stopIndex);

  if (rule) {
    if (rule.is_final) {
      await processFinalCut();
    } else {
      await processElimination(rule, stopIndex);
    }
  } else {
    // Just broadcast position update
    broadcastState();
  }

  // End game after last stop
  if (stopIndex >= STOPS.length - 1) {
    await endGame();
  }
}

async function processElimination(rule, stopIndex) {
  // Get all alive players
  const { rows: alivePlayers } = await db.query(
    `SELECT id, card_rank, card_suit FROM players
     WHERE session_id=$1 AND status='alive'`,
    [gameState.sessionId]
  );

  const eliminated = alivePlayers.filter(p =>
    cardKilled(p.card_rank, p.card_suit, rule)
  );

  if (eliminated.length > 0) {
    const ids = eliminated.map(p => p.id);
    await db.query(
      `UPDATE players SET status='eliminated', eliminated_at_stop=$1
       WHERE id = ANY($2)`,
      [stopIndex, ids]
    );
  }

  const { rows: counts } = await db.query(
    `SELECT COUNT(*) FILTER (WHERE status='alive') as alive,
            COUNT(*) as total
     FROM players WHERE session_id=$1`,
    [gameState.sessionId]
  );

  gameState.aliveCount = parseInt(counts[0].alive);
  gameState.lastEvent = {
    type: 'elimination',
    stopIndex,
    stopName: STOPS[stopIndex],
    callText: rule.call_text,
    eliminated: eliminated.length,
    survive: gameState.aliveCount
  };

  broadcastState();

  // Check if down to 1
  if (gameState.aliveCount === 1) {
    await declareWinner();
  }
}

async function processFinalCut() {
  // Eliminate everyone except one random survivor
  const { rows: alivePlayers } = await db.query(
    `SELECT id FROM players WHERE session_id=$1 AND status='alive'`,
    [gameState.sessionId]
  );

  if (alivePlayers.length <= 1) {
    await declareWinner();
    return;
  }

  const survivorIdx = Math.floor(Math.random() * alivePlayers.length);
  const eliminated = alivePlayers.filter((_, i) => i !== survivorIdx);
  const ids = eliminated.map(p => p.id);

  await db.query(
    `UPDATE players SET status='eliminated', eliminated_at_stop=$1
     WHERE id = ANY($2)`,
    [gameState.currentStopIndex, ids]
  );

  gameState.aliveCount = 1;
  gameState.lastEvent = {
    type: 'final_cut',
    stopIndex: gameState.currentStopIndex,
    stopName: STOPS[gameState.currentStopIndex],
    callText: 'FINAL CUT!',
    eliminated: eliminated.length,
    survive: 1
  };

  broadcastState();
  await declareWinner();
}

async function declareWinner() {
  const { rows } = await db.query(
    `SELECT id, name, card_rank, card_suit FROM players
     WHERE session_id=$1 AND status='alive' LIMIT 1`,
    [gameState.sessionId]
  );

  if (rows.length === 0) return;
  const winner = rows[0];

  // Card flip: win based on probability setting
  const won = Math.random() * 100 < gameState.winProbability;

  await db.query(
    `UPDATE players SET status='winner' WHERE id=$1`,
    [winner.id]
  );

  await db.query(
    `UPDATE sessions SET status='complete' WHERE id=$1`,
    [gameState.sessionId]
  );

  gameState.status = 'complete';
  gameState.lastEvent = {
    type: 'winner',
    name: winner.name,
    cardRank: winner.card_rank,
    cardSuit: winner.card_suit,
    won
  };

  broadcastState();
  stopPolling();
}

async function endGame() {
  if (gameState.status === 'complete') return;
  gameState.status = 'complete';
  await db.query(`UPDATE sessions SET status='complete' WHERE id=$1`, [gameState.sessionId]);
  broadcastState();
  stopPolling();
}

// ─── State broadcast ──────────────────────────────────────────────────────────

function broadcastState() {
  io.emit('game_state', publicState());
}

function publicState() {
  return {
    status: gameState.status,
    currentStopIndex: gameState.currentStopIndex,
    currentStopName: gameState.currentStopIndex >= 0 ? STOPS[gameState.currentStopIndex] : null,
    nextStopName: gameState.currentStopIndex >= 0 && gameState.currentStopIndex < STOPS.length - 1
      ? STOPS[gameState.currentStopIndex + 1] : null,
    playerCount: gameState.playerCount,
    aliveCount: gameState.aliveCount,
    lastEvent: gameState.lastEvent,
    stops: STOPS
  };
}

// ─── Polling ──────────────────────────────────────────────────────────────────

let pollInterval = null;

function startPolling() {
  if (pollInterval) return;
  pollInterval = setInterval(pollGTFS, 30000);
  pollGTFS(); // immediate first poll
  console.log('GTFS polling started');
}

function stopPolling() {
  if (pollInterval) {
    clearInterval(pollInterval);
    pollInterval = null;
    console.log('GTFS polling stopped');
  }
}

// ─── Player API ───────────────────────────────────────────────────────────────

// GET game state (called on every app open — rehydration)
app.get('/api/state', async (req, res) => {
  res.json(publicState());
});

// POST register player
app.post('/api/register', async (req, res) => {
  const { name, phone } = req.body;

  if (!name || !phone) return res.status(400).json({ error: 'Name and phone required' });
  if (name.length > 12) return res.status(400).json({ error: 'Name too long' });
  if (!gameState.sessionId) return res.status(400).json({ error: 'No active session' });
  if (gameState.status !== 'waiting' && gameState.status !== 'active') {
    return res.status(400).json({ error: 'Game not accepting players' });
  }

  try {
    // Check player limit
    const { rows: countRows } = await db.query(
      `SELECT COUNT(*) as cnt FROM players WHERE session_id=$1`,
      [gameState.sessionId]
    );
    if (parseInt(countRows[0].cnt) >= MAX_PLAYERS) {
      return res.status(400).json({ error: 'Game full — 52 players maximum' });
    }

    // Check duplicate phone
    const { rows: existing } = await db.query(
      `SELECT id, card_rank, card_suit, status FROM players
       WHERE session_id=$1 AND phone=$2`,
      [gameState.sessionId, phone]
    );
    if (existing.length > 0) {
      // Return existing player state
      return res.json({
        success: true,
        rejoining: true,
        player: existing[0]
      });
    }

    // Get used cards to avoid duplicates
    const { rows: usedCards } = await db.query(
      `SELECT card_rank as rank, card_suit as suit FROM players WHERE session_id=$1`,
      [gameState.sessionId]
    );

    const card = dealCard(usedCards);

    const { rows } = await db.query(
      `INSERT INTO players (session_id, name, phone, card_rank, card_suit)
       VALUES ($1, $2, $3, $4, $5) RETURNING id, card_rank, card_suit, status`,
      [gameState.sessionId, name.trim(), phone.trim(), card.rank, card.suit]
    );

    gameState.playerCount++;
    broadcastState();

    res.json({ success: true, player: rows[0] });
  } catch (err) {
    console.error('Register error:', err.message);
    res.status(500).json({ error: 'Registration failed' });
  }
});

// GET player status (rehydration by phone)
app.get('/api/player/:phone', async (req, res) => {
  if (!gameState.sessionId) return res.json({ found: false });

  const { rows } = await db.query(
    `SELECT id, name, card_rank, card_suit, status, eliminated_at_stop
     FROM players WHERE session_id=$1 AND phone=$2`,
    [gameState.sessionId, req.params.phone]
  );

  if (rows.length === 0) return res.json({ found: false });
  res.json({ found: true, player: rows[0] });
});

// ─── Admin API ────────────────────────────────────────────────────────────────

// Serve admin panel
app.get(`/${ADMIN_PATH}`, (req, res) => {
  res.sendFile(path.join(__dirname, 'admin', 'index.html'));
});

// GET admin state
app.get(`/${ADMIN_PATH}/api/state`, async (req, res) => {
  const { rows: sessions } = await db.query(
    `SELECT * FROM sessions ORDER BY created_at DESC LIMIT 5`
  );
  const { rows: players } = gameState.sessionId ? await db.query(
    `SELECT name, card_rank, card_suit, status FROM players
     WHERE session_id=$1 ORDER BY joined_at`,
    [gameState.sessionId]
  ) : { rows: [] };

  res.json({
    gameState,
    sessions,
    players,
    stops: STOPS
  });
});

// POST create new session
app.post(`/${ADMIN_PATH}/api/session`, async (req, res) => {
  const { winProbability = 50, elimRules } = req.body;

  try {
    const { rows } = await db.query(
      `INSERT INTO sessions (win_probability) VALUES ($1) RETURNING id`,
      [winProbability]
    );
    const sessionId = rows[0].id;

    // Insert elim rules
    if (elimRules && elimRules.length > 0) {
      for (let i = 0; i < elimRules.length; i++) {
        const r = elimRules[i];
        await db.query(
          `INSERT INTO elim_rules
           (session_id, stop_index, call_text, kills_suit, kills_ranks, is_final, sort_order)
           VALUES ($1,$2,$3,$4,$5,$6,$7)`,
          [sessionId, r.stopIndex, r.callText, r.killsSuit || null,
           r.killsRanks || null, r.isFinal || false, i]
        );
      }
    }

    const { rows: rules } = await db.query(
      `SELECT * FROM elim_rules WHERE session_id=$1 ORDER BY sort_order`,
      [sessionId]
    );

    // Load into memory
    gameState.sessionId = sessionId;
    gameState.status = 'waiting';
    gameState.currentStopIndex = -1;
    gameState.playerCount = 0;
    gameState.aliveCount = 0;
    gameState.winProbability = winProbability;
    gameState.elimRules = rules;
    gameState.lastEvent = null;
    lastProcessedStop = -1;

    broadcastState();
    res.json({ success: true, sessionId });
  } catch (err) {
    console.error('Session create error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST start game (begin GTFS polling)
app.post(`/${ADMIN_PATH}/api/start`, async (req, res) => {
  if (!gameState.sessionId) return res.status(400).json({ error: 'No session' });
  gameState.status = 'active';
  await db.query(`UPDATE sessions SET status='active' WHERE id=$1`, [gameState.sessionId]);
  broadcastState();
  startPolling();
  res.json({ success: true });
});

// POST stop game
app.post(`/${ADMIN_PATH}/api/stop`, async (req, res) => {
  stopPolling();
  if (gameState.sessionId) {
    gameState.status = 'complete';
    await db.query(`UPDATE sessions SET status='complete' WHERE id=$1`, [gameState.sessionId]);
    broadcastState();
  }
  res.json({ success: true });
});

// POST manually trigger a stop event (testing / fallback)
app.post(`/${ADMIN_PATH}/api/trigger-stop`, async (req, res) => {
  const { stopIndex } = req.body;
  if (stopIndex === undefined) return res.status(400).json({ error: 'stopIndex required' });
  await processStopEvent(parseInt(stopIndex));
  res.json({ success: true });
});

// POST update win probability mid-game
app.post(`/${ADMIN_PATH}/api/win-prob`, async (req, res) => {
  const { probability } = req.body;
  gameState.winProbability = parseInt(probability);
  await db.query(`UPDATE sessions SET win_probability=$1 WHERE id=$2`,
    [gameState.winProbability, gameState.sessionId]);
  res.json({ success: true });
});

// ─── Socket.io ────────────────────────────────────────────────────────────────

io.on('connection', (socket) => {
  // Send current state immediately on connect
  socket.emit('game_state', publicState());
});

// ─── Boot ─────────────────────────────────────────────────────────────────────

// Restore active session on restart
async function restoreSession() {
  try {
    const { rows } = await db.query(
      `SELECT s.*, array_agg(row_to_json(r) ORDER BY r.sort_order) as rules
       FROM sessions s
       LEFT JOIN elim_rules r ON r.session_id = s.id
       WHERE s.status IN ('waiting','active')
       ORDER BY s.created_at DESC LIMIT 1`,
    );
    if (rows.length > 0 && rows[0].id) {
      const s = rows[0];
      const { rows: counts } = await db.query(
        `SELECT COUNT(*) as total,
                COUNT(*) FILTER (WHERE status='alive') as alive
         FROM players WHERE session_id=$1`,
        [s.id]
      );
      gameState.sessionId = s.id;
      gameState.status = s.status;
      gameState.winProbability = s.win_probability;
      gameState.elimRules = (s.rules || []).filter(Boolean);
      gameState.playerCount = parseInt(counts[0].total);
      gameState.aliveCount = parseInt(counts[0].alive);
      console.log(`Restored session ${s.id} — status: ${s.status}`);
      if (s.status === 'active') startPolling();
    }
  } catch (err) {
    console.error('Session restore error:', err.message);
  }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, async () => {
  console.log(`Survivor Royale running on port ${PORT}`);
  await restoreSession();
});
