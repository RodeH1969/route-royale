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
const WIN_PROBABILITY = 10; // fixed at 10%
const MAX_PLAYERS = 52;

// 379 stop sequence — game ends at Normanby Hotel (index 19)
const STOPS = [
  "Royal Pde, Ashgrove",           // 0
  "Waterworks Rd / Monoplane St",  // 1
  "Glory St, West Ashgrove",       // 2
  "Waterworks Rd / Myagh Rd",      // 3
  "Waterworks Rd / Girraween Gr",  // 4
  "Waterworks Rd / Hibiscus Ave",  // 5
  "Waterworks Rd / Elimatta Dr",   // 6
  "Waterworks Rd, Ashgrove",       // 7
  "Waterworks Rd / St Finbarr's",  // 8
  "Waterworks Rd / Woodland St",   // 9
  "Waterworks Rd / Boon St",       // 10
  "Waterworks Rd / Mossvale St",   // 11
  "Waterworks Rd / Whitta St",     // 12
  "Waterworks Rd / Glenrosa Rd",   // 13
  "Waterworks Rd / Cairns St",     // 14
  "Musgrave Rd, Red Hill Shops",   // 15
  "Musgrave Rd / Hammond St",      // 16
  "Musgrave Rd / Upper Clifton Tce", // 17
  "Musgrave Rd, Normanby Fiveways", // 18 — FINAL CUT here
  "Musgrave Rd / Normanby Hotel"   // 19 — must have winner by here
];

const REGISTRATION_CLOSES_AT_STOP = 14; // must join before stop 14
const FIRST_ELIM_STOP = 4;              // earliest elimination stop
const FINAL_CUT_STOP = 18;             // last elimination — must be down to 1

// ─── Game State ───────────────────────────────────────────────────────────────
let gameState = {
  sessionId: null,
  status: 'waiting',
  currentStopIndex: -1,
  playerCount: 0,
  aliveCount: 0,
  elimPlan: [],      // dynamically calculated [{stopIndex, callText, killsSuit, killsRanks, isFinal}]
  lastEvent: null,
  tripId: null,
  tripDeparture: null
};

// ─── Dynamic Elimination Planner ──────────────────────────────────────────────
// Called when registration closes — looks at actual cards dealt and
// calculates the smartest elimination sequence to get from N to 1
// before FINAL_CUT_STOP

async function calculateElimPlan(sessionId) {
  const { rows: players } = await db.query(
    `SELECT card_rank, card_suit FROM players WHERE session_id=$1 AND status='alive'`,
    [sessionId]
  );

  if (players.length === 0) return [];
  if (players.length === 1) return [{ stopIndex: FINAL_CUT_STOP, callText: 'FINAL CUT!', isFinal: true }];

  const n = players.length;

  // Count players per suit and per rank
  const suitCount = { '♠': 0, '♥': 0, '♦': 0, '♣': 0 };
  const rankCount = {};
  players.forEach(p => {
    suitCount[p.card_suit] = (suitCount[p.card_suit] || 0) + 1;
    rankCount[p.card_rank] = (rankCount[p.card_rank] || 0) + 1;
  });

  // Available elim stops
  const availableStops = [];
  for (let i = FIRST_ELIM_STOP; i < FINAL_CUT_STOP; i++) availableStops.push(i);

  // Build a pool of possible elimination calls sorted by usefulness
  // Each call: { callText, killsSuit, killsRanks, eliminates (count) }
  const callPool = [];

  // Suit eliminations
  Object.entries(suitCount).forEach(([suit, count]) => {
    if (count > 0) callPool.push({
      callText: `${suit} ${suitName(suit)} Out!`,
      killsSuit: suit,
      killsRanks: null,
      eliminates: count
    });
  });

  // Rank group eliminations — group low cards, face cards, specific ranks
  const rankGroups = [
    { ranks: ['2','3','4'], label: '2s 3s 4s Out!' },
    { ranks: ['5','6'],     label: '5s 6s Out!' },
    { ranks: ['9','10'],    label: '9s 10s Out!' },
    { ranks: ['J','Q','K'], label: 'J Q K Out!' },
    { ranks: ['A'],         label: 'Aces Out!' },
    { ranks: ['7','8'],     label: '7s 8s Out!' },
  ];

  rankGroups.forEach(g => {
    const count = g.ranks.reduce((sum, r) => sum + (rankCount[r] || 0), 0);
    if (count > 0) callPool.push({
      callText: g.label,
      killsSuit: null,
      killsRanks: g.ranks.join(','),
      eliminates: count
    });
  });

  // Sort by elimination count descending — biggest culls first
  callPool.sort((a, b) => b.eliminates - a.eliminates);

  // Now greedily assign calls to stops
  // Goal: eliminate exactly n-1 players across availableStops
  // Strategy: space them out so game stays tense as long as possible
  // — don't wipe everyone in first 2 stops
  // — aim for roughly equal culls spread across stops

  const plan = [];
  let remaining = n;
  let callIdx = 0;
  const usedCalls = new Set();

  // Figure out how many stops we have to work with
  const stopsAvailable = availableStops.length;

  // Simulate: pick calls that bring us smoothly from n down to 1
  // Use a planning pass first
  const targetPerStop = (n - 1) / stopsAvailable;

  // Assign stops evenly — skip stops if elimination not needed
  // We want eliminations at roughly: 1/3, 1/2, 2/3 of the way through + final
  // But ONLY if those calls actually eliminate real players

  // Filter to calls that eliminate > 0 players from current pool
  let livePlayers = players.map(p => ({ rank: p.card_rank, suit: p.card_suit }));

  // Assign elimination calls to stops
  // Work through sorted callPool, assign to spaced stops
  const elimStops = distributeStops(availableStops, Math.min(callPool.length, availableStops.length));

  elimStops.forEach((stopIdx, i) => {
    if (callIdx >= callPool.length) return;
    if (remaining <= 1) return;

    // Find a call that still kills someone alive
    while (callIdx < callPool.length) {
      const call = callPool[callIdx];
      const actualKills = livePlayers.filter(p =>
        (call.killsSuit && p.suit === call.killsSuit) ||
        (call.killsRanks && call.killsRanks.split(',').includes(p.rank))
      ).length;

      callIdx++;
      if (actualKills === 0) continue; // skip useless calls

      // Don't eliminate everyone — leave at least 1
      const willKill = Math.min(actualKills, remaining - 1);
      if (willKill === 0) continue;

      plan.push({
        stopIndex: stopIdx,
        callText: call.callText,
        killsSuit: call.killsSuit || null,
        killsRanks: call.killsRanks || null,
        isFinal: false
      });

      // Remove killed players from live pool
      livePlayers = livePlayers.filter(p => {
        const killed = (call.killsSuit && p.suit === call.killsSuit) ||
          (call.killsRanks && call.killsRanks.split(',').includes(p.rank));
        return !killed;
      });

      remaining = livePlayers.length;
      break;
    }
  });

  // Final cut at stop 18 — eliminates everyone except 1
  plan.push({
    stopIndex: FINAL_CUT_STOP,
    callText: 'FINAL CUT!',
    killsSuit: null,
    killsRanks: null,
    isFinal: true
  });

  console.log(`Elim plan for ${n} players:`, plan.map(p => `Stop ${p.stopIndex}: ${p.callText} (${p.isFinal?'FINAL':''}`));
  return plan;
}

function suitName(suit) {
  return { '♠': 'Spades', '♥': 'Hearts', '♦': 'Diamonds', '♣': 'Clubs' }[suit] || suit;
}

// Distribute n stops evenly across available stop indices
function distributeStops(available, n) {
  if (n <= 0) return [];
  if (n >= available.length) return available;
  const result = [];
  const step = available.length / n;
  for (let i = 0; i < n; i++) {
    result.push(available[Math.floor(i * step)]);
  }
  return result;
}

// ─── GTFS-RT Polling ─────────────────────────────────────────────────────────
let lastProcessedStop = -1;
let pollInterval = null;

async function pollGTFS() {
  if (!gameState.sessionId || gameState.status !== 'active') return;
  try {
    const { FeedMessage } = require('gtfs-realtime-bindings');
    const res = await fetch(GTFS_RT_URL);
    if (!res.ok) return;
    const buffer = await res.arrayBuffer();
    const feed = FeedMessage.decode(new Uint8Array(buffer));

    for (const entity of feed.entity) {
      if (!entity.vehicle) continue;
      const tripId = entity.vehicle.trip && entity.vehicle.trip.tripId;
      const routeId = entity.vehicle.trip && entity.vehicle.trip.routeId;

      if (gameState.tripId) {
        if (tripId !== gameState.tripId) continue;
      } else {
        if (routeId !== ROUTE_ID) continue;
      }

      const stopSeq = entity.vehicle.currentStopSequence;
      const stopIndex = stopSeq - 2;

      if (stopIndex > lastProcessedStop && stopIndex < STOPS.length) {
        lastProcessedStop = stopIndex;
        await processStopEvent(stopIndex);
      }
    }
  } catch (err) {
    console.error('GTFS poll error:', err.message);
  }
}

function startPolling() {
  if (pollInterval) return;
  pollInterval = setInterval(pollGTFS, 30000);
  pollGTFS();
  console.log('GTFS polling started');
}

function stopPolling() {
  if (pollInterval) { clearInterval(pollInterval); pollInterval = null; }
}

// ─── Stop Event Processing ────────────────────────────────────────────────────
async function processStopEvent(stopIndex) {
  console.log(`Stop event: ${stopIndex} — ${STOPS[stopIndex]}`);
  gameState.currentStopIndex = stopIndex;

  // Close registration at cutoff stop
  if (stopIndex === REGISTRATION_CLOSES_AT_STOP && gameState.elimPlan.length === 0) {
    console.log('Registration closing — calculating elimination plan...');
    gameState.elimPlan = await calculateElimPlan(gameState.sessionId);
    await db.query(`UPDATE sessions SET status='active' WHERE id=$1`, [gameState.sessionId]);
  }

  await db.query(
    'INSERT INTO stop_events (session_id, stop_index, stop_name) VALUES ($1,$2,$3)',
    [gameState.sessionId, stopIndex, STOPS[stopIndex]]
  );

  const rule = gameState.elimPlan.find(r => r.stopIndex === stopIndex);
  if (rule) {
    if (rule.isFinal) await processFinalCut(stopIndex);
    else await processElimination(rule, stopIndex);
  } else {
    broadcastState();
  }
}

async function processElimination(rule, stopIndex) {
  const { rows: alive } = await db.query(
    `SELECT id, card_rank, card_suit FROM players WHERE session_id=$1 AND status='alive'`,
    [gameState.sessionId]
  );

  const eliminated = alive.filter(p => {
    if (rule.killsSuit && p.card_suit === rule.killsSuit) return true;
    if (rule.killsRanks && rule.killsRanks.split(',').includes(p.card_rank)) return true;
    return false;
  });

  // Safety — never eliminate everyone
  const safeElim = eliminated.length >= alive.length
    ? eliminated.slice(0, alive.length - 1)
    : eliminated;

  if (safeElim.length > 0) {
    await db.query(
      `UPDATE players SET status='eliminated', eliminated_at_stop=$1 WHERE id=ANY($2)`,
      [stopIndex, safeElim.map(p => p.id)]
    );
  }

  const { rows: counts } = await db.query(
    `SELECT COUNT(*) FILTER (WHERE status='alive') as alive FROM players WHERE session_id=$1`,
    [gameState.sessionId]
  );
  gameState.aliveCount = parseInt(counts[0].alive);

  gameState.lastEvent = {
    type: 'elimination',
    stopIndex,
    stopName: STOPS[stopIndex],
    callText: rule.callText,
    eliminated: safeElim.length,
    survive: gameState.aliveCount
  };

  broadcastState();
  if (gameState.aliveCount === 1) await declareWinner();
}

async function processFinalCut() {
  const { rows: alive } = await db.query(
    `SELECT id FROM players WHERE session_id=$1 AND status='alive'`,
    [gameState.sessionId]
  );

  if (alive.length <= 1) { await declareWinner(); return; }

  const survivorIdx = Math.floor(Math.random() * alive.length);
  const toEliminate = alive.filter((_, i) => i !== survivorIdx);

  await db.query(
    `UPDATE players SET status='eliminated', eliminated_at_stop=$1 WHERE id=ANY($2)`,
    [FINAL_CUT_STOP, toEliminate.map(p => p.id)]
  );

  gameState.aliveCount = 1;
  gameState.lastEvent = {
    type: 'final_cut',
    stopIndex: FINAL_CUT_STOP,
    stopName: STOPS[FINAL_CUT_STOP],
    callText: 'FINAL CUT!',
    eliminated: toEliminate.length,
    survive: 1
  };

  broadcastState();
  await declareWinner();
}

async function declareWinner() {
  const { rows } = await db.query(
    `SELECT id, name, card_rank, card_suit FROM players WHERE session_id=$1 AND status='alive' LIMIT 1`,
    [gameState.sessionId]
  );
  if (rows.length === 0) return;
  const winner = rows[0];
  const won = Math.random() * 100 < WIN_PROBABILITY;

  await db.query(`UPDATE players SET status='winner' WHERE id=$1`, [winner.id]);
  await db.query(`UPDATE sessions SET status='complete' WHERE id=$1`, [gameState.sessionId]);

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

// ─── State ────────────────────────────────────────────────────────────────────
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
    tripId: gameState.tripId,
    tripDeparture: gameState.tripDeparture,
    registrationOpen: gameState.currentStopIndex < REGISTRATION_CLOSES_AT_STOP,
    stops: STOPS
  };
}

function broadcastState() { io.emit('game_state', publicState()); }

// ─── Player API ───────────────────────────────────────────────────────────────
app.get('/api/state', async (req, res) => res.json(publicState()));

app.post('/api/register', async (req, res) => {
  const { name, phone } = req.body;
  if (!name || !phone) return res.status(400).json({ error: 'Name and phone required' });
  if (name.length > 12) return res.status(400).json({ error: 'Name too long' });
  if (!gameState.sessionId) return res.status(400).json({ error: 'No active session' });
  if (gameState.status === 'complete') return res.status(400).json({ error: 'Game over' });
  if (gameState.currentStopIndex >= REGISTRATION_CLOSES_AT_STOP) {
    return res.status(400).json({ error: 'Registration closed — too close to the city!' });
  }

  try {
    const { rows: existing } = await db.query(
      `SELECT id, card_rank, card_suit, status FROM players WHERE session_id=$1 AND phone=$2`,
      [gameState.sessionId, phone]
    );
    if (existing.length > 0) return res.json({ success: true, rejoining: true, player: existing[0] });

    const { rows: countRows } = await db.query(
      `SELECT COUNT(*) as cnt FROM players WHERE session_id=$1`, [gameState.sessionId]
    );
    if (parseInt(countRows[0].cnt) >= MAX_PLAYERS) {
      return res.status(400).json({ error: 'Game full — 52 players maximum' });
    }

    const { rows: usedCards } = await db.query(
      `SELECT card_rank as rank, card_suit as suit FROM players WHERE session_id=$1`, [gameState.sessionId]
    );
    const card = dealCard(usedCards);

    const { rows } = await db.query(
      `INSERT INTO players (session_id, name, phone, card_rank, card_suit) VALUES ($1,$2,$3,$4,$5) RETURNING id, card_rank, card_suit, status`,
      [gameState.sessionId, name.trim(), phone.trim(), card.rank, card.suit]
    );

    gameState.playerCount++;
    gameState.aliveCount++;
    broadcastState();
    res.json({ success: true, player: rows[0] });
  } catch (err) {
    console.error('Register error:', err.message);
    res.status(500).json({ error: 'Registration failed' });
  }
});

app.get('/api/player/:phone', async (req, res) => {
  if (!gameState.sessionId) return res.json({ found: false });
  const { rows } = await db.query(
    `SELECT id, name, card_rank, card_suit, status, eliminated_at_stop FROM players WHERE session_id=$1 AND phone=$2`,
    [gameState.sessionId, req.params.phone]
  );
  if (rows.length === 0) return res.json({ found: false });
  res.json({ found: true, player: rows[0] });
});

function dealCard(usedCards) {
  const ranks = ['2','3','4','5','6','7','8','9','10','J','Q','K','A'];
  const suits = ['♠','♥','♦','♣'];
  const deck = [];
  for (const r of ranks) for (const s of suits) deck.push({ rank: r, suit: s });
  const available = deck.filter(c => !usedCards.some(u => u.rank === c.rank && u.suit === c.suit));
  if (available.length === 0) return { rank: 'A', suit: '♠' };
  return available[Math.floor(Math.random() * available.length)];
}

// ─── Admin API ────────────────────────────────────────────────────────────────
app.get(`/${ADMIN_PATH}`, (req, res) => res.sendFile(path.join(__dirname, 'admin', 'index.html')));

// Live 379 trips from GTFS-RT
app.get(`/${ADMIN_PATH}/api/trips`, async (req, res) => {
  try {
    const { FeedMessage } = require('gtfs-realtime-bindings');
    const response = await fetch(GTFS_RT_URL);
    if (!response.ok) return res.status(502).json({ error: 'GTFS feed unavailable' });
    const buffer = await response.arrayBuffer();
    const feed = FeedMessage.decode(new Uint8Array(buffer));
    const trips = [];
    for (const entity of feed.entity) {
      if (!entity.vehicle) continue;
      const routeId = entity.vehicle.trip && entity.vehicle.trip.routeId;
      if (routeId !== ROUTE_ID) continue;
      const tripId = entity.vehicle.trip.tripId;
      const stopSeq = entity.vehicle.currentStopSequence || 0;
      const pos = entity.vehicle.position;
      trips.push({ tripId, stopSequence: stopSeq, lat: pos ? pos.latitude : null, lng: pos ? pos.longitude : null });
    }
    res.json({ trips });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin state
app.get(`/${ADMIN_PATH}/api/state`, async (req, res) => {
  const { rows: players } = gameState.sessionId ? await db.query(
    `SELECT name, card_rank, card_suit, status FROM players WHERE session_id=$1 ORDER BY joined_at`,
    [gameState.sessionId]
  ) : { rows: [] };
  res.json({ gameState, players, stops: STOPS, winProbability: WIN_PROBABILITY });
});

// Create session
app.post(`/${ADMIN_PATH}/api/session`, async (req, res) => {
  const { tripId, tripDeparture } = req.body;
  try {
    const { rows } = await db.query(
      `INSERT INTO sessions (win_probability, trip_id, trip_departure) VALUES ($1,$2,$3) RETURNING id`,
      [WIN_PROBABILITY, tripId || null, tripDeparture || null]
    );
    const sessionId = rows[0].id;
    gameState.sessionId = sessionId;
    gameState.status = 'waiting';
    gameState.currentStopIndex = -1;
    gameState.playerCount = 0;
    gameState.aliveCount = 0;
    gameState.elimPlan = [];
    gameState.lastEvent = null;
    gameState.tripId = tripId || null;
    gameState.tripDeparture = tripDeparture || null;
    lastProcessedStop = -1;
    broadcastState();
    res.json({ success: true, sessionId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Start game
app.post(`/${ADMIN_PATH}/api/start`, async (req, res) => {
  if (!gameState.sessionId) return res.status(400).json({ error: 'No session' });
  gameState.status = 'active';
  await db.query(`UPDATE sessions SET status='active' WHERE id=$1`, [gameState.sessionId]);
  broadcastState();
  startPolling();
  res.json({ success: true });
});

// Stop game
app.post(`/${ADMIN_PATH}/api/stop`, async (req, res) => {
  stopPolling();
  if (gameState.sessionId) {
    gameState.status = 'waiting';
    await db.query(`UPDATE sessions SET status='waiting' WHERE id=$1`, [gameState.sessionId]);
    broadcastState();
  }
  res.json({ success: true });
});

// Manual stop trigger (testing / GTFS fallback)
app.post(`/${ADMIN_PATH}/api/trigger-stop`, async (req, res) => {
  const { stopIndex } = req.body;
  if (stopIndex === undefined) return res.status(400).json({ error: 'stopIndex required' });
  await processStopEvent(parseInt(stopIndex));
  res.json({ success: true });
});

// ─── Socket.io ────────────────────────────────────────────────────────────────
io.on('connection', (socket) => { socket.emit('game_state', publicState()); });

// ─── Restore session on restart ───────────────────────────────────────────────
async function restoreSession() {
  try {
    const { rows } = await db.query(
      `SELECT s.*, array_agg(row_to_json(r) ORDER BY r.sort_order) as rules
       FROM sessions s LEFT JOIN elim_rules r ON r.session_id=s.id
       WHERE s.status IN ('waiting','active') ORDER BY s.created_at DESC LIMIT 1`
    );
    if (rows.length > 0 && rows[0].id) {
      const s = rows[0];
      const { rows: counts } = await db.query(
        `SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE status='alive') as alive FROM players WHERE session_id=$1`,
        [s.id]
      );
      gameState.sessionId = s.id;
      gameState.status = s.status;
      gameState.tripId = s.trip_id || null;
      gameState.tripDeparture = s.trip_departure || null;
      gameState.playerCount = parseInt(counts[0].total);
      gameState.aliveCount = parseInt(counts[0].alive);
      gameState.elimPlan = [];
      console.log(`Restored session ${s.id}`);
      if (s.status === 'active') startPolling();
    }
  } catch (err) {
    console.error('Session restore error:', err.message);
  }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, async () => {
  console.log(`Route Royale running on port ${PORT}`);
  await restoreSession();
});