const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const { Server } = require('socket.io');
const { Client, RemoteAuth } = require('whatsapp-web.js');
const QRCode = require('qrcode');
const { initializeApp, getApps, cert } = require('firebase-admin/app');
const { getDatabase } = require('firebase-admin/database');
const FirebaseStore = require('./FirebaseStore');

let firebaseDb = null;
function initFirebase() {
  if (firebaseDb) return firebaseDb;
  if (getApps().length > 0) {
    firebaseDb = getDatabase();
    return firebaseDb;
  }
  const raw = process.env.FIREBASE_SERVICE_ACCOUNT;
  if (!raw) {
    console.warn('FIREBASE_SERVICE_ACCOUNT not set — WhatsApp sessions will NOT persist across restarts!');
    return null;
  }
  try {
    const serviceAccount = JSON.parse(Buffer.from(raw, 'base64').toString());
    initializeApp({
      credential: cert(serviceAccount),
      databaseURL: `https://${serviceAccount.project_id}-default-rtdb.firebaseio.com`,
    });
    firebaseDb = getDatabase();
    console.log('Firebase RTDB initialized for session storage');
  } catch (err) {
    console.error('Firebase init failed:', err.message);
  }
  return firebaseDb;
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
});

app.use((_req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, x-api-key, x-user-id');
  if (_req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

const port = process.env.PORT || 3000;
const AUTH_BASE = process.env.WWEBJS_AUTH_PATH || path.join(__dirname, '.wwebjs_auth');
const ALLOWED_USERS = (process.env.ALLOWED_USERS || '').split(',').map(s => s.trim()).filter(Boolean);
const API_KEY = process.env.API_KEY || '';

const clients = new Map();   // userId -> { client, latestQr, isStarting, cachedChats }
const userSockets = new Map(); // userId -> Set<socketId>

function ensureUserDir(userId) {
  const dir = path.join(AUTH_BASE, userId);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function getUserState(userId) {
  if (!clients.has(userId)) {
    clients.set(userId, { client: null, latestQr: null, isStarting: false, cachedChats: null });
  }
  return clients.get(userId);
}

function getClient(userId) {
  return getUserState(userId).client;
}

function authMiddleware(req, res, next) {
  if (ALLOWED_USERS.length > 0) {
    const userId = req.headers['x-user-id'] || req.query.userId || req.params.userId;
    if (!userId || !ALLOWED_USERS.includes(userId)) {
      return res.status(403).json({ error: 'User not allowed' });
    }
  }
  if (API_KEY) {
    const key = req.headers['x-api-key'];
    if (key !== API_KEY) {
      return res.status(401).json({ error: 'Invalid API key' });
    }
  }
  next();
}

function emitToUser(userId, event, data) {
  const sockets = userSockets.get(userId);
  if (sockets) {
    for (const socketId of sockets) {
      const sock = io.sockets.sockets.get(socketId);
      if (sock) sock.emit(event, data);
    }
  }
}

function emitToAll(event, data) {
  for (const [userId] of clients) {
    emitToUser(userId, event, data);
  }
}

function startClient(userId) {
  const state = getUserState(userId);
  if (state.client || state.isStarting) return;
  state.isStarting = true;
  state.latestQr = null;
  console.log(`[${userId}] Starting WhatsApp client...`);

  const authDir = ensureUserDir(userId);
  const db = initFirebase();
  let authStrategy;
  if (db) {
    const store = new FirebaseStore({ db, userId, dataPath: authDir });
    authStrategy = new RemoteAuth({
      store,
      clientId: userId,
      dataPath: authDir,
      backupSyncIntervalMs: 60000,
    });
    console.log(`[${userId}] Using RemoteAuth with Firebase`);
  } else {
    const { LocalAuth } = require('whatsapp-web.js');
    authStrategy = new LocalAuth({ dataPath: authDir });
    console.log(`[${userId}] Using LocalAuth (no Firebase configured)`);
  }

  const client = new Client({
    authStrategy,
    puppeteer: {
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
    },
  });

  client.on('qr', async (qr) => {
    try {
      state.latestQr = await QRCode.toDataURL(qr);
    } catch {
      state.latestQr = qr;
    }
    emitToUser(userId, 'qr', state.latestQr);
  });

  client.on('authenticated', () => {
    console.log(`[${userId}] Authenticated`);
    emitToUser(userId, 'authenticated');
  });

  client.on('auth_failure', (msg) => {
    state.isStarting = false;
    console.error(`[${userId}] Auth failure:`, msg);
    emitToUser(userId, 'auth_failure', msg);
  });

  client.on('ready', async () => {
    state.isStarting = false;
    console.log(`[${userId}] Ready`);
    emitToUser(userId, 'ready', 'WhatsApp connected');
    await new Promise(r => setTimeout(r, 3000));
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const chats = await Promise.race([
          client.getChats(),
          new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 30000)),
        ]);
        state.cachedChats = chats;
        console.log(`[${userId}] Cached ${chats.length} chats`);
        emitToUser(userId, 'chats', chats.map(c => ({
          id: c.id._serialized,
          name: c.name || c.id._serialized,
          unreadCount: c.unreadCount,
          lastMessage: c.lastMessage ? {
            body: (c.lastMessage.body || '(media)').slice(0, 80),
            timestamp: c.lastMessage.timestamp,
          } : null,
        })));
        return;
      } catch (err) {
        console.error(`[${userId}] Error fetching chats (attempt ${attempt + 1}):`, err.message);
        if (attempt < 2) await new Promise(r => setTimeout(r, 5000));
      }
    }
    console.error(`[${userId}] Failed to fetch chats after all attempts`);
  });

  client.on('disconnected', (reason) => {
    state.isStarting = false;
    state.latestQr = null;
    state.cachedChats = null;
    console.log(`[${userId}] Disconnected:`, reason);
    emitToUser(userId, 'disconnected', reason);
  });

  client.on('message', async (msg) => {
    try {
      const chat = await msg.getChat();
      emitToUser(userId, 'new_message', {
        id: msg.id._serialized,
        from: msg.from,
        fromMe: msg.fromMe,
        body: msg.body || '(media)',
        timestamp: msg.timestamp,
        chat: { id: msg.from, name: chat.name || msg.from },
      });
    } catch (err) {
      console.error(`[${userId}] Error handling message:`, err.message);
    }
  });

  state.client = client;
  client.initialize();
}

io.on('connection', (socket) => {
  let currentUser = null;

  socket.on('register', (userId) => {
    if (!userId) return;
    if (ALLOWED_USERS.length > 0 && !ALLOWED_USERS.includes(userId)) {
      socket.emit('error', 'User not allowed');
      return;
    }
    currentUser = userId;
    if (!userSockets.has(userId)) userSockets.set(userId, new Set());
    userSockets.get(userId).add(socket.id);
    console.log(`[${userId}] Socket registered: ${socket.id}`);
  });

  socket.on('start', () => {
    if (!currentUser) return;
    startClient(currentUser);
  });

  socket.on('logout', async () => {
    if (!currentUser) return;
    const state = getUserState(currentUser);
    if (!state.client) return;
    try {
      await state.client.logout();
    } catch {}
    try { state.client.destroy(); } catch {}
    state.client = null;
    state.isStarting = false;
    state.latestQr = null;
    state.cachedChats = null;
    clients.delete(currentUser);
    emitToUser(currentUser, 'logged_out');
  });

  socket.on('get_chats', async () => {
    if (!currentUser) return;
    const state = getUserState(currentUser);
    if (!state.client) return;
    try {
      const chats = await state.client.getChats();
      state.cachedChats = chats;
      socket.emit('chats', chats.map(c => ({
        id: c.id._serialized,
        name: c.name || c.id._serialized,
        unreadCount: c.unreadCount,
        lastMessage: c.lastMessage ? {
          body: (c.lastMessage.body || '(media)').slice(0, 80),
          timestamp: c.lastMessage.timestamp,
        } : null,
      })));
    } catch (err) {
      console.error(`[${currentUser}] Error fetching chats:`, err.message);
    }
  });

  socket.on('get_messages', async (chatId) => {
    if (!currentUser) return;
    const state = getUserState(currentUser);
    if (!state.client) return;
    try {
      const chat = await state.client.getChatById(chatId);
      const msgs = await chat.fetchMessages({ limit: 50 });
      socket.emit('messages', msgs.map(m => ({
        id: m.id._serialized,
        from: m.from,
        fromMe: m.fromMe,
        body: m.body || '(media)',
        timestamp: m.timestamp,
      })));
    } catch (err) {
      console.error(`[${currentUser}] Error fetching messages:`, err.message);
    }
  });

  socket.on('send_message', async ({ chatId, message }) => {
    if (!currentUser) return;
    const state = getUserState(currentUser);
    if (!state.client) return;
    try {
      await state.client.sendMessage(chatId, message);
    } catch (err) {
      console.error(`[${currentUser}] Error sending message:`, err.message);
    }
  });

  socket.on('disconnect', () => {
    if (currentUser) {
      const sockets = userSockets.get(currentUser);
      if (sockets) {
        sockets.delete(socket.id);
        if (sockets.size === 0) userSockets.delete(currentUser);
      }
      console.log(`[${currentUser}] Socket disconnected: ${socket.id}`);
    }
  });
});

function resolveUserFromReq(req) {
  return req.headers['x-user-id'] || req.query.userId || process.env.DEFAULT_USER_ID || 'default';
}

// ── Backward-compatible routes (no userId in URL, uses x-user-id header or DEFAULT_USER_ID) ──

app.get('/api/status', (req, res) => {
  const userId = resolveUserFromReq(req);
  const client = getClient(userId);
  res.json({ connected: Boolean(client?.info?.wid?._serialized) });
});

app.post('/api/init', async (req, res) => {
  const userId = resolveUserFromReq(req);
  const state = getUserState(userId);
  if (state.client?.info?.wid?._serialized) {
    return res.json({ connected: true });
  }
  startClient(userId);
  res.json({ method: 'qr', qr: state.latestQr, message: 'Starting WhatsApp...' });
});

app.get('/api/qr', (req, res) => {
  const userId = resolveUserFromReq(req);
  const state = getUserState(userId);
  res.json({ qr: state.latestQr });
});

app.post('/api/logout', async (req, res) => {
  const userId = resolveUserFromReq(req);
  const state = getUserState(userId);
  if (!state.client) return res.json({ success: true });
  try { await state.client.logout(); } catch {}
  try { state.client.destroy(); } catch {}
  state.client = null;
  state.isStarting = false;
  state.latestQr = null;
  state.cachedChats = null;
  clients.delete(userId);
  res.json({ success: true });
});

async function fetchChats(client) {
  return Promise.race([
    client.getChats(),
    new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 30000)),
  ]);
}

app.get('/api/chats', async (req, res) => {
  const userId = resolveUserFromReq(req);
  const state = getUserState(userId);
  if (!state.client) return res.status(503).json({ error: 'Client not started' });
  try {
    const chats = state.cachedChats || await fetchChats(state.client);
    state.cachedChats = chats;
    res.json(chats.map(c => ({
      id: c.id._serialized,
      user: c.id.user,
      name: c.name || c.id._serialized,
      unreadCount: c.unreadCount,
      lastMessage: c.lastMessage ? {
        body: (c.lastMessage.body || '(media)').slice(0, 80),
        timestamp: c.lastMessage.timestamp,
      } : null,
    })));
  } catch (err) {
    console.error(`[${userId}] /api/chats failed:`, err.message);
    res.json([]);
  }
});

app.get('/api/messages/:chatId', async (req, res) => {
  const userId = resolveUserFromReq(req);
  const state = getUserState(userId);
  if (!state.client) return res.status(503).json({ error: 'Client not started' });
  try {
    const chat = await state.client.getChatById(req.params.chatId);
    const msgs = await chat.fetchMessages({ limit: 50 });
    res.json(msgs.map(m => ({
      id: m.id._serialized,
      from: m.from,
      fromMe: m.fromMe,
      body: m.body || '(media)',
      timestamp: m.timestamp,
    })));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/send', async (req, res) => {
  const userId = resolveUserFromReq(req);
  const state = getUserState(userId);
  const client = state.client;
  if (!client) return res.status(503).json({ error: 'Client not started' });
  try {
    let { chatId, message, name } = req.body;
    if (!chatId || !message) return res.status(400).json({ error: 'chatId and message required' });
    chatId = chatId.replace(/^\+/, '').trim();
    if (chatId.includes('@')) {
      try {
        const resolved = await Promise.race([
          client.getNumberId(chatId),
          new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 8000)),
        ]);
        if (resolved) {
          await doSend(client, resolved._serialized || String(resolved), message);
          return res.json({ success: true });
        }
      } catch {}
      await doSend(client, chatId, message);
      return res.json({ success: true });
    }
    if (name && state.cachedChats) {
      const nameLower = name.toLowerCase().trim();
      const match = state.cachedChats.find(c => {
        if (!c.name) return false;
        return c.name.toLowerCase().trim().includes(nameLower) || nameLower.includes(c.name.toLowerCase().trim());
      });
      if (match) {
        await doSend(client, match.id._serialized, message);
        return res.json({ success: true });
      }
    }
    const digits = chatId.replace(/[^0-9]/g, '');
    const intl = digits.replace(/^0+/, '');
    const phoneFormats = [intl, '20' + intl, digits].filter((v, i, a) => a.indexOf(v) === i);
    for (const fmt of phoneFormats) {
      try {
        const resolved = await Promise.race([
          client.getNumberId(fmt),
          new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 8000)),
        ]);
        if (resolved) {
          await doSend(client, resolved._serialized || String(resolved), message);
          return res.json({ success: true });
        }
      } catch {}
    }
    res.status(400).json({ error: 'Could not find this contact in WhatsApp. Make sure the number is registered and you have a chat with them.' });
  } catch (err) {
    if ((err.message || '').includes('No LID')) {
      return res.status(400).json({ error: 'Cannot find this contact. Make sure the number is registered on WhatsApp.' });
    }
    res.status(500).json({ error: err.message });
  }
});

// ── Multi-user REST API (explicit userId in URL) ────────────────────────

app.get('/api/:userId/status', authMiddleware, (req, res) => {
  const { userId } = req.params;
  const client = getClient(userId);
  res.json({ connected: Boolean(client?.info?.wid?._serialized) });
});

app.post('/api/:userId/init', authMiddleware, async (req, res) => {
  const { userId } = req.params;
  const state = getUserState(userId);
  if (state.client?.info?.wid?._serialized) {
    return res.json({ connected: true });
  }
  startClient(userId);
  res.json({ method: 'qr', qr: state.latestQr, message: 'Starting WhatsApp...' });
});

app.get('/api/:userId/qr', authMiddleware, (req, res) => {
  const { userId } = req.params;
  const state = getUserState(userId);
  res.json({ qr: state.latestQr });
});

app.post('/api/:userId/logout', authMiddleware, async (req, res) => {
  const { userId } = req.params;
  const state = getUserState(userId);
  if (!state.client) return res.json({ success: true });
  try { await state.client.logout(); } catch {}
  try { state.client.destroy(); } catch {}
  state.client = null;
  state.isStarting = false;
  state.latestQr = null;
  state.cachedChats = null;
  clients.delete(userId);
  res.json({ success: true });
});

app.get('/api/:userId/chats', authMiddleware, async (req, res) => {
  const { userId } = req.params;
  const state = getUserState(userId);
  const client = state.client;
  if (!client) return res.status(503).json({ error: 'Client not started' });
  try {
    const chats = state.cachedChats || await fetchChats(client);
    state.cachedChats = chats;
    res.json(chats.map(c => ({
      id: c.id._serialized,
      user: c.id.user,
      name: c.name || c.id._serialized,
      unreadCount: c.unreadCount,
      lastMessage: c.lastMessage ? {
        body: (c.lastMessage.body || '(media)').slice(0, 80),
        timestamp: c.lastMessage.timestamp,
      } : null,
    })));
  } catch (err) {
    console.error(`[${userId}] /api/chats failed:`, err.message);
    res.json([]);
  }
});

app.get('/api/:userId/messages/:chatId', authMiddleware, async (req, res) => {
  const { userId, chatId } = req.params;
  const state = getUserState(userId);
  const client = state.client;
  if (!client) return res.status(503).json({ error: 'Client not started' });
  try {
    const chat = await client.getChatById(chatId);
    const msgs = await chat.fetchMessages({ limit: 50 });
    res.json(msgs.map(m => ({
      id: m.id._serialized,
      from: m.from,
      fromMe: m.fromMe,
      body: m.body || '(media)',
      timestamp: m.timestamp,
    })));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

async function doSend(client, jid, message) {
  return Promise.race([
    client.sendMessage(jid, message, { waitUntilMsgSent: true }),
    new Promise((_, rej) => setTimeout(() => rej(new Error('Send timed out after 20s')), 20000)),
  ]);
}

app.post('/api/:userId/send', authMiddleware, async (req, res) => {
  const { userId } = req.params;
  const state = getUserState(userId);
  const client = state.client;
  if (!client) return res.status(503).json({ error: 'Client not started' });

  try {
    let { chatId, message, name } = req.body;
    if (!chatId || !message) return res.status(400).json({ error: 'chatId and message required' });
    chatId = chatId.replace(/^\+/, '').trim();
    console.log(`[${userId}] /api/send:`, { chatId, name });

    if (chatId.includes('@')) {
      try {
        const resolved = await Promise.race([
          client.getNumberId(chatId),
          new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 8000)),
        ]);
        if (resolved) {
          const jid = resolved._serialized || String(resolved);
          await doSend(client, jid, message);
          return res.json({ success: true });
        }
      } catch {}
      await doSend(client, chatId, message);
      return res.json({ success: true });
    }

    if (name && state.cachedChats) {
      const nameLower = name.toLowerCase().trim();
      const match = state.cachedChats.find(c => {
        if (!c.name) return false;
        return c.name.toLowerCase().trim().includes(nameLower) || nameLower.includes(c.name.toLowerCase().trim());
      });
      if (match) {
        await doSend(client, match.id._serialized, message);
        return res.json({ success: true });
      }
    }

    const digits = chatId.replace(/[^0-9]/g, '');
    const intl = digits.replace(/^0+/, '');
    const phoneFormats = [intl, '20' + intl, digits].filter((v, i, a) => a.indexOf(v) === i);
    for (const fmt of phoneFormats) {
      try {
        const resolved = await Promise.race([
          client.getNumberId(fmt),
          new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 8000)),
        ]);
        if (resolved) {
          const jid = resolved._serialized || String(resolved);
          await doSend(client, jid, message);
          return res.json({ success: true });
        }
      } catch {}
    }

    res.status(400).json({ error: 'Could not find this contact in WhatsApp. Make sure the number is registered and you have a chat with them.' });
  } catch (err) {
    const msg = err.message || '';
    console.error(`[${userId}] /api/send error:`, err);
    if (msg.includes('No LID')) {
      return res.status(400).json({ error: 'Cannot find this contact. Make sure the number is registered on WhatsApp.' });
    }
    res.status(500).json({ error: msg });
  }
});

server.listen(port, '0.0.0.0', () => {
  console.log(`WhatsApp server running on http://0.0.0.0:${port}`);
});
