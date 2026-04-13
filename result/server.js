/**
 * result/server.js - con External Configuration Store + Retry Pattern
 *
 * EXTERNAL CONFIGURATION STORE PATTERN:
 *   Toda la configuración se lee desde variables de entorno,
 *   las cuales son inyectadas por el ConfigMap de Kubernetes.
 *   Sin valores hardcodeados en el código.
 *
 * RETRY PATTERN:
 *   La función withRetry() reemplaza el uso de async.retry de la
 *   librería 'async'. Implementa backoff exponencial configurable
 *   para la conexión a PostgreSQL y las consultas a la base de datos.
 */

'use strict';

const express = require('express');
const pg = require('pg');
const path = require('path');
const cookieParser = require('cookie-parser');
const methodOverride = require('method-override');
const http = require('http');
const { Server } = require('socket.io');

// ── EXTERNAL CONFIGURATION STORE PATTERN ─────────────────────────
// Toda la config viene de variables de entorno (ConfigMap/Secret de K8s)
const CONFIG = {
  port:            process.env.PORT             || process.env.RESULT_PORT || 4000,
  databaseUrl:     process.env.DATABASE_URL     || 'postgres://okteto:okteto@postgresql/votes',
  maxRetries:      parseInt(process.env.WORKER_MAX_RETRIES    || '10', 10),
  retryDelayMs:    parseInt(process.env.WORKER_RETRY_DELAY_MS || '1000', 10),
  retryMaxDelayMs: 30000,
  retryFactor:     2,
  logLevel:        process.env.RESULT_LOG_LEVEL || 'info',
};
// ─────────────────────────────────────────────────────────────────

const app = express();
const server = http.createServer(app);
const io = new Server(server, { transports: ['polling'] });

// ── RETRY PATTERN ─────────────────────────────────────────────────
/**
 * withRetry - Implementación del patrón Retry con backoff exponencial.
 *
 * @param {string} operationName  Nombre de la operación (para logs)
 * @param {Function} fn           Función async a reintentar
 * @param {Object} opts           Opciones de configuración
 * @returns {Promise<*>}          Resultado de fn si tiene éxito
 * @throws                        Error si se agotan los reintentos
 */
async function withRetry(operationName, fn, opts = {}) {
  const maxAttempts  = opts.maxAttempts  || CONFIG.maxRetries;
  const baseDelay    = opts.baseDelay    || CONFIG.retryDelayMs;
  const maxDelay     = opts.maxDelay     || CONFIG.retryMaxDelayMs;
  const factor       = opts.factor       || CONFIG.retryFactor;

  let lastError;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const result = await fn();
      if (attempt > 1) {
        console.log(`[RETRY] '${operationName}' exitoso en el intento ${attempt}/${maxAttempts}`);
      }
      return result;
    } catch (err) {
      lastError = err;
      if (attempt === maxAttempts) break;

      // Backoff exponencial con cap
      const delay = Math.min(baseDelay * Math.pow(factor, attempt - 1), maxDelay);
      console.error(`[RETRY] '${operationName}' falló (intento ${attempt}/${maxAttempts}): ${err.message}. Reintentando en ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  throw new Error(`Operación '${operationName}' falló después de ${maxAttempts} intentos: ${lastError.message}`);
}
// ─────────────────────────────────────────────────────────────────

// Pool de conexión a PostgreSQL
// La URL viene del External Configuration Store (ENV → ConfigMap K8s)
const pool = new pg.Pool({
  connectionString: CONFIG.databaseUrl,
});

io.sockets.on('connection', function (socket) {
  socket.emit('message', { text: 'Welcome!' });
  socket.on('subscribe', function (data) {
    socket.join(data.channel);
  });
});

// Conectar a la DB con Retry Pattern
(async () => {
  let client;
  try {
    client = await withRetry('conectar a PostgreSQL', async () => {
      const c = await pool.connect();
      console.log('[DB] Conexión a PostgreSQL establecida');
      return c;
    });
    getVotes(client);
  } catch (err) {
    console.error('[FATAL] No se pudo conectar a la base de datos después de varios intentos:', err.message);
    process.exit(1);
  }
})();

function getVotes(client) {
  client.query(
    'SELECT vote, COUNT(id) AS count FROM votes GROUP BY vote',
    [],
    function (err, result) {
      if (err) {
        console.error('[DB ERROR] Error en query:', err.message);
      } else {
        const votes = collectVotesFromResult(result);
        io.sockets.emit('scores', JSON.stringify(votes));
      }
      setTimeout(() => getVotes(client), 1000);
    }
  );
}

function collectVotesFromResult(result) {
  const votes = { a: 0, b: 0 };
  result.rows.forEach(row => {
    votes[row.vote] = parseInt(row.count);
  });
  return votes;
}

app.use(cookieParser());
app.use(express.urlencoded({ extended: true }));
app.use(methodOverride('X-HTTP-Method-Override'));
app.use((_req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  res.header('Access-Control-Allow-Methods', 'PUT, GET, POST, DELETE, OPTIONS');
  next();
});

app.use(express.static(path.join(__dirname, 'views')));
app.get('/', (_req, res) => res.sendFile(path.resolve(__dirname, 'views/index.html')));

// Endpoint de health check (útil para el pipeline de CI/CD)
app.get('/health', (_req, res) => res.json({ status: 'ok', service: 'result' }));

server.listen(CONFIG.port, () => {
  console.log(`[START] Result service corriendo en el puerto ${CONFIG.port}`);
  console.log(`[CONFIG] DATABASE_URL: ${CONFIG.databaseUrl.replace(/:\/\/.*@/, '://***@')}`);
  console.log(`[CONFIG] Max retries: ${CONFIG.maxRetries}, Base delay: ${CONFIG.retryDelayMs}ms`);
});
