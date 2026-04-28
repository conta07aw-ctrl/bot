/**
 * PM2 ecosystem configuration.
 *
 * Usage:
 *   pm2 start ecosystem.config.js
 *   pm2 status
 *   pm2 logs sarbccode
 *   pm2 logs sarbccode-tunnel     — see tunnel URL here
 *   pm2 restart sarbccode
 *   pm2 stop all
 *   pm2 save                      — persist across reboots
 *   pm2 startup                   — auto-start PM2 on boot
 */

module.exports = {
  apps: [
    {
      name: 'sarbccode',
      script: 'src/index.js',
      cwd: '/SARB',
      // Fork mode (not cluster) — we run a single instance, so cluster mode
      // adds pure overhead: it routes stdout/stderr through an IPC pipe to
      // the pm2 god daemon (fd 3), serializes every log/metric, and can
      // stall the child when the master is slow to drain. Fork mode keeps
      // the child's stdout/stderr as direct file descriptors to the log
      // files declared below, eliminating that IPC path entirely.
      exec_mode: 'fork',
      // Disable pmx instrumentation. pmx monkey-patches console/http/https
      // and runs a sync metrics-collection tick every ~1s on the main thread.
      // For a latency-sensitive trading bot this is a liability — measured
      // Event Loop Latency p95 of 2258ms with pmx enabled. With pmx:false,
      // `pm2 describe` no longer shows the "Code metrics value" table, which
      // is expected and desired.
      pmx: false,
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '512M',
      env: {
        NODE_ENV: 'production',
        // Disable PM2's pmx instrumentation module.
        //
        // pmx monkey-patches console/http/https and collects event-loop/heap
        // metrics synchronously on the main thread every ~1s. On cluster mode
        // with instances:1, this is pure overhead with no paralelism benefit:
        // it redirects stdout through an IPC pipe (fd 3 -> pm2 god daemon),
        // adds per-call wrapping to hot-path HTTP/WS code, and adds a sync
        // metrics-collection tick that can stall the event loop.
        //
        // Disabling it keeps all PM2 process management intact (autorestart,
        // memory limit, log files) while removing the in-process instrumentation.
        // Side effect: `pm2 describe sarbccode` no longer shows "Code metrics
        // value" (heap/HTTP/event loop stats) — that's expected.
        PM2_NO_PMX: 'true',
      },
      exp_backoff_restart_delay: 1000,
      error_file: 'logs/error.log',
      out_file: 'logs/out.log',
      merge_logs: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    },
    {
      name: 'sarbccode-tunnel',
      script: 'cloudflared',
      args: 'tunnel --url http://localhost:3001',
      cwd: '/SARB',
      instances: 1,
      autorestart: true,
      watch: false,
      exp_backoff_restart_delay: 5000,
      error_file: 'logs/tunnel-error.log',
      out_file: 'logs/tunnel-out.log',
      merge_logs: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    },
  ],
};
