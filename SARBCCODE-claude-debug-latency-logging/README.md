# SARBCCODE

**S**imultaneous **A**symmetric a**RB**itrage engine — Kalshi × Polymarket.

## Architecture

```
PC (dev) → GitHub → VPS (NYC, execution only)
                      ├── src/
                      │   ├── config/       # env + asset settings
                      │   ├── connectors/   # Kalshi & Polymarket APIs
                      │   ├── engine/       # monitor + decision engine
                      │   ├── execution/    # parallel dispatcher
                      │   └── api/          # Express + Socket.io
                      └── frontend/         # Next.js dashboard (Cloudflare Tunnel)
```

**Backend** (Node.js on VPS) = sole executor.
**Frontend** (Next.js via Cloudflare Tunnel) = viewer/configurator only.

## Security

- `.env`, `.pem`, and wallet keys are **never** committed.
- Sensitive files are created **manually on the VPS only**.
- See `.env.example` for the required variables.

## Roadmap

| Etapa | Description | Status |
|-------|-------------|--------|
| 1 | Project structure & security | **current** |
| 2 | Global monitor & WebSockets | next |
| 3 | Decision engine & bankroll mgmt | — |
| 4 | Parallel execution & Cloudflare Tunnel | — |
| 5 | Dashboard & Socket.io UI | — |

## Quick Start (VPS)

```bash
git clone <repo-url> && cd sarbccode
cp .env.example .env   # fill in your keys
npm install
npm start
```
