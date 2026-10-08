ORACLE — INTELLIGENT BUILD

Files
-----
server.js                         Backend, persistence, AI boundary, deterministic validator, bridge emitter
Oracle Final Corrected Locked.html  Canonical ORACLE source; not modified by the server
ORACLE_FINAL_INTELLIGENT_REPAIRED.html  Generated integrated artifact
.env.example                     Optional AI configuration template

Run
---
1. Install Node.js 18+ (Node 22.5+ is recommended for SQLite).
2. Optional: copy .env.example to .env and set ORACLE_AI_KEY + ORACLE_AI_MODEL.
3. Test validator:
   node server.js --test-mission
4. Generate integrated artifact:
   node server.js --emit-artifact
5. Start:
   node server.js
6. Open:
   http://127.0.0.1:8787

Architecture
------------
Canonical ORACLE state -> packet capture -> AI proposal -> deterministic validator -> verdict.
The AI does not own campaign state. The original ORACLE HTML remains the source of truth.

Important
---------
The generated artifact is intended to be served through server.js so its API calls reach the local backend.
Keep API keys only in .env; never put them in the HTML.
