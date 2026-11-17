# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Crownlands is a browser/PWA strategy game. The whole client is `www/index.html`. `server.js` (with `remote.js` for Upstash) adds multiplayer, and Capacitor wraps `www/` as an Android APK. There is no build step, linter or test suite.

- **Run:** `npm install && npm start` (`cloud-start.js` → `server.js`, port 8080). Use `node server.js` to skip the Upstash restore. The APK is built only in CI (`.github/workflows/build-apk.yml`). Render deploys via `Dockerfile`/`render.yaml`.
- **Never Read `www/index.html` whole** (~635 KB, very long lines). Find code with Grep, using the anchors in `docs/CODE-MAP.md`, then Read narrow `offset`/`limit` ranges.
- **Later script blocks wrap or redefine globals** (`render`, `draw`, `hud`, `xtraHtml`, `act`, `upB`…). Grep every definition and edit the one that is actually live, or add a new wrapper.
- **Seasons and world-event logic (CORE7)** is duplicated: once in `server.js` (between `/*@@CORE7@@*/` markers) and once in `www/index.html`. Keep the two copies identical.
- **Save-format changes** must still pass the pre-boot save check (the first `<script>`). Server schema changes mean bumping `SCHEMA` and adding a migration.
- **Bump the cache name in `www/sw.js`** (`crownlands-vN`) when shipping client changes.
- Server environment variables are documented in the header comment of `server.js`.
