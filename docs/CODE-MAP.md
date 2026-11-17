# Crownlands code map

Navigate with Grep, not full reads. `www/index.html` is ~635 KB, 4,600 lines, with lines up to ~1,700 chars. Line numbers drift, so search for the anchors below and Read a narrow `offset`/`limit` range.

## Files

| Path | What it is |
|---|---|
| `www/index.html` | The whole game (CSS + 15 `<script>` blocks). **Canonical.** |
| `www/sw.js` | Network-first service worker; cache name `crownlands-vN` (bump when shipping). |
| `server.js` | HTTP + WebSocket server, persistence, backups, admin API, world events. |
| `remote.js` | Optional Upstash Redis mirror (gzip+base64, one key per island). |
| `cloud-start.js` | `npm start`: restore from Upstash, then `require('./server.js')`. |
| `Dockerfile`, `render.yaml` | Render deploy (Docker, health check `/healthz`). |
| `.github/workflows/build-apk.yml`, `capacitor.config.json` | Android APK built in CI from `www/`. |

## `www/index.html`: script blocks in load order

Find each block with `Grep "<script" www/index.html`. Later blocks change earlier ones by **wrapping or redefining globals** (`const _x=fn;fn=function(){_x();…}`).

| Header comment (grep it) | Contents |
|---|---|
| `pre-boot save check` | Validates `localStorage['crownlands2']`, restores `-backup/-bk2/-bk3`. |
| `sea life` | Ships, dolphins, sea serpent. |
| `===== data =====` | Core game: constants (`BN`, `MAXL`, `UPC`, `FC`, `PC`, `BLD`, `SFX`, `SET`), state `S`, `act`, `render`, `draw`, `tick`, `save/load`, missions; then `v3` (army, buildings, research, events), `v4` (chunk cache `renderChunk`, `upB`, `xtraHtml`, ports), dev tree `PATHS`. |
| `building icons` | Tier medallions for buildings ≥5. |
| `multiplayer + smarter bots` | `MP`, `window.MPX`, `window.ONL`, WebSocket client, tile sync by `sig(t)`. |
| `admin tools` | Developer panel `A`/`adm` (password hash). |
| `Major Update` | Strategy, events, onboarding (`cl-tut*`), minimap/overlay. |
| `development tree screen` | Dev tree UI. |
| `NEW LANDS` | Shipyard, voyages, colony realm (`S.realm=='c'`), Metropolis (`findBlock`, `t.mx/t.pt`). |
| `<style id="gx-css">` + next `<script>` | Treasury IIFE: gems, chests (`TR`, `PL`, `LW`, `roll`, `openCh`, `oLoop`), cosmetics `CAT`, `treR` + `morph`, auras `window.AUH`, `window.__gx`. |
| `Kingdom Overview` | Overview screen. |
| `v6 launch layer` | Launch features. |
| `CORE7` | Seasons + world events, **verbatim copy** of `server.js` `/*@@CORE7@@*/…/*@@END CORE7@@*/`; edit both. Then `window.X7` (Event Centre). |
| `minor changes` | Upgrade max (`gxUpMax`, `plan`), demolish dialog (`demo`, `#gxdm`), map FX (`fxUp`, `fxDemo`). |
| `missions redesign` (`<style id="q-css">` + last `<script>`) | The live mission screen: tabs Daily (`S.dq` gift streak, 3 tasks, bonus chest) · Tasks (`MIS`, no chests) · Quests (`S.side`, 3 at a time, chest) · Goals (`V6G` milestones/objectives, rare chest) · Wonders (Great Works + Monuments). Live `openMis`/`renderMis`/`setTab`/`sideFill`; dock/menu badges (`qBadges`); `renderSheet` patches the DOM (`qMorph`) so scrolling survives refreshes. |

### Most-wrapped functions

The **last** definition is the live entry point. Check all of them before you edit:

`Grep -n "function NAME(\|NAME=function" www/index.html`

| Function | Definitions |
|---|---|
| `render` | 5 |
| `draw` | 5 |
| `xtraHtml` | 6 |
| `hud` | 7 |
| `vMenu` | 8 |
| `newGame` | 7 |
| `resolve` | 5 |
| `act` | 3 |
| `upB` | 4 |

### Key facts

- **Map:** `S.tiles` is `N×N` (N ∈ 56/96/128). Tile fields: `o` owner (1 = player), `b` building (1 city, 2 mine, 3 stronghold, 4 port, 5–12 `BLD`), `l` level, `w` walls, `t` terrain.
- **Drawing:** `draw()` paints terrain chunks (`renderChunk`/`CHK`), then auras (`window.AUH` hook), then building sprites (`SPR`), then `drawFX`.
- **Saves:** schema `v:2`. The pre-boot check must keep accepting any format change.

## `server.js`

Search for these anchors:

| Anchor | Purpose |
|---|---|
| `/*@@CORE7@@*/` | Shared seasons/world-event logic (see above). |
| `data safety` / `schema migration` | `okAcc`, `okRooms`, `atomicWrite`, `backup`, `recoverData`; `SCHEMA` + `migrateAccounts`/`migrateRooms`. |
| `rooms (persisted)` | `seed`, `loadRooms`, `persistNow`. |
| `---------- http` | `/healthz`, `/status`, static `www/` + `public/`. |
| `admin:` | `adminApi` (`/admin/*`, needs `ADMIN_TOKEN`). |
| `shared market` | `mkNew`, `mkStep`. |
| `function auth(` | Login/resume (scrypt, session keys). |
| `w7Handle` | World-event messages `we`, `wec`, `wecl`, `scl`. |
| `function handle(` | Main switch on `m.t`: `create join start leave trade score gscore lb fx chat save tiles tally hit siege`. |

**Client and server must agree** on these:

- `START` spawn points
- shared-market pacing (server `mkStep` mirrors the offline market)
- tile index layout (`56*56` home + `128*128` shared)
- tile message `[i, owner, b, l, w, conquest]`, with `l ≤ 40`
