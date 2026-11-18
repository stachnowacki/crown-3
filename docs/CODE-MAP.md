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
| `map performance` | `CHK`/`SPR` become LRU caches with a pixel budget (evicted canvases released with width=0), chunk resolution capped (`e.pr`), `sprite`/`spriteBig`/`paintB` guarded, canvas `contextlost`/`contextrestored` recovery. |
| `missions redesign` (`<style id="q-css">` + `<script>`) | The live mission screen: tabs Daily (`S.dq` gift streak, 3 tasks, bonus chest) · Tasks (`MIS`, no chests) · Quests (`S.side`, 3 at a time, chest) · Goals (`V6G` milestones/objectives, rare chest) · Wonders (Great Works + Monuments). Live `openMis`/`renderMis`/`setTab`/`sideFill`; dock/menu badges (`qBadges`); `renderSheet` patches the DOM (`qMorph`) so scrolling survives refreshes. |
| `update 18.11` (`<style id="u1811-css">` + `<script>`) | Far-zoom overview renderer (`drawFar`, below 13 px per tile), `UPC` cost balance (`upBal`), island snapshots + read-only viewer (`visitIsland`), chat reactions + "new messages" line, `lateBoost` for late joiners, daily gift pop-up (`dailyPop`), chest pop-up (`chestGot`), opening many chests (`gxBulk`), and the online admin panel (`oadmOpen`/`oadm`: gifts, land, buildings, terrain; checked by the server's `ADMIN_TOKEN`). Its old overlay viewer (`#visit`) now only shows loading and errors; a loaded island goes to `visitEnter`. |
| `rebalance 19.11` (`<style id="rb-css">` + last `<script>`) | Upgrade gates (`upGate`: cities need land and mines, mines/strongholds need a city level; used by `act('up')`, the tile panel and `plan`), island visits in the real map view (`visitEnter`/`visitLeave`: the visited island is swapped into `S`, `N`, `zoom`, `WD`… only inside each `draw` call, so the real realm keeps ticking and syncing; read-only building card `#rbc`), online admin tabs **Black** (`oaLook`) and **Title** (`oaTitle`), custom-title badges (`.ctit`). |

The **first** `<script>` after the pre-boot check also holds the 18.11 map code, because `load()`/`newGame()` run during boot: the compact tile codec (`encT`/`decT`, `packS`/`unpackS`), map growth (`genLand`, `stampOld`, `growHome`, `growCol`, `growSave`) and the per-tick scan cache (`tcache`).

### Most-wrapped functions

The **last** definition is the live entry point. Check all of them before you edit:

`Grep -n "function NAME(\|NAME=function" www/index.html`

| Function | Definitions |
|---|---|
| `render` | 5 |
| `draw` | 6 |
| `xtraHtml` | 6 |
| `hud` | 7 |
| `vMenu` | 7 |
| `newGame` | 7 |
| `resolve` | 5 |
| `act` | 4 |
| `upB` | 4 |

### Key facts

- **Map:** `S.tiles` is `N×N`: home island `HN0`=126, New Land `NL_CN`=404 (before 18.11: 56 and 128; `NL_OLD`). Old maps are grown on load with the old map kept in the middle (`HOFF`=35, `COFF`=138); a save is marked `gv:2` once grown. Tile fields: `o` owner (1 = player), `b` building (1 city, 2 mine, 3 stronghold, 4 port, 5–12 `BLD`), `l` level, `w` walls, `t` terrain.
- **Drawing:** `draw()` paints terrain chunks (`renderChunk`/`CHK`), then auras (`window.AUH` hook), then building sprites (`SPR`), then `drawFX`.
- **Saves:** schema `v:2`. Tiles are stored compactly as `tz` (`{n,t,x}`: 2 chars per tile + a sparse list of owned/built tiles); the old `tiles` array format still loads. The pre-boot check must keep accepting any format change.
- **Difficulty:** `DIFFS[i]` has `cost`/`inc`/`rinc` for the player, `aic` for rival build costs and `rw` for rewards. `rwK()` scales every coin/material reward (missions, quests, daily, goals, events, rank chest); chest loot itself is not scaled, chests are just rarer.
- **Market:** fair price `S.price` drifts with supply and demand only; trades move the pressure `S.mz` (quantity / `mkDepth`, fades over ~150 s). Shown price `mkMid` = fair × exp(−pressure) in 0.5×–2×; every fill is the area under that curve (`mkFill`/`mkArea`), so splitting or round-tripping trades never pays. Spread `spH()` is at least 2.5% per side.
- **Scans:** `owned`, `cities`, `ports`, `portLv`, `maxLv` are cached for ~95 ms (`tcache`). Wrap a new player action in `tcacheOff` if it reads them in a loop while changing tiles.

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
| `function handle(` | Main switch on `m.t`: `create join start leave spawn trade score gscore lb fx chat save tiles tally hit siege`. |

**Client and server must agree** on these:

- `START` spawn points for slots 1–5, offset by `OFFH` (slots 6–7 and late joiners send a `spawn` message instead; kept as `p.sp`)
- up to 7 players per island (`MAXP`); local owner ids 1–7 map to global slots with `L(s)`
- shared market: server `mkStep`/`mkArea` mirror the offline market; the server sends `z` (pressure) with prices and fills at depth `MKD × MKSRV` (client `mkDepth` uses the same when `MKT.live`)
- tile index layout (`126*126` home + `404*404` shared; rooms from before are migrated once, `r.mv=2`). Clients send `pv:2` on `auth`/`resume`; an older page is told to reload and is not let into an island
- online admin: `adm` (ops `who`, `give`, `tiles`, `terr`) → `admok`; gifts `agift` wait in `p.gifts` until the player confirms with `agok`; admin terrain is `r.terr` (sent in init and as `terr`)
- custom titles: admin op `ctitle` (`to` slot or `id` account, `ti` ≤ 24 chars, empty removes) stores `u.ct`; sent as `ct` in room players, chat messages, `lb` rows and `isle`
- extra messages: `react` (chat emoji), `gsnap`/`visit` → `isle` (offline island snapshots in `DATA_DIR/snaps`, cloud key `snap:<id>`)
- tile message `[i, owner, b, l, w, conquest]`, with `l ≤ 40`
