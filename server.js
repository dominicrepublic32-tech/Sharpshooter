const express   = require('express');
const WebSocket = require('ws');
const crypto    = require('crypto');
const path      = require('path');
const fs        = require('fs');

const app = express();
app.use(express.json());

const PORT         = process.env.PORT         || 3000;
const APP_ID_LIVE  = process.env.DERIV_APP_ID || '33ENEhEj7R3Q7qcaE1HTy';
const REDIRECT_URI = process.env.REDIRECT_URI || 'https://mirca.onrender.com/callback';
const WS_DEMO      = 'wss://ws.binaryws.com/websockets/v3?app_id=1089';
const STATE_FILE   = path.join(__dirname, 'state.json');

const num = v => { const n = parseFloat(v); return isNaN(n) ? null : n; };
function serverLog(msg, data) { console.log(`[${new Date().toISOString()}] ${msg}`, data || ''); }

process.on('uncaughtException',  e => serverLog('uncaughtException',  { err: e && e.message }));
process.on('unhandledRejection', e => serverLog('unhandledRejection', { err: e && e.message ? e.message : String(e) }));

// ─── PKCE helpers ─────────────────────────────────────────────────────────────
function base64url(buf){ return buf.toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,''); }
function generateCodeVerifier(){ return base64url(crypto.randomBytes(32)); }
function generateCodeChallenge(v){ return base64url(crypto.createHash('sha256').update(v).digest()); }
const oauthPending = new Map();

// ─── SSE ─────────────────────────────────────────────────────────────────────
const clients = [];
function push(data){
  const s = `data: ${JSON.stringify(data)}\n\n`;
  clients.forEach(r => { try { r.write(s); } catch {} });
}

// ─── Deriv account ────────────────────────────────────────────────────────────
class Account {
  constructor(){
    this.token = ''; this.ws = null; this.ready = false;
    this.loginid = null; this.balance = null; this.currency = null;
    this.reqs = new Map(); this.watchers = new Map(); this.rid = 1;
    this.pinger = null; this._reconnecting = false;
    this.liveAccessToken = null; this.liveAccountId = null; this.isLive = false;
  }

  setToken(t){
    if (!t || t === this.token) return;
    this.token = t; this.ready = false; this.isLive = false;
    if (this.ws) try { this.ws.close(); } catch {}
  }

  _attachHandlers(ws){
    ws.on('message', raw => {
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (m.req_id && this.reqs.has(m.req_id)) { this.reqs.get(m.req_id)(m); this.reqs.delete(m.req_id); }
      if (m.msg_type === 'proposal_open_contract') {
        const c = m.proposal_open_contract;
        if (c && this.watchers.has(c.contract_id)) this.watchers.get(c.contract_id)(m);
      }
      if (m.msg_type === 'balance') { this.balance = m.balance?.balance; push({ type:'account', data:this.info() }); }
    });
    ws.on('error', e => serverLog('WS error', { err: e.message }));
    ws.on('close', () => {
      this.ready = false;
      if (this.pinger) clearInterval(this.pinger);
      push({ type:'account', data:this.info() });
      if (this.isLive && this.liveAccessToken && this.liveAccountId && !this._reconnecting) {
        this._reconnecting = true;
        serverLog('Live disconnected — refreshing OTP in 5s');
        setTimeout(() => this.refreshOTP(), 5000);
      } else if (!this.isLive && this.token && !this._reconnecting) {
        this._reconnecting = true;
        serverLog('Demo disconnected — reconnecting in 4s');
        setTimeout(() => { this.connect().catch(e => { this._reconnecting = false; serverLog('Reconnect failed', { err: e.message }); }); }, 4000);
      }
    });
  }

  connect(){
    return new Promise((resolve, reject) => {
      if (!this.token) return reject(new Error('No token — enter your demo API token'));
      if (this.ws) try { this.ws.close(); } catch {}
      const ws = new WebSocket(WS_DEMO);
      this.ws = ws; this.isLive = false;
      ws.on('open', () => {
        this.send({ authorize: this.token }).then(r => {
          if (r.error) return reject(new Error('Auth failed: ' + r.error.message));
          this.ready = true;
          this.loginid = r.authorize?.loginid;
          this.balance = r.authorize?.balance;
          this.currency = r.authorize?.currency;
          this._reconnecting = false;
          serverLog('Demo account connected', { loginid: this.loginid });
          this.pinger = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ ping: 1 })); }, 25000);
          this.send({ balance: 1, subscribe: 1 }).catch(() => {});
          push({ type:'account', data:this.info() });
          resolve();
        }).catch(e => { this._reconnecting = false; reject(e); });
      });
      this._attachHandlers(ws);
    });
  }

  connectLive(wssUrl, accountId){
    return new Promise((resolve, reject) => {
      if (this.ws) try { this.ws.close(); } catch {}
      const ws = new WebSocket(wssUrl);
      this.ws = ws; this.isLive = true; this.loginid = accountId;
      ws.on('open', () => {
        this.ready = true; this._reconnecting = false;
        serverLog('Live account connected', { loginid: accountId });
        this.pinger = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ ping: 1 })); }, 25000);
        this.send({ balance: 1, subscribe: 1 }).catch(() => {});
        push({ type:'account', data:this.info() });
        resolve();
      });
      this._attachHandlers(ws);
      ws.on('error', e => reject(e));
    });
  }

  async refreshOTP(){
    try {
      const otpRes = await fetch(`https://api.derivws.com/trading/v1/options/accounts/${this.liveAccountId}/otp`, {
        method:'POST', headers:{ 'Authorization':`Bearer ${this.liveAccessToken}`, 'Deriv-App-ID':APP_ID_LIVE },
      });
      const otpData = await otpRes.json();
      if (!otpData.data?.url) throw new Error('No URL in OTP response');
      await this.connectLive(otpData.data.url, this.liveAccountId);
      this._reconnecting = false;
    } catch (err) {
      this._reconnecting = false; this.liveAccessToken = null; this.liveAccountId = null;
      serverLog('OTP refresh failed', { err: err.message });
      push({ type:'live_status', status:'expired', msg:'⚠ Session expired — please login again' });
    }
  }

  send(obj){
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return reject(new Error('Account socket not open'));
      const req_id = this.rid++;
      this.reqs.set(req_id, resolve);
      this.ws.send(JSON.stringify({ ...obj, req_id }));
      setTimeout(() => { if (this.reqs.has(req_id)) { this.reqs.delete(req_id); reject(new Error('Request timed out')); } }, 15000);
    });
  }

  buy(params){ return this.send({ buy: 1, price: params.amount, parameters: params }); }

  watchContract(id){
    return new Promise((resolve, reject) => {
      let maxSpot = null, minSpot = null, entrySpot = null, barrier = null;
      const t = setTimeout(() => { this.watchers.delete(id); reject(new Error('Watch timed out')); }, 5 * 60 * 1000);
      this.watchers.set(id, msg => {
        const c = msg.proposal_open_contract; if (!c) return;
        const cs = parseFloat(c.current_spot);
        if (!isNaN(cs)) { if (maxSpot === null || cs > maxSpot) maxSpot = cs; if (minSpot === null || cs < minSpot) minSpot = cs; }
        if (c.entry_spot != null) entrySpot = parseFloat(c.entry_spot);
        if (c.barrier != null)    barrier   = parseFloat(c.barrier);
        if (c.is_sold || c.is_expired) {
          clearTimeout(t); this.watchers.delete(id);
          this.send({ forget: msg.subscription?.id }).catch(() => {});
          c._maxSpot = maxSpot; c._minSpot = minSpot; c._entrySpot = entrySpot; c._barrier = barrier;
          resolve(c);
        }
      });
      this.send({ proposal_open_contract: 1, contract_id: id, subscribe: 1 })
        .catch(e => { clearTimeout(t); this.watchers.delete(id); reject(e); });
    });
  }

  openCandleStream(symbol, cb, granularity = 60){
    const ws = new WebSocket(WS_DEMO);
    ws.on('open', () => ws.send(JSON.stringify({ ticks_history: symbol, adjust_start_time: 1, count: 50, end: 'latest', granularity, style: 'candles', subscribe: 1 })));
    ws.on('message', raw => {
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (m.msg_type === 'candles' && m.candles) m.candles.forEach(c => cb(c, false));
      if (m.msg_type === 'ohlc' && m.ohlc) cb(m.ohlc, true);
    });
    ws.on('error', () => {}); return ws;
  }

  // Closed-candle-only stream: fires cb ONLY when a candle has fully closed.
  // Detects closure by watching for epoch change in ohlc updates.
  // lastCandle holds the most recent forming candle; when epoch changes, lastCandle is now closed.
  openClosedCandleStream(symbol, cb, granularity = 60){
    const ws = new WebSocket(WS_DEMO);
    ws.on('open', () => ws.send(JSON.stringify({ ticks_history: symbol, adjust_start_time: 1, count: 50, end: 'latest', granularity, style: 'candles', subscribe: 1 })));
    ws.on('message', raw => {
      let m; try { m = JSON.parse(raw); } catch { return; }
      // One-time historical snapshot of closed candles
      if (m.msg_type === 'candles' && m.candles){
        m.candles.forEach(c => cb(c, false));
        return;
      }
      // Live ohlc stream — is_closed=1 means this candle just fully closed
      if (m.msg_type === 'ohlc' && m.ohlc && (m.ohlc.is_closed === 1 || m.ohlc.is_closed === true)){
        cb(m.ohlc, true);
      }
    });
    ws.on('error', () => {}); return ws;
  }

  openTickStream(symbol, cb){
    const ws = new WebSocket(WS_DEMO);
    ws.on('open', () => ws.send(JSON.stringify({ ticks: symbol, subscribe: 1 })));
    ws.on('message', raw => {
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (m.msg_type === 'tick' && m.tick) cb(parseFloat(m.tick.quote));
    });
    ws.on('error', () => {}); return ws;
  }

  info(){ return { ready:this.ready, loginid:this.loginid, balance:this.balance, currency:this.currency, isLive:this.isLive }; }
}

const acct = new Account();

// ─── contract builder ─────────────────────────────────────────────────────────
const CT_MAP = { CALL:'CALL', PUT:'PUT', HIGHER:'CALL', LOWER:'PUT', ONETOUCH:'ONETOUCH', NOTOUCH:'NOTOUCH', VANILLA_CALL:'VANILLALONGCALL', VANILLA_PUT:'VANILLALONGPUT' };
const NEEDS_BARRIER = new Set(['HIGHER','LOWER','ONETOUCH','NOTOUCH','VANILLA_CALL','VANILLA_PUT']);
function buildParams(symbol, type, barrier, durValue, durUnit, stake){
  const ct = CT_MAP[type] || type;
  const p = { amount:Number(stake), basis:'stake', contract_type:ct, currency:'USD', duration:Number(durValue), duration_unit:durUnit, symbol };
  if (NEEDS_BARRIER.has(type) && barrier != null && String(barrier).trim() !== '') p.barrier = String(barrier).trim();
  return p;
}

// ─── trade execution ──────────────────────────────────────────────────────────
async function runContract(slot, params, label, opts = {}){
  if (!acct.ready) throw new Error('Account not connected');
  slot.emit(`Opening ${label} | ${params.contract_type}${params.barrier ? ' @'+params.barrier : ''} | ${params.duration}${params.duration_unit} | $${params.amount}`);
  const res = await acct.buy(params);
  if (res.error) throw new Error(res.error.message);
  const cid = res.buy.contract_id;
  const settled = await acct.watchContract(cid);
  const profit = parseFloat(settled.profit || 0);
  const won = profit > 0;
  slot.stats.trades++;
  slot.stats[won ? 'wins' : 'losses']++;
  slot.stats.profit  = parseFloat((slot.stats.profit + profit).toFixed(2));
  slot.sessionProfit = parseFloat((slot.sessionProfit + profit).toFixed(2));
  slot.emit(`${won ? '✓ WIN' : '✗ LOSS'} ${label} $${Math.abs(profit).toFixed(2)} | session: ${slot.sessionProfit >= 0 ? '+' : ''}$${slot.sessionProfit}`, won ? 'win' : 'loss');
  const rec = {
    time:new Date().toISOString(), contract_id:cid, symbol:params.symbol, type:params.contract_type,
    barrier:params.barrier ?? null, stake:params.amount, profit, won, role:opts.role || 'primary',
    entrySpot:num(settled._entrySpot), exitSpot:num(settled.exit_spot ?? settled.sell_spot),
    barrierAbs:num(settled._barrier), maxSpot:num(settled._maxSpot), minSpot:num(settled._minSpot),
  };
  slot.history.unshift(rec);
  if (slot.history.length > 200) slot.history.pop();
  slot.pushState();
  push({ type:'trade', slotId:slot.id, data:rec });
  persistSoon();
  return rec;
}

// ─── TP / SL ─────────────────────────────────────────────────────────────────
function checkTPSL(slot){
  const { take_profit, stop_loss } = slot.cfg;
  const p = slot.sessionProfit;
  if (take_profit > 0 && p >= take_profit)            { slot.emit(`🎯 Take profit $${take_profit} reached — stopping`, 'win');  stopSlot(slot); return true; }
  if (stop_loss   > 0 && p <= -Math.abs(stop_loss))   { slot.emit(`🛑 Stop loss $${stop_loss} reached — stopping`, 'loss');     stopSlot(slot); return true; }
  return false;
}

// ─── filters ─────────────────────────────────────────────────────────────────
function passMomentum(slot){
  const { cfg, buf } = slot;
  const n = Math.min(cfg.momentum_candles, buf.length);
  if (n < 2) return true;
  if (cfg.observe_mode === 'ticks') {
    const r = buf.slice(-n).map(Number);
    const up = r.every((p,i)=>i===0||p>=r[i-1]), dn = r.every((p,i)=>i===0||p<=r[i-1]);
    if (!up && !dn) { slot.emit('Momentum: mixed direction — skip','err'); return false; }
    if (slot.zoneHigh && slot.zoneLow) {
      const pos = (r[r.length-1] - slot.zoneLow) / ((slot.zoneHigh - slot.zoneLow) || 1);
      if (pos > 0.35 && pos < 0.65) { slot.emit('Momentum: price dead-center — skip','err'); return false; }
    }
  } else {
    const r = buf.slice(-n);
    const bull = r.every(c=>parseFloat(c.close)>=parseFloat(c.open)), bear = r.every(c=>parseFloat(c.close)<=parseFloat(c.open));
    if (!bull && !bear) { slot.emit('Momentum: mixed candles — skip','err'); return false; }
    if (cfg.momentum_body_mult > 0 && buf.length >= 3) {
      const slice = buf.slice(-10), avgBody = slice.reduce((s,c)=>s+Math.abs(parseFloat(c.close)-parseFloat(c.open)),0)/slice.length;
      const last = r[r.length-1], body = Math.abs(parseFloat(last.close)-parseFloat(last.open));
      if (body < avgBody * cfg.momentum_body_mult) { slot.emit('Momentum: breakout candle too weak — skip','err'); return false; }
    }
  }
  return true;
}

function passTrend(slot){
  const { cfg, trendBuf } = slot;
  const n = Math.min(cfg.trend_candles, trendBuf.length);
  if (n < 3) return true;
  const candles = trendBuf.slice(-n);
  const avgBody = candles.reduce((s,c)=>s+Math.abs(parseFloat(c.close)-parseFloat(c.open)),0)/n;
  if (avgBody < cfg.min_body_size) { slot.emit('Trend: body too small — choppy — skip','err'); return false; }
  let ov = 0;
  for (let i=1;i<candles.length;i++) {
    const a=candles[i-1], b=candles[i];
    const oH=Math.min(parseFloat(a.high),parseFloat(b.high)), oL=Math.max(parseFloat(a.low),parseFloat(b.low));
    const overlap=Math.max(0,oH-oL), range=Math.max(parseFloat(a.high)-parseFloat(a.low),parseFloat(b.high)-parseFloat(b.low))||1;
    ov += overlap/range;
  }
  if (ov/(candles.length-1) > cfg.max_overlap) { slot.emit('Trend: overlap too high — ranging — skip','err'); return false; }
  let maxRun=1, run=1;
  for (let i=1;i<candles.length;i++) {
    const pb=parseFloat(candles[i-1].close)>=parseFloat(candles[i-1].open), cb=parseFloat(candles[i].close)>=parseFloat(candles[i].open);
    if (pb===cb){run++;maxRun=Math.max(maxRun,run);} else run=1;
  }
  if (maxRun < cfg.min_dir_candles) { slot.emit('Trend: no trend — skip','err'); return false; }
  return true;
}

function passFilter(slot){
  if (slot.cfg.filter === 'momentum') return passMomentum(slot);
  if (slot.cfg.filter === 'trend')    return passTrend(slot);
  return true;
}

// ─── slot defaults ────────────────────────────────────────────────────────────
function defaultCfg(){
  return {
    symbol:'1HZ100V', logic:'golden', observe_mode:'ticks', observe_count:5, reaction_range:0.3,
    confirm_count:1, stake:1, duration_value:5, duration_unit:'t',
    barrier1:'+0.25', barrier2:'-0.25', contract_type:'ONETOUCH',
    rest_seconds:30, skip_rest_on_win:false, take_profit:0, stop_loss:0,
    // Golden Logic — one attached contract per reversal side (independent params)
    combo_high_on:false, combo_high_type:'FALL', combo_high_barrier:'+2', combo_high_dv:5, combo_high_du:'t', combo_high_stake:1,
    combo_low_on:false,  combo_low_type:'RISE', combo_low_barrier:'-2', combo_low_dv:5, combo_low_du:'t', combo_low_stake:1,
    filter:'none', momentum_candles:3, momentum_body_mult:1.0,
    trend_candles:5, min_body_size:0.05, max_overlap:0.5, min_dir_candles:3,
    // Super Logic settings — all configurable, nothing hardcoded
    super_same_color_count:3, super_opposite_count:2,
    super_trend_enabled:true, super_trend_mode:'m1_5_15', super_trend_min_match:2,
    super_cooldown_seconds:30,
  };
}

// ─── slot ─────────────────────────────────────────────────────────────────────
class Slot {
  constructor(idx){
    this.idx=idx; this.id=`slot${idx}`; this.cfg=defaultCfg();
    this.running=false; this.busy=false;
    this.logs=[]; this.stats={trades:0,wins:0,losses:0,profit:0}; this.history=[];
    this.sessionProfit=0; this.liveCount=0;
    this.buf=[]; this.trendBuf=[]; this.dataWs=null; this.trendWs=null;
    this.phase='warmup'; this.zoneHigh=null; this.zoneLow=null;
    this.confSide=null; this.confBuf=[];
    // Super Logic state
    this.superM5Ws=null; this.superM15Ws=null;
    this.superM5Buf=[]; this.superM15Buf=[];
    this.superState='scanning';
    this.superRun=[];
    this.superRunColor=null;
    this.superTriggerBuf=[];
  }
  emit(msg, level='info'){
    const e={ time:new Date().toISOString(), msg, level };
    this.logs.push(e); if (this.logs.length>400) this.logs.shift();
    push({ type:'log', slotId:this.id, entry:e });
  }
  pushState(){
    push({ type:'state', slotId:this.id, data:{ running:this.running, busy:this.busy, stats:this.stats, phase:this.phase, liveCount:this.liveCount, sessionProfit:this.sessionProfit, superState:this.superState, superTriggerCount:this.superTriggerBuf.length, superOppositeCount:this.cfg.super_opposite_count } });
  }
  clearLog(){ this.logs=[]; push({ type:'log_cleared', slotId:this.id }); }
  clearStats(){ this.stats={trades:0,wins:0,losses:0,profit:0}; this.sessionProfit=0; this.history=[]; push({ type:'stats_cleared', slotId:this.id }); persistSoon(); }
  softReset(){
    this.phase='warmup'; this.zoneHigh=null; this.zoneLow=null; this.confSide=null; this.confBuf=[]; this.buf=[]; this.liveCount=0;
    this.superState='scanning'; this.superRun=[]; this.superRunColor=null; this.superTriggerBuf=[];
  }
  snap(){ return { id:this.id, idx:this.idx, running:this.running, busy:this.busy, stats:this.stats, cfg:this.cfg, phase:this.phase, liveCount:this.liveCount, sessionProfit:this.sessionProfit, superState:this.superState, superTriggerCount:this.superTriggerBuf.length, superOppositeCount:this.cfg.super_opposite_count }; }
}

const slots  = Array.from({length:10},(_,i)=>new Slot(i));
const slotOf = new Map(slots.map(s=>[s.id,s]));

function bufPrices(buf,mode){ return mode==='candles' ? buf.flatMap(c=>[parseFloat(c.high),parseFloat(c.low)]) : buf.map(Number); }
function bufSpread(buf,mode){ const ps=bufPrices(buf,mode); return parseFloat((Math.max(...ps)-Math.min(...ps)).toFixed(5)); }

// ─── Golden Logic — reversal touch (was Logic 3) ───────────────────────────────
function evalGolden_watch(slot){
  if (slot.busy || slot.buf.length < slot.cfg.observe_count) return;
  if (!passFilter(slot)) return;
  const sp = bufSpread(slot.buf, slot.cfg.observe_mode);
  if (sp > slot.cfg.reaction_range) return;
  const ps = bufPrices(slot.buf, slot.cfg.observe_mode);
  slot.zoneHigh = Math.max(...ps); slot.zoneLow = Math.min(...ps);
  slot.phase='confirming'; slot.confSide=null; slot.confBuf=[];
  slot.emit(`Zone locked — HIGH:${slot.zoneHigh} | LOW:${slot.zoneLow} | spread:${sp}`);
  slot.emit('Waiting for price to reach HIGH or LOW…');
  slot.pushState();
}

function onConfirmTick(slot, price){
  if (!slot.running || slot.busy || slot.phase !== 'confirming') return;
  if (!slot.confSide) {
    if (price >= slot.zoneHigh) { slot.confSide='high'; slot.confBuf=[price]; slot.emit(`HIGH touched (${slot.zoneHigh}) — waiting ${slot.cfg.confirm_count} back`); }
    else if (price <= slot.zoneLow) { slot.confSide='low'; slot.confBuf=[price]; slot.emit(`LOW touched (${slot.zoneLow}) — waiting ${slot.cfg.confirm_count} back`); }
    return;
  }
  slot.confBuf.push(price);
  if (slot.confSide==='high' && (slot.confBuf.filter(p=>p<slot.zoneHigh).length>=slot.cfg.confirm_count || slot.cfg.confirm_count===0)) fireReversal(slot,'high');
  if (slot.confSide==='low'  && (slot.confBuf.filter(p=>p>slot.zoneLow ).length>=slot.cfg.confirm_count || slot.cfg.confirm_count===0)) fireReversal(slot,'low');
}

function fireReversal(slot, side){
  slot.phase='trading'; slot.busy=true; slot.pushState();
  const c = slot.cfg;
  const barrier = side==='high' ? c.barrier1 : c.barrier2;

  // ── Golden Logic: PRIMARY one touch + ONE optional attached contract, fired together ──
  const on      = side==='high' ? c.combo_high_on      : c.combo_low_on;
  const aType   = side==='high' ? c.combo_high_type    : c.combo_low_type;
  const aBar    = side==='high' ? c.combo_high_barrier : c.combo_low_barrier;
  const aDV     = side==='high' ? c.combo_high_dv      : c.combo_low_dv;
  const aDU     = side==='high' ? c.combo_high_du      : c.combo_low_du;
  const aStake  = side==='high' ? c.combo_high_stake   : c.combo_low_stake;

  const primaryParams = buildParams(c.symbol,'ONETOUCH',barrier,c.duration_value,c.duration_unit,c.stake);
  slot.emit(`Confirmed ${side.toUpperCase()} reversal — PRIMARY ONETOUCH @${barrier}`);
  const primaryP = runContract(slot, primaryParams, `PRIMARY ONETOUCH ${side.toUpperCase()}`, { role:'primary' })
    .catch(e => { slot.emit('Primary error: '+e.message,'err'); return null; });

  let comboP = Promise.resolve(null);
  if (on && aType && aType !== 'none') {
    const ap = buildParams(c.symbol, aType, aBar, aDV, aDU, aStake);
    slot.emit(`Combo attach — ${aType}${ap.barrier ? ' @'+ap.barrier : ''} | ${ap.duration}${ap.duration_unit} | $${ap.amount}`);
    comboP = runContract(slot, ap, `COMBO ${aType}`, { role:'combo' })
      .catch(e => { slot.emit('Combo error: '+e.message,'err'); return null; });
  }
  // rest / momentum decision is based on the PRIMARY one touch ONLY
  Promise.all([primaryP, comboP]).then(([primaryRec]) => afterTrade(slot, primaryRec ? primaryRec.won : false));
}

function afterTrade(slot, won){
  if (!slot.running) return;
  if (checkTPSL(slot)) return;
  if (won && slot.cfg.skip_rest_on_win) {
    slot.busy=false; slot.phase='watching'; slot.zoneHigh=null; slot.zoneLow=null; slot.confSide=null; slot.confBuf=[];
    slot.pushState(); slot.emit('WIN — no rest, riding momentum with current data','win'); return;
  }
  const rest = Math.max(0, Number(slot.cfg.rest_seconds)) * 1000;
  if (rest > 0) slot.emit(`Resting ${slot.cfg.rest_seconds}s…`);
  setTimeout(() => {
    if (!slot.running) return;
    slot.busy=false; slot.softReset(); slot.pushState();
    slot.emit(`Collecting fresh data 0/${slot.cfg.observe_count}…`);
  }, rest);
}

// ─── Super Logic engine ────────────────────────────────────────────────────────

// Candle color: green = close > open, red = close < open (doji treated as same-color reset)
function candleColor(c){
  const cl = parseFloat(c.close), op = parseFloat(c.open);
  if (cl > op) return 'green';
  if (cl < op) return 'red';
  return 'doji';
}

// Trend detection on a candle buffer: Uptrend (HH+HL), Downtrend (LH+LL), Neutral
function detectTrend(buf){
  if (buf.length < 3) return 'neutral';
  const highs = buf.map(c => parseFloat(c.high));
  const lows  = buf.map(c => parseFloat(c.low));
  let hh = 0, hl = 0, lh = 0, ll = 0;
  for (let i = 1; i < highs.length; i++){
    if (highs[i] > highs[i-1]) hh++; else if (highs[i] < highs[i-1]) lh++;
    if (lows[i]  > lows[i-1])  hl++; else if (lows[i]  < lows[i-1])  ll++;
  }
  const pairs = highs.length - 1;
  const upScore  = (hh + hl) / (pairs * 2);
  const dnScore  = (lh + ll) / (pairs * 2);
  if (upScore >= 0.6 && hh > lh && hl > ll) return 'uptrend';
  if (dnScore >= 0.6 && lh > hh && ll > hl) return 'downtrend';
  return 'neutral';
}

// Trend confirmation gate — returns true if the trade direction is confirmed
function superTrendConfirmed(slot, direction){
  const cfg = slot.cfg;
  if (!cfg.super_trend_enabled) return true;

  const m1Trend  = detectTrend(slot.trendBuf);   // M1 candles (reuse existing trendBuf)
  const m5Trend  = detectTrend(slot.superM5Buf);
  const m15Trend = detectTrend(slot.superM15Buf);

  const required = direction === 'buy' ? 'uptrend' : 'downtrend';

  if (cfg.super_trend_mode === 'm1_5') {
    // Mode 2: M1 or M5 must match — M15 ignored
    const match = (m1Trend === required) || (m5Trend === required);
    slot.emit(`Super trend (M1-5 mode): M1=${m1Trend} M5=${m5Trend} | need=${required} | ${match ? 'PASS' : 'FAIL'}`);
    return match;
  } else {
    // Mode 1: M1-5-15, require minimum matching count
    const minMatch = Number(cfg.super_trend_min_match) || 2;
    const matches  = [m1Trend, m5Trend, m15Trend].filter(t => t === required).length;
    const pass     = matches >= minMatch;
    slot.emit(`Super trend (M1-5-15 mode): M1=${m1Trend} M5=${m5Trend} M15=${m15Trend} | need ${minMatch} x ${required} | got ${matches} | ${pass ? 'PASS' : 'FAIL'}`);
    return pass;
  }
}

// Full Super Logic reset — discard setup, start fresh
function superReset(slot){
  slot.superState     = 'scanning';
  slot.superRun       = [];
  slot.superRunColor  = null;
  slot.superTriggerBuf = [];
}

// Fire a Super Logic trade (primary ONETOUCH only, no combo)
function superFireTrade(slot, direction){
  if (slot.busy) return;
  slot.busy = true; slot.phase = 'trading'; slot.pushState();
  const c       = slot.cfg;
  const barrier = direction === 'buy' ? c.barrier1 : c.barrier2;
  const label   = direction === 'buy' ? 'BUY +Barrier' : 'SELL -Barrier';
  slot.emit(`Super Logic signal: ${label} ONETOUCH @${barrier}`);
  const params  = buildParams(c.symbol, 'ONETOUCH', barrier, c.duration_value, c.duration_unit, c.stake);
  runContract(slot, params, `SUPER ${label}`, { role:'primary' })
    .then(rec  => superAfterTrade(slot, rec.won))
    .catch(e   => { slot.emit('Super trade error: '+e.message,'err'); superAfterTrade(slot, false); });
}

// Post-trade cooldown then clean restart
function superAfterTrade(slot, won){
  if (!slot.running) return;
  if (checkTPSL(slot)) return;
  superReset(slot);
  const cooldown = Math.max(0, Number(slot.cfg.super_cooldown_seconds)) * 1000;
  slot.emit(`Super cooldown ${slot.cfg.super_cooldown_seconds}s — pausing all analysis…`);
  setTimeout(() => {
    if (!slot.running) return;
    slot.busy  = false;
    slot.phase = 'watching';
    superReset(slot);
    slot.pushState();
    slot.emit('Super cooldown done — scanning for fresh candle sequence…');
  }, cooldown);
}

// Called on every newly closed M1 candle for Super Logic
function superOnCandle(slot, candle){
  if (!slot.running || slot.busy) return;

  const color = candleColor(candle);
  const close  = parseFloat(candle.close);

  // ── SCANNING phase ──────────────────────────────────────────────────────────
  if (slot.superState === 'scanning'){
    if (color === 'doji'){
      // Doji breaks any run — treat as run-start of nothing, keep scanning
      superReset(slot); return;
    }

    if (slot.superRun.length === 0){
      // Start a fresh run with this candle
      slot.superRun      = [candle];
      slot.superRunColor = color;
      return;
    }

    if (color === slot.superRunColor){
      // Same color — check directional progression
      const prev = parseFloat(slot.superRun[slot.superRun.length - 1].close);
      const progressOk = (color === 'green') ? (close > prev) : (close < prev);
      if (progressOk){
        slot.superRun.push(candle);
        // Run grows — no state change yet, keep scanning for first opposite candle
      } else {
        // Direction broke on this same-color candle — it becomes run[0] of a new run
        slot.superRun      = [candle];
        slot.superRunColor = color;
        slot.emit(`Super: direction break on ${color} candle — resetting run, this candle is new start`);
      }
    } else {
      // Opposite color arrived — evaluate the run
      const n = Number(slot.cfg.super_same_color_count) || 3;
      if (slot.superRun.length < n){
        // Run too short — opposite candle becomes run[0] of a new run in the opposite color
        slot.superRun      = [candle];
        slot.superRunColor = color;
        return;
      }

      // Validate only the last N candles of the run
      const lastN = slot.superRun.slice(-n);
      let valid = true;
      for (let i = 1; i < lastN.length; i++){
        const pc = parseFloat(lastN[i-1].close), cc = parseFloat(lastN[i].close);
        if (slot.superRunColor === 'green' ? cc <= pc : cc >= pc){ valid = false; break; }
      }

      if (!valid){
        // Last N don't validate — opposite candle starts a new run
        slot.superRun      = [candle];
        slot.superRunColor = color;
        slot.emit('Super: last-N direction invalid — reset, opposite candle is new run start');
        return;
      }

      // Valid sequence confirmed — switch to triggering phase
      slot.superState      = 'triggering';
      slot.superTriggerBuf = [candle]; // first opposite candle counts as trigger #1
      const dir = slot.superRunColor === 'green' ? 'SELL' : 'BUY';
      slot.emit(`Super: valid ${slot.superRunColor.toUpperCase()} run (${slot.superRun.length} candles, using last ${n}) — waiting for ${slot.cfg.super_opposite_count} ${color.toUpperCase()} trigger candles (1/${slot.cfg.super_opposite_count})`);
      slot.pushState();
    }
    return;
  }

  // ── TRIGGERING phase ────────────────────────────────────────────────────────
  if (slot.superState === 'triggering'){
    const oppositeColor = slot.superRunColor === 'green' ? 'red' : 'green';

    if (color !== oppositeColor){
      // Original color or doji returned before trigger complete — entire setup invalid
      slot.emit(`Super: trigger broken by ${color} candle — discarding setup, starting fresh`);
      superReset(slot);
      if (color !== 'doji'){
        // Non-doji: this candle becomes run[0] of a new run
        slot.superRun      = [candle];
        slot.superRunColor = color;
      }
      // Doji: full reset, start completely fresh next candle
      return;
    }

    slot.superTriggerBuf.push(candle);
    const need = Number(slot.cfg.super_opposite_count) || 2;

    if (slot.superTriggerBuf.length < need){
      slot.emit(`Super: trigger ${slot.superTriggerBuf.length}/${need} ${oppositeColor.toUpperCase()} candles`);
      slot.pushState();
      return;
    }

    // Trigger complete — run trend confirmation then fire
    const direction = slot.superRunColor === 'green' ? 'sell' : 'buy';
    slot.emit(`Super: trigger complete (${need} ${oppositeColor.toUpperCase()} candles) — checking trend…`);

    if (!superTrendConfirmed(slot, direction)){
      slot.emit('Super: trend check FAILED — discarding setup, scanning fresh','err');
      superReset(slot);
      return;
    }

    // All conditions met — fire
    slot.emit(`Super: trend confirmed — firing ${direction.toUpperCase()}…`,'ok');
    superReset(slot);
    superFireTrade(slot, direction);
  }
}

// ─── data dispatch — guaranteed fresh warm-up before any trade ──────────────────
function dispatch(slot, val){
  if (!slot.running) return;

  // ── Super Logic: only uses M1 closed candles ─────────────────────────────
  if (slot.cfg.logic === 'super'){
    if (typeof val === 'object' && val.open != null){
      // val is a candle — only process closed ones (isLive=false from history, or ohlc with epoch matching)
      // The stream is set up so dispatch is only called with closed candles (isLive flag filtered in startWatcher)
      superOnCandle(slot, val);
    }
    return;
  }

  // ── Golden Logic ──────────────────────────────────────────────────────────
  const price = slot.cfg.observe_mode==='candles' ? parseFloat(val.close) : Number(val);
  if (slot.phase === 'confirming') { onConfirmTick(slot, price); return; }
  if (slot.phase === 'trading' || slot.busy) return;

  slot.liveCount++;
  slot.buf.push(val); if (slot.buf.length > slot.cfg.observe_count) slot.buf.shift();

  if (slot.liveCount < slot.cfg.observe_count) {
    slot.emit(`Collecting fresh ${slot.cfg.observe_mode==='candles'?'candles':'ticks'} ${slot.liveCount}/${slot.cfg.observe_count}…`);
    slot.pushState(); return;
  }
  if (slot.liveCount === slot.cfg.observe_count && slot.phase === 'warmup') {
    slot.phase = 'watching';
    slot.emit(`Warm-up complete — ${slot.cfg.observe_count} fresh ${slot.cfg.observe_mode==='candles'?'candles':'ticks'} collected — monitoring`);
    slot.pushState();
  }
  if (slot.phase === 'watching') evalGolden_watch(slot);
}

// ─── watchers ─────────────────────────────────────────────────────────────────
function closeWs(ws){ if (ws) try { ws.close(); } catch {} }

function startWatcher(slot){
  closeWs(slot.dataWs); slot.dataWs=null;
  closeWs(slot.trendWs); slot.trendWs=null;
  closeWs(slot.superM5Ws); slot.superM5Ws=null;
  closeWs(slot.superM15Ws); slot.superM15Ws=null;

  const cfg = slot.cfg;

  if (cfg.logic === 'super'){
    slot.dataWs = acct.openClosedCandleStream(cfg.symbol, (candle, isLive) => {
      if (!slot.running) return;
      slot.trendBuf.push(candle); if (slot.trendBuf.length > 100) slot.trendBuf.shift();
      if (!isLive) return; // historical — seed trend buffer only
      dispatch(slot, candle); // is_closed=1 — fully sealed candle
    }, 60);

    slot.superM5Ws = acct.openClosedCandleStream(cfg.symbol, (candle) => {
      if (!slot.running) return;
      slot.superM5Buf.push(candle); if (slot.superM5Buf.length > 100) slot.superM5Buf.shift();
    }, 300);

    if (cfg.super_trend_mode !== 'm1_5'){
      slot.superM15Ws = acct.openClosedCandleStream(cfg.symbol, (candle) => {
        if (!slot.running) return;
        slot.superM15Buf.push(candle); if (slot.superM15Buf.length > 100) slot.superM15Buf.shift();
      }, 900);
    }

    const attachClose = ws => {
      if (!ws) return;
      ws.on('error', e => slot.emit('Stream error: '+e.message,'err'));
      ws.on('close', () => {
        if (!slot.running) return;
        slot.emit('Stream dropped — reconnecting in 4s…','err');
        setTimeout(() => { if (slot.running) { superReset(slot); startWatcher(slot); } }, 4000);
      });
    };
    attachClose(slot.dataWs); attachClose(slot.superM5Ws); attachClose(slot.superM15Ws);
    slot.phase = 'watching'; slot.pushState();
    slot.emit(`Super Logic started — M1 candles (core) | M5 + M15 (trend) | Symbol: ${cfg.symbol}`);
    return;
  }

  // Golden Logic streams
  if (cfg.filter === 'trend') {
    slot.trendWs = acct.openCandleStream(cfg.symbol, candle => { if (!slot.running) return; slot.trendBuf.push(candle); if (slot.trendBuf.length>50) slot.trendBuf.shift(); });
  }
  const attachClose = ws => {
    ws.on('error', e => slot.emit('Stream error: '+e.message,'err'));
    ws.on('close', () => {
      if (!slot.running) return;
      slot.emit('Stream dropped — reconnecting in 4s…','err');
      setTimeout(() => { if (slot.running) { slot.buf=[]; slot.liveCount=0; slot.phase='warmup'; startWatcher(slot); } }, 4000);
    });
  };
  if (cfg.observe_mode === 'candles') {
    slot.dataWs = acct.openCandleStream(cfg.symbol, (candle, isLive) => { if (!isLive) return; dispatch(slot, candle); });
  } else {
    slot.dataWs = acct.openTickStream(cfg.symbol, price => dispatch(slot, price));
  }
  attachClose(slot.dataWs);
  slot.emit(`Stream open — ${cfg.observe_mode==='candles'?'M1 Candles':'Ticks'} | Golden Logic | Filter: ${cfg.filter}`);
}

async function startSlot(slot){
  if (slot.running) return { error:'Already running' };
  if (!acct.ready) { try { await acct.connect(); } catch(e) { return { error:e.message }; } }
  slot.running=true; slot.busy=false; slot.sessionProfit=0;
  slot.buf=[]; slot.trendBuf=[]; slot.liveCount=0;
  slot.phase='warmup'; slot.zoneHigh=null; slot.zoneLow=null; slot.confSide=null; slot.confBuf=[];
  slot.superM5Buf=[]; slot.superM15Buf=[];
  superReset(slot);
  slot.pushState(); startWatcher(slot);
  if (slot.cfg.logic !== 'super') slot.emit(`Bot started — collecting fresh data 0/${slot.cfg.observe_count} (no trade until warm-up done)…`);
  return { success:true };
}

function stopSlot(slot){
  slot.running=false; slot.busy=false;
  closeWs(slot.dataWs); slot.dataWs=null;
  closeWs(slot.trendWs); slot.trendWs=null;
  closeWs(slot.superM5Ws); slot.superM5Ws=null;
  closeWs(slot.superM15Ws); slot.superM15Ws=null;
  slot.buf=[]; slot.trendBuf=[]; slot.liveCount=0;
  slot.phase='warmup'; slot.zoneHigh=null; slot.zoneLow=null; slot.confSide=null; slot.confBuf=[];
  slot.superM5Buf=[]; slot.superM15Buf=[];
  superReset(slot);
  slot.emit('Bot stopped'); slot.pushState(); return { success:true };
}

// ─── health monitor — keep all running bots alive forever ───────────────────────
setInterval(() => {
  if (acct.token && !acct.ready && !acct._reconnecting && !acct.isLive) { acct.connect().catch(() => {}); }
  slots.forEach(slot => {
    if (!slot.running) return;
    const ws = slot.dataWs;
    if (!ws || ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING) {
      slot.emit('Health check: stream dead — restarting','err');
      if (slot.cfg.logic !== 'super'){ slot.buf=[]; slot.liveCount=0; slot.phase='warmup'; }
      else { superReset(slot); }
      startWatcher(slot);
    }
  });
}, 10000);

// ─── symbols ──────────────────────────────────────────────────────────────────
let symsCache = null;
function fetchSymbols(){
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(WS_DEMO);
    const t = setTimeout(() => { try { ws.close(); } catch {} reject(new Error('Timed out')); }, 10000);
    ws.on('open', () => ws.send(JSON.stringify({ active_symbols:'full', product_type:'basic' })));
    ws.on('message', raw => { let m; try { m=JSON.parse(raw); } catch { return; } if (m.msg_type==='active_symbols') { clearTimeout(t); try { ws.close(); } catch {} if (m.error) return reject(new Error(m.error.message)); resolve(m.active_symbols); } });
    ws.on('error', e => { clearTimeout(t); reject(e); });
  });
}

// ─── persistence (survives restarts) ─────────────────────────────────────────
let persistTimer = null;
function persistSoon(){ if (persistTimer) return; persistTimer = setTimeout(persistNow, 3000); }
function persistNow(){
  persistTimer = null;
  try {
    const data = slots.map(s => ({ id:s.id, cfg:s.cfg, stats:s.stats, history:s.history, sessionProfit:s.sessionProfit }));
    fs.writeFile(STATE_FILE, JSON.stringify(data), () => {});
  } catch {}
}
function loadState(){
  try {
    if (!fs.existsSync(STATE_FILE)) return;
    const data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    data.forEach(d => {
      const s = slotOf.get(d.id); if (!s) return;
      if (d.cfg)   s.cfg   = { ...s.cfg, ...d.cfg };
      if (d.stats) s.stats = d.stats;
      if (d.history) s.history = d.history;
      if (typeof d.sessionProfit === 'number') s.sessionProfit = d.sessionProfit;
    });
    serverLog('State restored from disk');
  } catch (e) { serverLog('State load failed', { err:e.message }); }
}
setInterval(persistNow, 30000);

// ─── OAuth routes ────────────────────────────────────────────────────────────
app.get('/api/oauth/url', (req, res) => {
  const verifier = generateCodeVerifier(), challenge = generateCodeChallenge(verifier), state = base64url(crypto.randomBytes(16));
  oauthPending.set(state, { verifier });
  setTimeout(() => oauthPending.delete(state), 10*60*1000);
  const params = new URLSearchParams({ response_type:'code', client_id:APP_ID_LIVE, redirect_uri:REDIRECT_URI, scope:'trade', state, code_challenge:challenge, code_challenge_method:'S256' });
  res.json({ url:`https://auth.deriv.com/oauth2/auth?${params.toString()}` });
});

app.get('/callback', async (req, res) => {
  const { code, state, error } = req.query;
  if (error) return res.send(`<script>window.location.href='/';</script><p>Login failed: ${error}</p>`);
  if (!code || !state) return res.send(`<script>window.location.href='/';</script><p>Missing params</p>`);
  const pending = oauthPending.get(state);
  if (!pending) return res.send(`<script>window.location.href='/';</script><p>Session expired — try again</p>`);
  oauthPending.delete(state);

  res.send(`<!DOCTYPE html><html><head><title>Zone Touch — Connecting</title>
  <style>body{background:#060910;color:#A78BFA;font-family:monospace;display:flex;align-items:center;justify-content:center;height:100vh;flex-direction:column;gap:16px;}
  .spin{width:40px;height:40px;border:3px solid #192030;border-top-color:#A78BFA;border-radius:50%;animation:s .8s linear infinite;}
  @keyframes s{to{transform:rotate(360deg);}}</style></head>
  <body><div class="spin"></div><p>Connecting to Deriv live account...</p>
  <script>setTimeout(()=>window.location.href='/',12000);</script></body></html>`);

  try {
    push({ type:'live_status', status:'working', msg:'Exchanging auth code…' });
    const tokenRes = await fetch('https://auth.deriv.com/oauth2/token', {
      method:'POST', headers:{ 'Content-Type':'application/x-www-form-urlencoded' },
      body:new URLSearchParams({ grant_type:'authorization_code', client_id:APP_ID_LIVE, code, code_verifier:pending.verifier, redirect_uri:REDIRECT_URI }).toString(),
    });
    const tokenData = await tokenRes.json();
    if (!tokenRes.ok || !tokenData.access_token) throw new Error(tokenData.error_description || tokenData.error || 'Token exchange failed');
    const accessToken = tokenData.access_token;
    push({ type:'live_status', status:'working', msg:'Access token obtained — fetching account…' });

    let accountsRes = await fetch('https://api.derivws.com/trading/v1/options/accounts', { headers:{ 'Authorization':`Bearer ${accessToken}`, 'Deriv-App-ID':APP_ID_LIVE } });
    let accountsData = await accountsRes.json();
    let accountId = accountsData.data?.[0]?.account_id;

    if (!accountId) {
      push({ type:'live_status', status:'working', msg:'No account found — creating one…' });
      const createRes = await fetch('https://api.derivws.com/trading/v1/options/accounts', { method:'POST', headers:{ 'Authorization':`Bearer ${accessToken}`, 'Deriv-App-ID':APP_ID_LIVE, 'Content-Type':'application/json' }, body:JSON.stringify({ currency:'USD', group:'row', account_type:'real' }) });
      const createData = await createRes.json();
      accountId = createData.data?.[0]?.account_id;
      if (!accountId) throw new Error('Could not find or create a live account');
    }
    push({ type:'live_status', status:'working', msg:`Account: ${accountId} — getting WebSocket token…` });

    const otpRes = await fetch(`https://api.derivws.com/trading/v1/options/accounts/${accountId}/otp`, { method:'POST', headers:{ 'Authorization':`Bearer ${accessToken}`, 'Deriv-App-ID':APP_ID_LIVE } });
    const otpData = await otpRes.json();
    if (!otpData.data?.url) throw new Error('OTP endpoint did not return a WebSocket URL');

    acct.liveAccessToken = accessToken; acct.liveAccountId = accountId;
    push({ type:'live_status', status:'working', msg:'Connecting live WebSocket…' });
    await acct.connectLive(otpData.data.url, accountId);
    push({ type:'live_status', status:'ready', msg:`✅ Connected — ${accountId}` });
    push({ type:'account', data:acct.info() });
  } catch (err) {
    serverLog('Live login error', { err:err.message });
    push({ type:'live_status', status:'error', msg:`❌ ${err.message}` });
  }
});

// ─── app routes ──────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.get('/api/stream', (req, res) => {
  res.set({ 'Content-Type':'text/event-stream', 'Cache-Control':'no-cache', Connection:'keep-alive' });
  res.flushHeaders(); clients.push(res);
  res.write(`data: ${JSON.stringify({ type:'init', account:acct.info(), slots:slots.map(s=>s.snap()), appId:APP_ID_LIVE })}\n\n`);
  req.on('close', () => { const i = clients.indexOf(res); if (i !== -1) clients.splice(i,1); });
});

app.get('/api/account', (req, res) => res.json(acct.info()));

app.post('/api/token', async (req, res) => {
  const { token } = req.body; if (!token) return res.status(400).json({ error:'No token' });
  acct.setToken(token);
  try { await acct.connect(); res.json({ success:true, ...acct.info() }); }
  catch (e) { res.status(500).json({ error:e.message }); }
});

app.get('/api/symbols', async (req, res) => {
  try {
    if (!symsCache) {
      const raw = await fetchSymbols();
      symsCache = raw.filter(s => s.market==='synthetic_index' && (/^1HZ/.test(s.symbol) || /\(1s\)/i.test(s.display_name)))
        .map(s => ({ symbol:s.symbol, display_name:s.display_name }))
        .sort((a,b) => a.display_name.localeCompare(b.display_name));
    }
    res.json(symsCache);
  } catch (e) { res.status(500).json({ error:e.message }); }
});

app.get('/api/slots', (req, res) => res.json(slots.map(s=>s.snap())));
app.get('/api/slot/:id', (req, res) => { const s=slotOf.get(req.params.id); s ? res.json(s.snap()) : res.status(404).json({ error:'Not found' }); });
app.get('/api/slot/:id/logs', (req, res) => { const s=slotOf.get(req.params.id); s ? res.json(s.logs) : res.status(404).json({ error:'Not found' }); });
app.get('/api/slot/:id/history', (req, res) => { const s=slotOf.get(req.params.id); s ? res.json(s.history) : res.status(404).json({ error:'Not found' }); });

app.post('/api/slot/:id/start', async (req, res) => { const s=slotOf.get(req.params.id); if (!s) return res.status(404).json({ error:'Not found' }); res.json(await startSlot(s)); });
app.post('/api/slot/:id/stop', (req, res) => { const s=slotOf.get(req.params.id); if (!s) return res.status(404).json({ error:'Not found' }); res.json(stopSlot(s)); });
app.post('/api/slot/:id/cfg', (req, res) => { const s=slotOf.get(req.params.id); if (!s) return res.status(404).json({ error:'Not found' }); s.cfg={ ...s.cfg, ...req.body }; persistNow(); res.json(s.cfg); });
app.post('/api/slot/:id/clearlogs', (req, res) => { const s=slotOf.get(req.params.id); if (!s) return res.status(404).json({ error:'Not found' }); s.clearLog(); res.json({ success:true }); });
app.post('/api/slot/:id/clearstats', (req, res) => { const s=slotOf.get(req.params.id); if (!s) return res.status(404).json({ error:'Not found' }); s.clearStats(); res.json({ success:true }); });
app.post('/api/stopall', (req, res) => { slots.forEach(s => { if (s.running) stopSlot(s); }); res.json({ success:true }); });

loadState();
app.listen(PORT, () => serverLog(`Zone Touch 10-in-1 v6 running on port ${PORT}`));
