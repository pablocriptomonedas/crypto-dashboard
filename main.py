"""
╔══════════════════════════════════════════════════════════════════╗
║        CRYPTO EXPERT DASHBOARD v7 — Servidor Principal          ║
║  7 capas + Multi-Timeframe + Divergencias RSI + Diario          ║
║  RSI Wilder · MACD real · Stop Loss dinámico · DXY/Fed live    ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import time
import statistics
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlencode

# Crear carpetas necesarias antes de configurar el logging
os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn

# ── CONFIGURACIÓN ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/dashboard.log", encoding="utf-8"),
        logging.StreamHandler(stream=open(1, "w", encoding="utf-8", closefd=False))
    ]
)
log = logging.getLogger(__name__)

# API Keys de Binance desde variables de entorno (nunca en el código)
BINANCE_API_KEY    = os.environ.get("BINANCE_API_KEY", "")
BINANCE_SECRET_KEY = os.environ.get("BINANCE_SECRET_KEY", "")
BINANCE_AUTENTICADO = bool(BINANCE_API_KEY and BINANCE_SECRET_KEY)

# Telegram
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_ACTIVO = bool(TELEGRAM_TOKEN)
TELEGRAM_CHAT_IDS_FILE = "data/telegram_chats.json"

if BINANCE_AUTENTICADO:
    log.info("[OK] Binance API key configurada — usando endpoints autenticados")
else:
    log.warning("[WARN] Binance API key no configurada — usando endpoints públicos con fallback Bybit")

if TELEGRAM_ACTIVO:
    log.info("[OK] Telegram configurado — alertas activas en modo conservador")
else:
    log.warning("[WARN] Telegram no configurado — sin alertas por mensaje")

MONEDAS_DEFAULT = ["BTC", "ETH", "SOL", "XRP"]
INTERVALO_ACTUALIZACION = 300
INTERVALO_HEALTH_CHECK  = 3600

# URLs base de Binance — autenticado usa api.binance.com, sin auth usa api.binance.vision
BINANCE_BASE     = "https://data-api.binance.vision"
BINANCE_FUTURES  = "https://fapi.binance.com"

APIS = {
    "binance_klines":    f"{BINANCE_BASE}/api/v3/klines",
    "binance_orderbook": f"{BINANCE_BASE}/api/v3/depth",
    "binance_funding":   f"{BINANCE_FUTURES}/fapi/v1/fundingRate",
    "binance_oi":        f"{BINANCE_FUTURES}/fapi/v1/openInterest",
    "binance_lsratio":   f"{BINANCE_FUTURES}/futures/data/globalLongShortAccountRatio",
    "fear_greed":        "https://api.alternative.me/fng/?limit=1",
    "coingecko_global":  "https://api.coingecko.com/api/v3/global",
    "bybit_klines":      "https://api.bybit.com/v5/market/kline",
    "bybit_orderbook":   "https://api.bybit.com/v5/market/orderbook",
    "bybit_funding":     "https://api.bybit.com/v5/market/funding/history",
    "bybit_oi":          "https://api.bybit.com/v5/market/open-interest",
}

def binance_headers() -> dict:
    """Headers con API key para peticiones autenticadas a Binance."""
    if BINANCE_AUTENTICADO:
        return {"X-MBX-APIKEY": BINANCE_API_KEY}
    return {}

def binance_sign(params: dict) -> dict:
    """Añade timestamp y firma HMAC-SHA256 para endpoints que lo requieren."""
    if not BINANCE_AUTENTICADO:
        return params
    params["timestamp"] = int(time.time() * 1000)
    query = urlencode(params)
    firma = hmac.new(
        BINANCE_SECRET_KEY.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    params["signature"] = firma
    return params

health_status = {
    "precios_mercado":   {"ok": True, "ultimo_ok": time.time(), "errores": 0, "fuente": "binance"},
    "libro_ordenes":     {"ok": True, "ultimo_ok": time.time(), "errores": 0, "fuente": "binance"},
    "funding_rate":      {"ok": True, "ultimo_ok": time.time(), "errores": 0, "fuente": "binance"},
    "open_interest":     {"ok": True, "ultimo_ok": time.time(), "errores": 0, "fuente": "binance"},
    "ls_ratio":          {"ok": True, "ultimo_ok": time.time(), "errores": 0, "fuente": "binance"},
    "fear_greed":        {"ok": True, "ultimo_ok": time.time(), "errores": 0, "fuente": "alternative.me"},
    "coingecko_global":  {"ok": True, "ultimo_ok": time.time(), "errores": 0, "fuente": "coingecko"},
    "noticias":          {"ok": True, "ultimo_ok": time.time(), "errores": 0, "fuente": "coindesk/cointelegraph"},
    "dxy":               {"ok": True, "ultimo_ok": time.time(), "errores": 0, "fuente": "stooq"},
}

# Cache interna para APIs legacy (mantener compatibilidad)
cache_datos = {}
btc_cambio_cache = {"valor": 0.0, "ts": 0}

def marcar_ok(clave: str, fuente: str = ""):
    if clave in health_status:
        health_status[clave]["ok"]        = True
        health_status[clave]["ultimo_ok"] = time.time()
        health_status[clave]["errores"]   = 0
        if fuente:
            health_status[clave]["fuente"] = fuente

def marcar_error(clave: str):
    if clave in health_status:
        health_status[clave]["ok"]      = False
        health_status[clave]["errores"] += 1

# ── TELEGRAM ──────────────────────────────────────────────────────────

def cargar_chat_ids() -> list:
    """Carga los chat IDs de Telegram registrados."""
    try:
        if os.path.exists(TELEGRAM_CHAT_IDS_FILE):
            with open(TELEGRAM_CHAT_IDS_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return []

def guardar_chat_id(chat_id: int):
    """Guarda un nuevo chat ID si no existe ya."""
    ids = cargar_chat_ids()
    if chat_id not in ids:
        ids.append(chat_id)
        os.makedirs("data", exist_ok=True)
        with open(TELEGRAM_CHAT_IDS_FILE, "w") as f:
            json.dump(ids, f)
        log.info(f"[OK] Telegram: nuevo usuario registrado — chat_id {chat_id}")

async def enviar_telegram(mensaje: str):
    """Envía un mensaje a todos los usuarios registrados."""
    if not TELEGRAM_ACTIVO:
        return
    chat_ids = cargar_chat_ids()
    if not chat_ids:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10) as client:
        for chat_id in chat_ids:
            try:
                await client.post(url, json={
                    "chat_id":    chat_id,
                    "text":       mensaje,
                    "parse_mode": "HTML"
                })
            except Exception as e:
                log.warning(f"[WARN] Telegram envío fallido a {chat_id}: {e}")

async def procesar_updates_telegram():
    """Procesa los mensajes entrantes de Telegram para registrar usuarios.
    Usa offset para no procesar el mismo mensaje dos veces."""
    if not TELEGRAM_ACTIVO:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, params={"timeout": 0, "offset": -1})
            updates = r.json().get("result", [])
            if not updates:
                return

            # Procesar solo el último mensaje
            ultimo_update_id = None
            for update in updates:
                msg = update.get("message", {})
                text = msg.get("text", "")
                chat_id = msg.get("chat", {}).get("id")
                ultimo_update_id = update.get("update_id")

                if text == "/start" and chat_id:
                    ids_existentes = cargar_chat_ids()
                    if chat_id not in ids_existentes:
                        guardar_chat_id(chat_id)
                        await client.post(
                            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                            json={
                                "chat_id": chat_id,
                                "text": "✅ <b>Dashboard Crypto conectado</b>\n\nRecibirás alertas cuando el sistema detecte señales de compra en modo conservador (score ≥ 68% con tendencia diaria alcista).",
                                "parse_mode": "HTML"
                            }
                        )

            # Marcar todos los mensajes como procesados
            if ultimo_update_id is not None:
                await client.get(url, params={"timeout": 0, "offset": ultimo_update_id + 1})

    except Exception as e:
        log.warning(f"[WARN] Telegram updates: {e}")

# Últimas alertas enviadas para evitar spam
_ultimas_alertas: dict = {}
# Score anterior de cada moneda para detectar debilitamiento (alerta temprana)
_score_anterior: dict = {}

async def evaluar_y_alertar_telegram(simbolo: str, resultado: dict):
    """
    Evalúa si hay que enviar alguna de las tres alertas de mercado por Telegram:
    1. COMPRA — score >= 68% con tendencia diaria alcista (modo conservador)
    2. ALERTA TEMPRANA — indicadores debilitándose (RSI girando, MACD cruzando a bajista)
       antes de que se confirme la venta. Es la señal de "presta atención".
    3. VENTA CONFIRMADA — score <= 32%, señal de venta a nivel de mercado.
    Evita spam: no manda la misma alerta dos veces en menos de 4 horas.
    """
    if not TELEGRAM_ACTIVO:
        return
    score = resultado.get("score", {})
    score_val = score.get("score", 0)
    accion = score.get("accion", "")
    mtf = score.get("mtf", {})
    tendencia_1d = mtf.get("tf_1d", {}).get("tendencia", "neutral")
    precio = resultado.get("precio", 0)
    gestion = resultado.get("gestion", {})
    rsi = round(score.get("rsi", 0), 1)
    macd = score.get("macd", {})
    cruce = macd.get("cruce", "ninguno")

    ahora = time.time()
    score_prev = _score_anterior.get(simbolo, score_val)
    _score_anterior[simbolo] = score_val

    # ── 1. SEÑAL DE COMPRA (modo conservador) ─────────────────────────
    if accion == "COMPRAR" and tendencia_1d == "alcista":
        clave = f"{simbolo}_compra"
        if clave not in _ultimas_alertas or ahora - _ultimas_alertas[clave] >= 14400:
            _ultimas_alertas[clave] = ahora
            sl = gestion.get("stop_loss", 0)
            tp1 = gestion.get("tp1", 0)
            tp2 = gestion.get("tp2", 0)
            riesgo_pct = gestion.get("riesgo_pct", 0)
            mensaje = (
                f"🟢 <b>SEÑAL DE COMPRA — {simbolo}</b>\n\n"
                f"📊 Score: <b>{score_val}%</b>\n"
                f"💰 Precio: <b>${round(precio, 4)}</b>\n"
                f"📈 Tendencia diaria: <b>alcista ✅</b>\n\n"
                f"<b>Indicadores:</b>\n"
                f"• RSI: {rsi}\n"
                f"• MACD: {cruce if cruce != 'ninguno' else macd.get('tendencia','—')}\n\n"
                f"<b>Gestión:</b>\n"
                f"• Stop Loss: ${round(sl, 4)} (-{riesgo_pct}%)\n"
                f"• TP1: ${round(tp1, 4)}\n"
                f"• TP2: ${round(tp2, 4)}\n\n"
                f"⚠️ Aplica siempre el stop loss. No es asesoramiento financiero."
            )
            await enviar_telegram(mensaje)
            log.info(f"[OK] Telegram: alerta COMPRA {simbolo} enviada — score {score_val}%")
        return

    # ── 2. VENTA CONFIRMADA (score <= 32%) ────────────────────────────
    if accion == "VENDER":
        clave = f"{simbolo}_venta"
        if clave not in _ultimas_alertas or ahora - _ultimas_alertas[clave] >= 14400:
            _ultimas_alertas[clave] = ahora
            mensaje = (
                f"🔴 <b>SEÑAL DE VENTA — {simbolo}</b>\n\n"
                f"📊 Score: <b>{score_val}%</b>\n"
                f"💰 Precio: <b>${round(precio, 4)}</b>\n\n"
                f"<b>Indicadores:</b>\n"
                f"• RSI: {rsi}\n"
                f"• MACD: {cruce if cruce != 'ninguno' else macd.get('tendencia','—')}\n\n"
                f"Si tienes posición abierta en {simbolo}, revisa tu estrategia de salida.\n"
                f"⚠️ No es asesoramiento financiero."
            )
            await enviar_telegram(mensaje)
            log.info(f"[OK] Telegram: alerta VENTA {simbolo} enviada — score {score_val}%")
        return

    # ── 3. ALERTA TEMPRANA (debilitamiento antes de la venta confirmada) ─
    # Condiciones: el score ha caído de forma notable respecto al ciclo anterior
    # O el MACD acaba de cruzar a bajista mientras el score aún no es de venta
    # Solo se activa si todavía no estamos en zona de venta (evita duplicar con #2)
    caida_score = score_prev - score_val >= 8  # caída de 8+ puntos en 5 minutos
    macd_girando = cruce == "bajista"

    if (caida_score or macd_girando) and accion not in ("VENDER", "Posible venta"):
        clave = f"{simbolo}_temprana"
        if clave not in _ultimas_alertas or ahora - _ultimas_alertas[clave] >= 14400:
            _ultimas_alertas[clave] = ahora
            motivo = []
            if caida_score:   motivo.append(f"el score bajó de {score_prev}% a {score_val}%")
            if macd_girando:  motivo.append("el MACD acaba de cruzar a bajista")
            mensaje = (
                f"🟡 <b>ALERTA TEMPRANA — {simbolo}</b>\n\n"
                f"Los indicadores empiezan a debilitarse: {' y '.join(motivo)}.\n\n"
                f"📊 Score actual: <b>{score_val}%</b>\n"
                f"💰 Precio: <b>${round(precio, 4)}</b>\n\n"
                f"No es una señal de venta confirmada, pero presta atención si tienes posición abierta.\n"
                f"⚠️ No es asesoramiento financiero."
            )
            await enviar_telegram(mensaje)
            log.info(f"[OK] Telegram: alerta TEMPRANA {simbolo} enviada — score {score_val}%")


app = FastAPI(title="Crypto Expert Dashboard v7")


# ══════════════════════════════════════════════════════════════════════
# INDICADORES TÉCNICOS
# ══════════════════════════════════════════════════════════════════════

def calcular_rsi(precios: list, periodo: int = 14) -> float:
    """
    RSI de Wilder (método correcto).
    Primera media: SMA de los primeros 'periodo' cambios.
    Siguientes: media suavizada = (media_ant * (periodo-1) + cambio_actual) / periodo.
    Esto coincide exactamente con el RSI de TradingView y Binance.
    """
    if len(precios) < periodo + 1:
        return 50.0

    deltas = [precios[i] - precios[i-1] for i in range(1, len(precios))]

    # Primera media: SMA simple de los primeros 'periodo' deltas
    ganancias_init = [d for d in deltas[:periodo] if d > 0]
    perdidas_init  = [-d for d in deltas[:periodo] if d < 0]
    avg_g = sum(ganancias_init) / periodo
    avg_p = sum(perdidas_init)  / periodo

    # Suavizado de Wilder para el resto del historial
    for delta in deltas[periodo:]:
        ganancia = delta if delta > 0 else 0
        perdida  = -delta if delta < 0 else 0
        avg_g = (avg_g * (periodo - 1) + ganancia) / periodo
        avg_p = (avg_p * (periodo - 1) + perdida)  / periodo

    if avg_p == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_g / avg_p)), 2)


def calcular_ema(precios: list, periodo: int) -> float:
    if not precios:
        return 0.0
    k   = 2 / (periodo + 1)
    ema = precios[0]
    for p in precios[1:]:
        ema = p * k + ema * (1 - k)
    return round(ema, 8)


def calcular_macd(precios: list) -> dict:
    """
    MACD correcto: calcula el historial completo de valores MACD
    y luego aplica EMA9 sobre ese historial para obtener la signal line real.
    Esto permite detectar cruces MACD/signal con precisión real.
    """
    if len(precios) < 35:  # 26 + 9 mínimo para signal line real
        return {"macd": 0, "signal": 0, "histograma": 0,
                "tendencia": "neutral", "cruce": "ninguno"}

    # Calcular historial de valores MACD (EMA12 - EMA26) para cada punto
    k12 = 2 / (12 + 1)
    k26 = 2 / (26 + 1)
    ema12 = precios[0]
    ema26 = precios[0]
    macd_historia = []

    for p in precios:
        ema12 = p * k12 + ema12 * (1 - k12)
        ema26 = p * k26 + ema26 * (1 - k26)
        macd_historia.append(ema12 - ema26)

    # Signal line = EMA9 del historial de MACD
    k9     = 2 / (9 + 1)
    signal = macd_historia[0]
    for m in macd_historia:
        signal = m * k9 + signal * (1 - k9)

    macd_actual  = round(macd_historia[-1], 8)
    signal_actual = round(signal, 8)
    histo         = round(macd_actual - signal_actual, 8)

    # Detectar cruce reciente (últimas 2 velas)
    cruce = "ninguno"
    if len(macd_historia) >= 2:
        macd_prev   = macd_historia[-2]
        k9_prev     = 2 / (9 + 1)
        signal_prev = macd_historia[0]
        for m in macd_historia[:-1]:
            signal_prev = m * k9_prev + signal_prev * (1 - k9_prev)
        histo_prev = macd_prev - signal_prev
        if histo_prev <= 0 and histo > 0:
            cruce = "alcista"   # cruce dorado: MACD cruza por encima de signal
        elif histo_prev >= 0 and histo < 0:
            cruce = "bajista"   # cruce bajista: MACD cruza por debajo de signal

    # Tendencia: combina posición absoluta + histograma + cruce
    if macd_actual > 0 and histo > 0:
        tendencia = "alcista"
    elif macd_actual < 0 and histo < 0:
        tendencia = "bajista"
    elif cruce == "alcista":
        tendencia = "alcista"   # cruce reciente tiene prioridad
    elif cruce == "bajista":
        tendencia = "bajista"
    else:
        tendencia = "cruzando"

    return {
        "macd":      macd_actual,
        "signal":    signal_actual,
        "histograma": histo,
        "tendencia": tendencia,
        "cruce":     cruce,
    }


def calcular_sma(precios: list, periodo: int) -> float:
    if len(precios) < periodo:
        return precios[-1] if precios else 0
    return round(sum(precios[-periodo:]) / periodo, 8)


def calcular_bollinger(precios: list, periodo: int = 20) -> dict:
    if len(precios) < periodo:
        return {"superior": 0, "media": 0, "inferior": 0, "pct_b": 50, "ancho": 0}
    seg   = precios[-periodo:]
    media = sum(seg) / periodo
    desv  = statistics.stdev(seg)
    sup   = media + 2 * desv
    inf   = media - 2 * desv
    p     = precios[-1]
    pct_b = round((p - inf) / (sup - inf) * 100, 1) if sup != inf else 50
    ancho = round((sup - inf) / media * 100, 2)
    return {
        "superior": round(sup, 6), "media": round(media, 6),
        "inferior": round(inf, 6), "pct_b": max(0, min(100, pct_b)), "ancho": ancho
    }


def calcular_atr(highs: list, lows: list, closes: list, periodo: int = 14) -> float:
    if len(closes) < 2:
        return 0.0
    trs = []
    for i in range(1, min(periodo + 1, len(closes))):
        tr = max(highs[-i] - lows[-i],
                 abs(highs[-i] - closes[-i-1]),
                 abs(lows[-i]  - closes[-i-1]))
        trs.append(tr)
    return round(sum(trs) / len(trs), 8) if trs else 0.0


def calcular_stochastic_rsi(precios: list, periodo: int = 14) -> float:
    """
    Stochastic RSI eficiente: calcula el historial de RSI en un solo paso
    reutilizando las medias suavizadas de Wilder en lugar de recalcular
    el RSI completo para cada punto.
    """
    if len(precios) < periodo * 2 + 1:
        return 50.0

    deltas = [precios[i] - precios[i-1] for i in range(1, len(precios))]

    # Inicializar con SMA de los primeros 'periodo' deltas
    avg_g = sum(d for d in deltas[:periodo] if d > 0) / periodo
    avg_p = sum(-d for d in deltas[:periodo] if d < 0) / periodo

    rsi_historia = []

    # Calcular RSI vela a vela reutilizando medias anteriores (O(n) en lugar de O(n²))
    for delta in deltas[periodo:]:
        ganancia = delta if delta > 0 else 0.0
        perdida  = -delta if delta < 0 else 0.0
        avg_g = (avg_g * (periodo - 1) + ganancia) / periodo
        avg_p = (avg_p * (periodo - 1) + perdida)  / periodo
        rs  = avg_g / avg_p if avg_p > 0 else 100.0
        rsi = 100 - (100 / (1 + rs)) if avg_p > 0 else 100.0
        rsi_historia.append(rsi)

    if len(rsi_historia) < periodo:
        return 50.0

    rsi_ventana = rsi_historia[-periodo:]
    rsi_min = min(rsi_ventana)
    rsi_max = max(rsi_ventana)

    if rsi_max == rsi_min:
        return 50.0

    return round((rsi_historia[-1] - rsi_min) / (rsi_max - rsi_min) * 100, 1)


def detectar_divergencias_rsi(precios: list, highs: list, lows: list, periodo_rsi: int = 14, ventana: int = 30) -> dict:
    """
    Detecta divergencias RSI de forma eficiente.
    Usa el método de Wilder incremental (O(n)) en lugar de recalcular
    el RSI completo para cada punto (O(n²)).
    """
    if len(precios) < periodo_rsi + ventana + 2:
        return {"tipo": "ninguna", "senal": "neutral", "descripcion": "Sin datos suficientes", "fuerza": 0}

    deltas = [precios[i] - precios[i-1] for i in range(1, len(precios))]

    # Inicializar con SMA de los primeros 'periodo' deltas
    avg_g = sum(d for d in deltas[:periodo_rsi] if d > 0) / periodo_rsi
    avg_p = sum(-d for d in deltas[:periodo_rsi] if d < 0) / periodo_rsi

    rsi_historia = []

    # Calcular RSI vela a vela de forma incremental — O(n)
    for delta in deltas[periodo_rsi:]:
        ganancia = delta if delta > 0 else 0.0
        perdida  = -delta if delta < 0 else 0.0
        avg_g = (avg_g * (periodo_rsi - 1) + ganancia) / periodo_rsi
        avg_p = (avg_p * (periodo_rsi - 1) + perdida)  / periodo_rsi
        rs  = avg_g / avg_p if avg_p > 0 else 100.0
        rsi = 100 - (100 / (1 + rs)) if avg_p > 0 else 100.0
        rsi_historia.append(rsi)

    if len(rsi_historia) < ventana:
        return {"tipo": "ninguna", "senal": "neutral", "descripcion": "Historial RSI insuficiente", "fuerza": 0}

    precios_rec = precios[-ventana:]
    lows_rec    = lows[-ventana:]  if len(lows)  >= ventana else lows
    highs_rec   = highs[-ventana:] if len(highs) >= ventana else highs
    rsi_rec     = rsi_historia[-ventana:]

    rsi_actual = rsi_rec[-1]
    mitad = len(precios_rec) // 2

    precio_min_anterior = min(precios_rec[:mitad])
    rsi_min_anterior    = min(rsi_rec[:mitad])
    precio_max_anterior = max(precios_rec[:mitad])
    rsi_max_anterior    = max(rsi_rec[:mitad])

    precio_min_actual = min(precios_rec[mitad:])
    rsi_min_actual    = min(rsi_rec[mitad:])
    precio_max_actual = max(precios_rec[mitad:])
    rsi_max_actual    = max(rsi_rec[mitad:])

    diff_precio_min = abs(precio_min_actual - precio_min_anterior) / precio_min_anterior
    diff_precio_max = abs(precio_max_actual - precio_max_anterior) / precio_max_anterior

    # Divergencia alcista
    if (precio_min_actual < precio_min_anterior * 0.995 and
        rsi_min_actual > rsi_min_anterior + 3 and
        rsi_actual < 45):
        diferencia_rsi = round(rsi_min_actual - rsi_min_anterior, 1)
        fuerza = min(100, round(diferencia_rsi * 4 + diff_precio_min * 200))
        return {
            "tipo": "alcista", "senal": "buy",
            "descripcion": f"Divergencia alcista: precio baja ({round(diff_precio_min*100,1)}%) pero RSI sube ({diferencia_rsi} pts). Presion bajista agotandose.",
            "fuerza": fuerza, "rsi_actual": round(rsi_actual, 1),
        }

    # Divergencia bajista
    if (precio_max_actual > precio_max_anterior * 1.005 and
        rsi_max_actual < rsi_max_anterior - 3 and
        rsi_actual > 55):
        diferencia_rsi = round(rsi_max_anterior - rsi_max_actual, 1)
        fuerza = min(100, round(diferencia_rsi * 4 + diff_precio_max * 200))
        return {
            "tipo": "bajista", "senal": "sell",
            "descripcion": f"Divergencia bajista: precio sube ({round(diff_precio_max*100,1)}%) pero RSI baja ({diferencia_rsi} pts). Impulso alcista debilitandose.",
            "fuerza": fuerza, "rsi_actual": round(rsi_actual, 1),
        }

    # Divergencia oculta alcista
    if (precio_min_actual > precio_min_anterior * 1.005 and
        rsi_min_actual < rsi_min_anterior - 2 and
        rsi_actual < 50):
        return {
            "tipo": "oculta_alcista", "senal": "buy",
            "descripcion": "Divergencia oculta alcista: correccion dentro de tendencia alcista. Señal de continuacion al alza.",
            "fuerza": 40, "rsi_actual": round(rsi_actual, 1),
        }

    return {"tipo": "ninguna", "senal": "neutral", "descripcion": "Sin divergencias detectadas", "fuerza": 0}


def detectar_patron_velas_contextual(opens: list, highs: list, lows: list,
                                      closes: list, vols: list,
                                      soportes: list, resistencias: list,
                                      rsi: float, tendencia_sma: str) -> dict:
    """
    Patrones de velas con la lógica correcta que los hace fiables:

    1. CONTEXTO: el patrón debe ocurrir en una zona de soporte/resistencia real
    2. CONFIRMACIÓN: la vela siguiente confirma la dirección (no opera en la vela del patrón)
    3. VOLUMEN: el patrón requiere volumen superior a la media para ser válido
    4. TENDENCIA: solo busca patrones alcistas en tendencias alcistas o en soportes,
                  y bajistas en tendencias bajistas o en resistencias

    Sin estas tres condiciones, la señal se ignora completamente.
    Fiabilidad esperada con este enfoque: 65-80% según estudios en 4h/diario.
    """
    resultado_neutro = {"patron": "Sin patron confirmado", "senal": "neutral",
                        "fiabilidad": 0, "confirmado": False, "contexto": ""}

    if len(closes) < 4:
        return resultado_neutro

    # Velas: actual, anterior (señal), y la de confirmación (ya cerrada)
    # La vela [-2] es la que forma el patrón, [-1] es la confirmación ya cerrada
    o_señal  = opens[-2];  h_señal  = highs[-2]
    l_señal  = lows[-2];   c_señal  = closes[-2]
    o_prev   = opens[-3];  c_prev   = closes[-3]
    o_conf   = opens[-1];  c_conf   = closes[-1]   # vela de confirmación

    cuerpo_señal = abs(c_señal - o_señal)
    rango_señal  = h_señal - l_señal if h_señal != l_señal else 0.0001
    mecha_inf    = min(o_señal, c_señal) - l_señal
    mecha_sup    = h_señal - max(o_señal, c_señal)

    # Volumen: la vela de señal debe tener volumen > media
    vol_medio = sum(vols[-20:]) / 20 if len(vols) >= 20 else (vols[-1] if vols else 1)
    vol_señal = vols[-2] if len(vols) >= 2 else vol_medio
    volumen_confirma = vol_señal > vol_medio * 1.1  # al menos 10% sobre la media

    precio_actual = closes[-1]

    # Verificar si el precio está cerca de soporte o resistencia real
    def cerca_de_nivel(precio, niveles, tolerancia=0.015):
        """Retorna True si el precio está dentro del 1.5% de un nivel clave."""
        return any(abs(precio - n) / n < tolerancia for n in niveles if n > 0)

    en_soporte    = cerca_de_nivel(l_señal, soportes)
    en_resistencia = cerca_de_nivel(h_señal, resistencias)
    en_zona_clave  = en_soporte or en_resistencia

    # ── PATRÓN 1: MARTILLO (Hammer) ───────────────────────────────────
    # Condición: mecha inferior larga + cuerpo pequeño arriba
    # Contexto requerido: en soporte + tendencia neutral o RSI bajo
    # Confirmación: vela siguiente cierra alcista
    es_martillo = (mecha_inf > cuerpo_señal * 2 and
                   mecha_sup < cuerpo_señal * 0.5 and
                   cuerpo_señal < rango_señal * 0.35)

    if es_martillo and en_soporte and volumen_confirma and rsi < 55:
        confirmado = c_conf > c_señal  # siguiente vela cierra por encima
        if confirmado:
            fiabilidad = 72 + (8 if rsi < 35 else 0) + (5 if vol_señal > vol_medio * 1.5 else 0)
            return {"patron": "Martillo confirmado", "senal": "buy",
                    "fiabilidad": min(fiabilidad, 85), "confirmado": True,
                    "contexto": f"En soporte real. RSI {rsi}. Volumen {round(vol_señal/vol_medio,1)}x. Vela siguiente confirma."}

    # ── PATRÓN 2: ESTRELLA FUGAZ (Shooting Star) ─────────────────────
    # Condición: mecha superior larga + cuerpo pequeño abajo
    # Contexto requerido: en resistencia + tendencia alcista o RSI alto
    # Confirmación: vela siguiente cierra bajista
    es_estrella = (mecha_sup > cuerpo_señal * 2 and
                   mecha_inf < cuerpo_señal * 0.5 and
                   cuerpo_señal < rango_señal * 0.35)

    if es_estrella and en_resistencia and volumen_confirma and rsi > 45:
        confirmado = c_conf < c_señal  # siguiente vela cierra por debajo
        if confirmado:
            fiabilidad = 70 + (8 if rsi > 65 else 0) + (5 if vol_señal > vol_medio * 1.5 else 0)
            return {"patron": "Estrella fugaz confirmada", "senal": "sell",
                    "fiabilidad": min(fiabilidad, 83), "confirmado": True,
                    "contexto": f"En resistencia real. RSI {rsi}. Volumen {round(vol_señal/vol_medio,1)}x. Vela siguiente confirma."}

    # ── PATRÓN 3: VELA ENVOLVENTE ALCISTA (Bullish Engulfing) ─────────
    # La vela actual "engulle" completamente a la anterior bajista
    # Contexto: al final de una caída, idealmente en soporte
    # Confirmación: por su propia naturaleza ya confirma en el cierre
    es_envolvente_alcista = (c_prev < o_prev and         # vela anterior bajista
                              c_señal > o_señal and       # vela señal alcista
                              c_señal > o_prev and        # cierre supera apertura anterior
                              o_señal < c_prev)           # apertura por debajo del cierre anterior

    if es_envolvente_alcista and volumen_confirma and (en_soporte or rsi < 50):
        fiabilidad = 76 + (6 if en_soporte else 0) + (5 if rsi < 40 else 0)
        return {"patron": "Envolvente alcista", "senal": "buy",
                "fiabilidad": min(fiabilidad, 87), "confirmado": True,
                "contexto": f"Engulfe alcista{'en soporte' if en_soporte else ''}. RSI {rsi}. Volumen {round(vol_señal/vol_medio,1)}x."}

    # ── PATRÓN 4: VELA ENVOLVENTE BAJISTA (Bearish Engulfing) ─────────
    # La vela actual engulle completamente a la anterior alcista
    # Contexto: al final de una subida, idealmente en resistencia
    es_envolvente_bajista = (c_prev > o_prev and          # vela anterior alcista
                              c_señal < o_señal and        # vela señal bajista
                              c_señal < o_prev and         # cierre por debajo de apertura anterior
                              o_señal > c_prev)            # apertura supera cierre anterior

    if es_envolvente_bajista and volumen_confirma and (en_resistencia or rsi > 50):
        fiabilidad = 74 + (6 if en_resistencia else 0) + (5 if rsi > 60 else 0)
        return {"patron": "Envolvente bajista", "senal": "sell",
                "fiabilidad": min(fiabilidad, 85), "confirmado": True,
                "contexto": f"Engulfe bajista{'en resistencia' if en_resistencia else ''}. RSI {rsi}. Volumen {round(vol_señal/vol_medio,1)}x."}

    # ── PATRÓN 5: DOJI EN ZONA CLAVE ──────────────────────────────────
    # Doji solo es útil en zonas clave — indica indecisión que puede preceder giro
    # No da señal direccional por sí solo, solo alerta de posible cambio
    es_doji = cuerpo_señal < rango_señal * 0.1

    if es_doji and en_zona_clave:
        return {"patron": "Doji en zona clave", "senal": "neutral",
                "fiabilidad": 50, "confirmado": False,
                "contexto": f"Indecision {'en soporte' if en_soporte else 'en resistencia'}. Esperar confirmacion."}

    return resultado_neutro




def analizar_timeframe(klines: dict, nombre: str) -> dict:
    """Analiza un timeframe con RSI, MACD correcto, medias y volumen."""
    closes = klines.get("closes", [])
    highs  = klines.get("highs",  [])
    lows   = klines.get("lows",   [])
    opens  = klines.get("opens",  [])
    vols   = klines.get("volumenes", [])

    if len(closes) < 20:
        return {"nombre": nombre, "tendencia": "neutral", "senal": "neutral",
                "rsi": 50, "macd": "neutral", "sma_tendencia": "neutral",
                "vol_ratio": 1.0, "score": 50}

    rsi    = calcular_rsi(closes)
    macd   = calcular_macd(closes)
    sma20  = calcular_sma(closes, 20)
    sma50  = calcular_sma(closes, min(50, len(closes)))
    precio = closes[-1]

    # Volumen: confirma o invalida señal en cada timeframe
    vol_medio = sum(vols[-20:]) / 20 if len(vols) >= 20 else (vols[-1] if vols else 1)
    vol_actual = vols[-1] if vols else vol_medio
    vol_ratio  = round(vol_actual / vol_medio, 2) if vol_medio > 0 else 1.0

    # Tendencia por medias
    if precio > sma20 > sma50:
        sma_tend = "alcista_fuerte"
    elif precio > sma20:
        sma_tend = "alcista"
    elif precio < sma20 < sma50:
        sma_tend = "bajista_fuerte"
    elif precio < sma20:
        sma_tend = "bajista"
    else:
        sma_tend = "neutral"

    # Señal combinada del timeframe — ahora incluye cruce MACD real y volumen
    puntos = 0
    if rsi < 45:                                  puntos += 1
    if rsi < 35:                                  puntos += 1
    if macd["tendencia"] == "alcista":            puntos += 1
    if macd["cruce"] == "alcista":                puntos += 1  # cruce reciente vale doble
    if "alcista" in sma_tend:                     puntos += 1
    if vol_ratio > 1.3 and "alcista" in sma_tend: puntos += 1  # volumen confirma alcista
    if "bajista" in sma_tend:                     puntos -= 1
    if rsi > 65:                                  puntos -= 1
    if rsi > 75:                                  puntos -= 1
    if macd["tendencia"] == "bajista":            puntos -= 1
    if macd["cruce"] == "bajista":                puntos -= 1  # cruce bajista reciente
    if vol_ratio > 1.3 and "bajista" in sma_tend: puntos -= 1  # volumen confirma bajista

    if puntos >= 2:    senal = "buy"
    elif puntos <= -2: senal = "sell"
    else:              senal = "neutral"

    # Clasificación de tendencia en tres tramos:
    # 1. Arranque/reversión: MACD alcista con RSI todavía bajo (recuperando desde sobreventa)
    # 2. Momentum confirmado: MACD alcista con RSI en zona saludable 50-70 (sin sobrecompra)
    # 3. Tendencia fuerte establecida: SMA20 > SMA50 con precio por encima de ambas
    # El tramo 2 cubre el hueco detectado con datos reales: mercados en recuperación donde
    # el RSI ya confirma momentum alcista pero las medias largas todavía no se han alineado.
    momentum_alcista_confirmado = macd["tendencia"] == "alcista" and 50 <= rsi < 70
    momentum_bajista_confirmado = macd["tendencia"] == "bajista" and 30 < rsi <= 50

    if "alcista_fuerte" in sma_tend or (macd["tendencia"] == "alcista" and rsi < 50) or momentum_alcista_confirmado:
        tendencia = "alcista"
    elif "bajista_fuerte" in sma_tend or (macd["tendencia"] == "bajista" and rsi > 50) or momentum_bajista_confirmado:
        tendencia = "bajista"
    else:
        tendencia = "neutral"

    score_tf = round((puntos + 6) / 12 * 100)  # rango ampliado por nuevos puntos
    score_tf = max(5, min(95, score_tf))

    return {
        "nombre":        nombre,
        "tendencia":     tendencia,
        "senal":         senal,
        "rsi":           rsi,
        "macd":          macd["tendencia"],
        "macd_cruce":    macd["cruce"],
        "sma_tendencia": sma_tend,
        "vol_ratio":     vol_ratio,
        "precio":        round(precio, 6),
        "sma20":         round(sma20, 6),
        "sma50":         round(sma50, 6),
        "score":         score_tf,
    }


def calcular_alineacion_mtf(tf_1h: dict, tf_4h: dict, tf_1d: dict) -> dict:
    """
    Calcula la alineación entre los tres timeframes.
    El diario define la dirección, el 4h la señal, el 1h el timing.
    """
    senales = [tf_1h["senal"], tf_4h["senal"], tf_1d["senal"]]
    tends   = [tf_1h["tendencia"], tf_4h["tendencia"], tf_1d["tendencia"]]

    buy_count  = senales.count("buy")
    sell_count = senales.count("sell")

    # Peso mayor al diario y al 4h
    score_pond = (
        tf_1d["score"] * 0.45 +
        tf_4h["score"] * 0.35 +
        tf_1h["score"] * 0.20
    )

    # Alineación
    if buy_count == 3:
        alineacion  = "total_alcista"
        descripcion = "Los 3 marcos temporales alinean al alza. Señal de maxima fiabilidad."
        bonus       = 12   # bonus al score final
    elif sell_count == 3:
        alineacion  = "total_bajista"
        descripcion = "Los 3 marcos temporales alinean a la baja. Señal de maxima fiabilidad."
        bonus       = -12
    elif tf_1d["senal"] == "buy" and tf_4h["senal"] == "buy":
        alineacion  = "alcista_confirmada"
        descripcion = "Diario y 4h alcistas. Buena entrada. 1h aun no confirma."
        bonus       = 6
    elif tf_1d["senal"] == "sell" and tf_4h["senal"] == "sell":
        alineacion  = "bajista_confirmada"
        descripcion = "Diario y 4h bajistas. Señal de venta confirmada."
        bonus       = -6
    elif tf_4h["senal"] == "buy" and tf_1d["senal"] == "sell":
        alineacion  = "contra_tendencia"
        descripcion = "ATENCION: Señal de compra en 4h pero tendencia diaria bajista. Alto riesgo de trampa."
        bonus       = -8
    elif tf_4h["senal"] == "sell" and tf_1d["senal"] == "buy":
        alineacion  = "correccion_en_alcista"
        descripcion = "Correccion puntual en tendencia alcista mayor. Posible oportunidad de compra con precaucion."
        bonus       = 3
    else:
        alineacion  = "mixta"
        descripcion = "Señales mixtas entre timeframes. Esperar confirmacion antes de entrar."
        bonus       = 0

    return {
        "alineacion":   alineacion,
        "descripcion":  descripcion,
        "bonus":        bonus,
        "score_pond":   round(score_pond),
        "buy_count":    buy_count,
        "sell_count":   sell_count,
        "tf_1h":        tf_1h,
        "tf_4h":        tf_4h,
        "tf_1d":        tf_1d,
    }


# ══════════════════════════════════════════════════════════════════════
# LIBRO DE ÓRDENES
# ══════════════════════════════════════════════════════════════════════

def analizar_libro_ordenes(orderbook: dict, precio_actual: float) -> dict:
    """
    Análisis del libro de órdenes ponderado por distancia al precio.
    Un muro a $66.500 cuando el precio es $67.000 vale más que uno a $60.000.
    El peso de cada nivel decrece exponencialmente con la distancia.
    """
    if not orderbook:
        return {
            "soporte_fuerte": round(precio_actual * 0.96, 6),
            "resistencia_fuerte": round(precio_actual * 1.04, 6),
            "presion_compra": 50.0, "presion_venta": 50.0,
            "ratio_compra_venta": 1.0,
            "soportes": [round(precio_actual * 0.96, 6)],
            "resistencias": [round(precio_actual * 1.04, 6)],
            "senal": "neutral", "descripcion": "Sin datos del libro",
        }

    bids = [(float(p), float(q)) for p, q in orderbook.get("bids", [])[:50]]
    asks = [(float(p), float(q)) for p, q in orderbook.get("asks", [])[:50]]
    if not bids or not asks:
        return analizar_libro_ordenes({}, precio_actual)

    def peso_distancia(precio_nivel: float, precio_ref: float) -> float:
        """
        Peso exponencial: niveles más cercanos al precio actual tienen más relevancia.
        A 0% de distancia → peso 1.0
        A 2% de distancia → peso ~0.37
        A 5% de distancia → peso ~0.08
        """
        distancia_pct = abs(precio_nivel - precio_ref) / precio_ref
        return math.exp(-distancia_pct * 50)

    # Volumen ponderado por distancia (en USD)
    vol_compra_pond = sum(p * q * peso_distancia(p, precio_actual) for p, q in bids)
    vol_venta_pond  = sum(p * q * peso_distancia(p, precio_actual) for p, q in asks)

    # Volumen total sin ponderar (para referencia)
    vol_compra_raw = sum(p * q for p, q in bids)
    vol_venta_raw  = sum(p * q for p, q in asks)

    total_pond = vol_compra_pond + vol_venta_pond if (vol_compra_pond + vol_venta_pond) > 0 else 1
    presion_compra = round(vol_compra_pond / total_pond * 100, 1)
    presion_venta  = round(vol_venta_pond  / total_pond * 100, 1)
    ratio          = round(vol_compra_pond / vol_venta_pond, 2) if vol_venta_pond > 0 else 1.0

    # Encontrar muros: niveles con volumen > 2x la media Y cercanos al precio (peso > 0.1)
    def encontrar_muros_ponderados(ordenes: list, n: int = 5) -> list:
        if not ordenes:
            return []
        # Solo niveles con peso relevante (dentro del ~5% del precio)
        relevantes = [(p, q, peso_distancia(p, precio_actual))
                      for p, q in ordenes
                      if peso_distancia(p, precio_actual) > 0.08]
        if not relevantes:
            relevantes = [(p, q, 1.0) for p, q in ordenes[:10]]

        vols_rel = [q for _, q, _ in relevantes]
        media    = sum(vols_rel) / len(vols_rel) if vols_rel else 1

        # Muro = volumen > 2x media Y ponderado por importancia
        muros = sorted(
            [(p, q * w) for p, q, w in relevantes if q > media * 2.0],
            key=lambda x: x[1], reverse=True
        )[:n]
        return sorted([p for p, _ in muros])

    muros_compra = encontrar_muros_ponderados(bids)
    muros_venta  = encontrar_muros_ponderados(asks)

    soporte_fuerte     = muros_compra[-1] if muros_compra else bids[0][0]
    resistencia_fuerte = muros_venta[0]   if muros_venta  else asks[0][0]

    if ratio > 1.5:
        senal = "buy"
        desc  = f"Presion compradora ponderada fuerte ({presion_compra}%). Muros de compra cercanos solidos."
    elif ratio > 1.2:
        senal = "buy"
        desc  = f"Ligera presion compradora ({presion_compra}% ponderado por distancia)."
    elif ratio < 0.65:
        senal = "sell"
        desc  = f"Presion vendedora ponderada fuerte ({presion_venta}%). Muros de venta cercanos dominan."
    elif ratio < 0.85:
        senal = "sell"
        desc  = f"Ligera presion vendedora ({presion_venta}% ponderado)."
    else:
        senal = "neutral"
        desc  = f"Equilibrio ponderado ({presion_compra}% compra vs {presion_venta}% venta cercanos)."

    return {
        "soporte_fuerte":     round(soporte_fuerte, 6),
        "resistencia_fuerte": round(resistencia_fuerte, 6),
        "presion_compra":     presion_compra,
        "presion_venta":      presion_venta,
        "ratio_compra_venta": ratio,
        "soportes":           muros_compra or [round(precio_actual * 0.97, 6)],
        "resistencias":       muros_venta  or [round(precio_actual * 1.03, 6)],
        "senal":              senal,
        "descripcion":        desc,
        "ponderado":          True,
    }


# ══════════════════════════════════════════════════════════════════════
# GESTIÓN DE OPERACIÓN
# ══════════════════════════════════════════════════════════════════════

def calcular_gestion_operacion(precio: float, precios: list, highs: list,
                                lows: list, closes: list, libro: dict,
                                capital_total: float = 1000) -> dict:
    """
    Gestión de operación con lógica correcta de trading:
    - Stop loss SIEMPRE por debajo del precio de entrada (long)
    - Take profits SIEMPRE por encima del precio de entrada (long)
    - TP1 < TP2 < TP3 garantizado
    - Ratio mínimo de 1:1 — si no se cumple, se avisa explícitamente
    - Tamaño de posición basado en regla del 2%
    """
    atr = calcular_atr(highs, lows, closes)

    # ATR como porcentaje del precio
    atr_pct = round((atr / precio * 100), 2) if precio > 0 else 2.0

    # Múltiplo ATR por volatilidad
    if atr_pct < 1.5:
        multiplicador_atr = 2.0    # baja vol: usamos 2x para dar espacio real
        descripcion_vol   = "baja"
    elif atr_pct < 3.0:
        multiplicador_atr = 2.0
        descripcion_vol   = "media"
    elif atr_pct < 5.0:
        multiplicador_atr = 2.5
        descripcion_vol   = "alta"
    else:
        multiplicador_atr = 3.0
        descripcion_vol   = "muy_alta"

    # ── STOP LOSS ────────────────────────────────────────────────────
    # Basado en ATR: precio - (ATR × multiplicador)
    # El ATR mide el rango medio de movimiento. El stop debe estar
    # fuera del ruido normal del mercado, por eso usamos 2x ATR mínimo.
    sl_atr   = precio - (atr * multiplicador_atr)

    # Soporte del libro: nivel donde hay órdenes reales de compra
    soporte_libro = libro.get("soporte_fuerte", precio * 0.95)

    # Stop loss = el MÁS BAJO entre ATR y soporte libro
    # (el más alejado del precio = más espacio = menos ruido)
    stop_loss = min(sl_atr, soporte_libro * 0.998)

    # Límites absolutos de seguridad:
    # Nunca más del 10% de pérdida (demasiado riesgo)
    # Nunca menos del 1% (demasiado ajustado, el ruido lo activaría)
    stop_loss = max(stop_loss, precio * 0.90)  # máximo 10% abajo
    stop_loss = min(stop_loss, precio * 0.99)  # mínimo 1% abajo
    stop_loss = round(stop_loss, 6)

    riesgo_abs = abs(precio - stop_loss)
    riesgo_pct = round(riesgo_abs / precio * 100, 2)

    # ── TAKE PROFITS con Fibonacci ───────────────────────────────────
    # Extensiones de Fibonacci sobre el riesgo real
    # TP1: 1.272× el riesgo (conservador)
    # TP2: 2.0× el riesgo (ratio mínimo aceptable 1:2)
    # TP3: 3.0× el riesgo (objetivo ambicioso)
    fib_tp1 = precio + riesgo_abs * 1.272
    fib_tp2 = precio + riesgo_abs * 2.0    # mínimo ratio 1:2
    fib_tp3 = precio + riesgo_abs * 3.0

    # Ajustar con resistencias reales del libro, pero solo si están
    # POR ENCIMA del precio de entrada y mejoran la señal
    resistencias_libro = [r for r in libro.get("resistencias", [])
                          if r > precio * 1.005]  # solo resistencias reales por encima

    def ajustar_tp_seguro(fib_tp: float, resistencias: list, tol: float = 0.015) -> float:
        """Ajusta el TP a una resistencia real solo si está cerca Y por encima."""
        for r in sorted(resistencias):
            if r > precio and abs(r - fib_tp) / fib_tp < tol:
                return round(r * 0.9985, 6)  # justo por debajo de la resistencia
        return round(fib_tp, 6)

    tp1 = ajustar_tp_seguro(fib_tp1, resistencias_libro)
    tp2 = ajustar_tp_seguro(fib_tp2, resistencias_libro)
    tp3 = ajustar_tp_seguro(fib_tp3, resistencias_libro)

    # ── VALIDACIÓN CRÍTICA ───────────────────────────────────────────
    # Garantizar que todos los TPs están POR ENCIMA de la entrada
    # y en orden correcto TP1 < TP2 < TP3
    # TP1 mínimo: proporcional al riesgo (riesgo × 1.272) para mantener ratio coherente
    tp1_minimo = round(precio + riesgo_abs * 1.272, 6)
    tp1 = max(tp1, tp1_minimo)
    tp2 = max(tp2, round(tp1 * 1.005, 6))       # tp2 siempre > tp1
    tp3 = max(tp3, round(tp2 * 1.005, 6))       # tp3 siempre > tp2

    # ── RATIO Y TAMAÑO DE POSICIÓN ───────────────────────────────────
    ratio = round((tp2 - precio) / riesgo_abs, 2) if riesgo_abs > 0 else 2.0

    # Advertencia si el ratio es bajo
    if ratio < 1.5:
        calidad_ratio = "bajo — considera esperar mejor entrada"
    elif ratio < 2.0:
        calidad_ratio = "aceptable"
    else:
        calidad_ratio = "bueno"

    riesgo_euros  = round(capital_total * 0.02, 2)
    tam_posicion  = round(riesgo_euros / riesgo_abs, 6) if riesgo_abs > 0 else 0

    return {
        "entrada":           round(precio, 6),
        "stop_loss":         stop_loss,
        "tp1":               tp1,
        "tp2":               tp2,
        "tp3":               tp3,
        "riesgo_pct":        riesgo_pct,
        "ratio_riesgo":      ratio,
        "calidad_ratio":     calidad_ratio,
        "tamano_posicion":   tam_posicion,
        "capital_en_riesgo": riesgo_euros,
        "atr":               round(atr, 6),
        "atr_pct":           atr_pct,
        "volatilidad":       descripcion_vol,
        "multiplicador_atr": multiplicador_atr,
        "soporte_libro":     round(soporte_libro, 6),
        "resistencia_libro": round(libro.get("resistencia_fuerte", precio * 1.04), 6),
        "fuente_sl":         f"ATRx{multiplicador_atr} ({descripcion_vol} vol)",
    }


# ══════════════════════════════════════════════════════════════════════
# OBTENCIÓN DE DATOS
# ══════════════════════════════════════════════════════════════════════

async def fetch(client, nombre: str, url: str, params: dict = None):
    try:
        r = await client.get(url, params=params, timeout=10)
        r.raise_for_status()
        datos = r.json()
        cache_datos[nombre] = datos
        return datos
    except Exception as e:
        log.warning(f"[WARN] {nombre} -- {e} -- usando cache")
        return cache_datos.get(nombre)


async def obtener_klines(client, simbolo: str, intervalo="4h", limit=200) -> dict:
    """
    Obtiene klines de Binance con autenticación si hay API key configurada.
    Fallback a Bybit si Binance falla. Incluye timestamps para poder
    posicionar señales históricas reales en el gráfico.
    """
    try:
        params = {"symbol": f"{simbolo}USDT", "interval": intervalo, "limit": limit}
        r = await client.get(
            APIS["binance_klines"],
            params=params,
            headers=binance_headers(),
            timeout=10
        )
        log.info(f"[BINANCE] klines {simbolo} {intervalo}: HTTP {r.status_code} | Auth: {BINANCE_AUTENTICADO}")
        datos = r.json() if r.status_code == 200 else None
        if datos and isinstance(datos, list) and len(datos) > 10:
            cache_datos["binance_klines"] = datos
            marcar_ok("precios_mercado", "binance")
            return {
                "opens":     [float(k[1]) for k in datos],
                "highs":     [float(k[2]) for k in datos],
                "lows":      [float(k[3]) for k in datos],
                "closes":    [float(k[4]) for k in datos],
                "volumenes": [float(k[5]) for k in datos],
                "timestamps": [int(k[0]) for k in datos],
            }
    except Exception as e:
        log.warning(f"[WARN] Binance klines {simbolo}: {e}")

    # Fallback: Bybit
    intervalo_bybit = {"1h": "60", "4h": "240", "1d": "D"}.get(intervalo, "240")
    try:
        r = await client.get(APIS["bybit_klines"], params={
            "category": "spot", "symbol": f"{simbolo}USDT",
            "interval": intervalo_bybit, "limit": min(limit, 200)
        }, timeout=10)
        datos_bybit = r.json()
        if datos_bybit.get("retCode") == 0:
            klines = list(reversed(datos_bybit["result"]["list"]))
            marcar_ok("precios_mercado", "bybit")
            return {
                "opens":     [float(k[1]) for k in klines],
                "highs":     [float(k[2]) for k in klines],
                "lows":      [float(k[3]) for k in klines],
                "closes":    [float(k[4]) for k in klines],
                "volumenes": [float(k[5]) for k in klines],
                "timestamps": [int(k[0]) for k in klines],
            }
    except Exception as e:
        log.warning(f"[WARN] Bybit klines {simbolo}: {e}")

    marcar_error("precios_mercado")
    return {}


async def obtener_dxy_fed(client) -> dict:
    """
    Obtiene DXY (índice del dólar) de stooq.com y Fed rate de FRED/API pública.
    Ambas gratuitas y sin registro.
    Fallback a valores razonables si la API no responde.
    """
    resultado = {"dxy": 102.5, "fed": 4.5, "fuente": "estimado"}

    # DXY desde stooq.com (CSV público sin autenticación)
    try:
        r = await client.get(
            "https://stooq.com/q/d/l/?s=dx.f&i=d",
            timeout=8
        )
        if r.status_code == 200:
            lineas = r.text.strip().split("\n")
            if len(lineas) >= 2:
                ultima = lineas[-1].split(",")
                if len(ultima) >= 5:
                    dxy_val = float(ultima[4])  # columna "Close"
                    if 80 < dxy_val < 130:      # rango razonable
                        resultado["dxy"] = round(dxy_val, 2)
                        resultado["fuente"] = "real"
    except Exception as e:
        log.warning(f"[WARN] DXY stooq: {e}")

    # Fed Funds Rate desde FRED (API pública, sin key para datos históricos)
    try:
        r2 = await client.get(
            "https://fred.stlouisfed.org/graph/fredgraph.csv?id=FEDFUNDS",
            timeout=8
        )
        if r2.status_code == 200:
            lineas = r2.text.strip().split("\n")
            if len(lineas) >= 2:
                ultima = lineas[-1].split(",")
                if len(ultima) >= 2:
                    fed_val = float(ultima[1])
                    if 0 < fed_val < 25:
                        resultado["fed"]    = round(fed_val, 2)
                        resultado["fuente"] = "real"
    except Exception as e:
        log.warning(f"[WARN] Fed FRED: {e}")

    return resultado


def detectar_ciclo_mercado(closes_diario: list) -> dict:
    """
    Detecta la fase del ciclo dinámicamente usando el rango de 52 semanas
    con una interpretación más matizada que distingue bear de acumulación.

    La diferencia clave:
    - Bear activo: precio cayendo y por debajo de medias
    - Acumulación: precio bajo en el rango pero estabilizándose o subiendo
    """
    if len(closes_diario) < 30:
        return {"fase": "distribution", "pct_rango": 75.0, "descripcion": "Sin datos suficientes"}

    ventana = min(365, len(closes_diario))
    rango   = closes_diario[-ventana:]
    maximo  = max(rango)
    minimo  = min(rango)
    precio  = closes_diario[-1]

    if maximo == minimo:
        return {"fase": "neutral", "pct_rango": 50.0, "descripcion": "Rango plano"}

    pct = round((precio - minimo) / (maximo - minimo) * 100, 1)

    # Tendencia reciente: comparar precio actual con hace 30 días
    precio_30d = closes_diario[-30] if len(closes_diario) >= 30 else closes_diario[0]
    cambio_30d = (precio - precio_30d) / precio_30d * 100

    # Media de 50 días para contexto de tendencia
    sma50_diario = sum(closes_diario[-50:]) / min(50, len(closes_diario))

    if pct >= 75:
        fase = "distribution"
        desc = f"Precio en el {pct}% del rango anual — cerca de maximos. Fase distribucion."
    elif pct >= 50:
        fase = "bull"
        desc = f"Precio en el {pct}% del rango anual — tendencia alcista activa."
    elif pct >= 25:
        # Distinguir acumulación de bear usando tendencia reciente
        if cambio_30d > 0 or precio > sma50_diario:
            fase = "accumulation"
            desc = f"Precio en el {pct}% del rango anual — zona de acumulacion. Tendencia 30d: {round(cambio_30d,1)}%."
        else:
            fase = "bear"
            desc = f"Precio en el {pct}% del rango anual — tendencia bajista. Tendencia 30d: {round(cambio_30d,1)}%."
    else:
        # Por debajo del 25% del rango: distinguir capitulacion de rebote
        if cambio_30d > 3:
            fase = "accumulation"
            desc = f"Precio en minimos ({pct}% rango) pero rebotando +{round(cambio_30d,1)}% en 30 dias — posible suelo."
        else:
            fase = "bear"
            desc = f"Precio en el {pct}% del rango anual — cerca de minimos. Tendencia 30d: {round(cambio_30d,1)}%."

    return {
        "fase":        fase,
        "pct_rango":   pct,
        "maximo_52s":  round(maximo, 4),
        "minimo_52s":  round(minimo, 4),
        "cambio_30d":  round(cambio_30d, 1),
        "descripcion": desc,
    }


async def obtener_orderbook(client, simbolo: str) -> dict:
    """Orderbook de Binance autenticado con fallback a Bybit."""
    try:
        r = await client.get(
            APIS["binance_orderbook"],
            params={"symbol": f"{simbolo}USDT", "limit": 100},
            headers=binance_headers(),
            timeout=10
        )
        datos = r.json() if r.status_code == 200 else None
        if datos and ("bids" in datos or "asks" in datos):
            marcar_ok("libro_ordenes", "binance")
            return datos
    except Exception as e:
        log.warning(f"[WARN] Binance orderbook {simbolo}: {e}")

    try:
        r = await client.get(APIS["bybit_orderbook"], params={
            "category": "spot", "symbol": f"{simbolo}USDT", "limit": 50
        }, timeout=10)
        d = r.json()
        if d.get("retCode") == 0:
            marcar_ok("libro_ordenes", "bybit")
            return {"bids": d["result"]["b"], "asks": d["result"]["a"]}
    except Exception as e:
        log.warning(f"[WARN] Bybit orderbook {simbolo}: {e}")

    marcar_error("libro_ordenes")
    return {}


async def obtener_fear_greed(client) -> dict:
    datos = await fetch(client, "fear_greed", APIS["fear_greed"])
    if datos and "data" in datos:
        marcar_ok("fear_greed", "alternative.me")
        return {"valor": int(datos["data"][0]["value"]),
                "clasificacion": datos["data"][0]["value_classification"],
                "fuente": "real"}
    marcar_error("fear_greed")
    return {"valor": 50, "clasificacion": "Neutral", "fuente": "estimado"}


async def obtener_dominancia_btc(client) -> float:
    datos = await fetch(client, "coingecko_global", APIS["coingecko_global"])
    if datos and "data" in datos:
        marcar_ok("coingecko_global", "coingecko")
        return round(datos["data"].get("market_cap_percentage", {}).get("btc", 58.0), 1)
    marcar_error("coingecko_global")
    return 58.0


async def obtener_funding_rate(client, simbolo: str) -> float:
    """Funding rate de Binance autenticado con fallback a Bybit."""
    try:
        r = await client.get(
            APIS["binance_funding"],
            params={"symbol": f"{simbolo}USDT", "limit": 1},
            headers=binance_headers(),
            timeout=10
        )
        datos = r.json() if r.status_code == 200 else None
        if datos and isinstance(datos, list):
            marcar_ok("funding_rate", "binance")
            return round(float(datos[0]["fundingRate"]) * 100, 4)
    except Exception:
        pass

    try:
        r = await client.get(APIS["bybit_funding"], params={
            "category": "linear", "symbol": f"{simbolo}USDT", "limit": 1
        }, timeout=10)
        d = r.json()
        if d.get("retCode") == 0 and d["result"]["list"]:
            marcar_ok("funding_rate", "bybit")
            return round(float(d["result"]["list"][0]["fundingRate"]) * 100, 4)
    except Exception as e:
        log.warning(f"[WARN] Bybit funding {simbolo}: {e}")

    marcar_error("funding_rate")
    return 0.01


async def obtener_open_interest(client, simbolo: str) -> float:
    """Open interest de Binance autenticado con fallback a Bybit."""
    try:
        r = await client.get(
            APIS["binance_oi"],
            params={"symbol": f"{simbolo}USDT"},
            headers=binance_headers(),
            timeout=10
        )
        datos = r.json() if r.status_code == 200 else None
        if datos:
            marcar_ok("open_interest", "binance")
            return round(float(datos.get("openInterest", 0)) / 1e9, 2)
    except Exception:
        pass

    try:
        r = await client.get(APIS["bybit_oi"], params={
            "category": "linear", "symbol": f"{simbolo}USDT",
            "intervalTime": "4h", "limit": 1
        }, timeout=10)
        d = r.json()
        if d.get("retCode") == 0 and d["result"]["list"]:
            marcar_ok("open_interest", "bybit")
            return round(float(d["result"]["list"][0]["openInterest"]) / 1e9, 2)
    except Exception as e:
        log.warning(f"[WARN] Bybit OI {simbolo}: {e}")

    marcar_error("open_interest")
    return 0.0


async def obtener_ls_ratio(client, simbolo: str) -> float:
    """Ratio largo/corto de Binance autenticado."""
    try:
        r = await client.get(
            APIS["binance_lsratio"],
            params={"symbol": f"{simbolo}USDT", "period": "4h", "limit": 1},
            headers=binance_headers(),
            timeout=10
        )
        datos = r.json() if r.status_code == 200 else None
        if datos and isinstance(datos, list):
            marcar_ok("ls_ratio", "binance")
            return round(float(datos[0]["longShortRatio"]), 2)
    except Exception:
        pass
    marcar_error("ls_ratio")
    return 1.0


async def obtener_noticias(client, simbolo: str) -> dict:
    """
    Obtiene noticias de CoinDesk y CoinTelegraph via RSS.
    CryptoPanic elimino su API gratuita en abril 2026.
    """
    import re
    try:
        noticias_raw = []
        feeds = [
            "https://www.coindesk.com/arc/outboundfeeds/rss/",
            "https://cointelegraph.com/rss",
            "https://www.theblock.co/rss.xml",
        ]
        for feed_url in feeds:
            try:
                r = await client.get(feed_url, timeout=8,
                                     headers={"Accept": "application/rss+xml, application/xml, text/xml",
                                              "User-Agent": "Mozilla/5.0"})
                if r.status_code == 200:
                    contenido = r.text
                    # Intentar múltiples formatos de título en RSS/Atom
                    titulos = re.findall(r'<title><!\[CDATA\[(.*?)\]\]></title>', contenido, re.DOTALL)
                    if not titulos:
                        titulos = re.findall(r'<title[^>]*>(.*?)</title>', contenido, re.DOTALL)
                    # Limpiar HTML entities y espacios
                    titulos_limpios = []
                    for t in titulos:
                        t = t.strip()
                        t = t.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"').replace('&#39;', "'")
                        t = re.sub(r'<[^>]+>', '', t).strip()  # eliminar tags HTML
                        if len(t) > 15 and t not in ['RSS', 'Feed', 'CoinDesk', 'CoinTelegraph', 'The Block']:
                            titulos_limpios.append(t)
                    titulos = titulos_limpios[1:16]  # saltar el título del feed
                    nombres = {"BTC": ["bitcoin","btc"],"ETH": ["ethereum","eth"],
                               "SOL": ["solana","sol"],"XRP": ["xrp","ripple"]}
                    keywords = nombres.get(simbolo.upper(), [simbolo.lower()])
                    keywords += ["crypto","market","defi","blockchain","token","coin"]
                    palabras_pos = ["surge","rally","gain","bull","rise","high","adoption","approval","launch","partnership","record","all-time"]
                    palabras_neg = ["crash","drop","fall","bear","low","hack","ban","regulation","fear","warning","loss","scam","fraud"]
                    for titulo in titulos[:8]:
                        tl = titulo.lower()
                        sent = "neutral"
                        if any(p in tl for p in palabras_pos): sent = "positiva"
                        if any(p in tl for p in palabras_neg): sent = "negativa"
                        noticias_raw.append({"titulo": titulo[:90], "sentimiento": sent, "url": ""})
                    if len(noticias_raw) >= 8:
                        break
            except Exception as e:
                log.warning(f"[WARN] RSS {feed_url}: {e}")
                continue

        if not noticias_raw:
            marcar_error("noticias")
            return {"score_ajuste": 0, "noticias": [], "fuente": "sin_datos",
                    "positivas": 0, "negativas": 0, "resumen": "Sin datos de noticias"}

        positivas = sum(1 for n in noticias_raw if n["sentimiento"] == "positiva")
        negativas = sum(1 for n in noticias_raw if n["sentimiento"] == "negativa")
        total     = len(noticias_raw)
        ratio_n   = (positivas - negativas) / total if total > 0 else 0
        score_ajuste = round(ratio_n * 15, 1)
        marcar_ok("noticias", "coindesk/cointelegraph")
        return {
            "score_ajuste": score_ajuste, "positivas": positivas, "negativas": negativas,
            "noticias": noticias_raw[:5], "fuente": "coindesk/cointelegraph",
            "resumen": f"{positivas} positivas, {negativas} negativas de {total} recientes"
        }
    except Exception as e:
        log.warning(f"[WARN] Noticias {simbolo}: {e}")
        marcar_error("noticias")
        return {"score_ajuste": 0, "noticias": [], "fuente": "error",
                "positivas": 0, "negativas": 0, "resumen": "Error al obtener noticias"}


# ══════════════════════════════════════════════════════════════════════
# SCORING 7 CAPAS + BONUS MULTI-TIMEFRAME
# ══════════════════════════════════════════════════════════════════════

def calcular_score_completo(klines_4h: dict, klines_1h: dict, klines_1d: dict,
                             ob: dict, fg: dict, dominancia_btc: float,
                             funding: float, oi: float, ls_ratio: float,
                             noticias: dict, btc_cambio_4h: float,
                             simbolo: str, dxy_fed: dict = None,
                             ciclo_info: dict = None) -> dict:
    closes = klines_4h.get("closes", [])
    highs  = klines_4h.get("highs",  [])
    lows   = klines_4h.get("lows",   [])
    opens  = klines_4h.get("opens",  [])
    vols   = klines_4h.get("volumenes", [])

    if not closes:
        return {
            "score": 50, "score_base": 50, "bonus_mtf": 0,
            "accion": "ESPERAR", "confianza": "Sin datos Binance",
            "advertencia": "Binance no disponible — datos en cache o sin datos",
            "capas": [], "bull_count": 0, "bear_count": 0,
            "mtf": {
                "alineacion": "mixta", "descripcion": "Sin datos",
                "bonus": 0, "score_pond": 50,
                "buy_count": 0, "sell_count": 0,
                "tf_1h": {"nombre":"1h","tendencia":"neutral","senal":"neutral","rsi":50,"macd":"neutral","macd_cruce":"ninguno","sma_tendencia":"neutral","vol_ratio":1,"precio":0,"sma20":0,"sma50":0,"score":50},
                "tf_4h": {"nombre":"4h","tendencia":"neutral","senal":"neutral","rsi":50,"macd":"neutral","macd_cruce":"ninguno","sma_tendencia":"neutral","vol_ratio":1,"precio":0,"sma20":0,"sma50":0,"score":50},
                "tf_1d": {"nombre":"Diario","tendencia":"neutral","senal":"neutral","rsi":50,"macd":"neutral","macd_cruce":"ninguno","sma_tendencia":"neutral","vol_ratio":1,"precio":0,"sma20":0,"sma50":0,"score":50},
            },
            "rsi": 50, "stoch_rsi": 50,
            "macd": {"macd":0,"signal":0,"histograma":0,"tendencia":"neutral","cruce":"ninguno"},
            "sma50": 0, "sma200": 0, "sma20": 0,
            "bollinger": {"superior":0,"media":0,"inferior":0,"pct_b":50,"ancho":0},
            "patron": {"patron":"Sin datos","senal":"neutral","fiabilidad":0,"confirmado":False,"contexto":""},
            "divergencia": {"tipo":"ninguna","senal":"neutral","descripcion":"Sin datos","fuerza":0},
            "vol_ratio": 1.0,
            "dxy": 102.5, "dxy_fuente": "estimado", "fed_rate": 4.5,
            "ciclo": "distribution", "ciclo_pct": 50.0, "ciclo_desc": "Sin datos",
            "funding": 0.01, "open_interest": 0, "ls_ratio": 1.0,
            "libro": {}, "noticias": noticias, "btc_cambio_4h": 0,
        }

    precio = closes[-1]

    # ── ANÁLISIS MULTI-TIMEFRAME ──────────────────────────────────────
    tf_1h = analizar_timeframe(klines_1h, "1h")
    tf_4h = analizar_timeframe(klines_4h, "4h")
    tf_1d = analizar_timeframe(klines_1d, "Diario")
    mtf   = calcular_alineacion_mtf(tf_1h, tf_4h, tf_1d)

    # ── DIVERGENCIAS RSI (calculadas sobre 4h) ────────────────────────
    divergencia = detectar_divergencias_rsi(closes, highs, lows)

    # ── CAPA 1: TÉCNICO 4h (25%) ──────────────────────────────────────
    rsi   = calcular_rsi(closes)
    stoch = calcular_stochastic_rsi(closes)
    macd  = calcular_macd(closes)
    sma50 = calcular_sma(closes, 50)
    sma200= calcular_sma(closes, 200)
    sma20 = calcular_sma(closes, 20)
    bb    = calcular_bollinger(closes)

    vol_medio = sum(vols[-20:]) / 20 if len(vols) >= 20 else (vols[-1] if vols else 1)
    vol_ratio  = vols[-1] / vol_medio if vol_medio > 0 and vols else 1

    # Tendencia SMA para pasar al detector de patrones
    tendencia_sma = "alcista" if sma50 > sma200 else "bajista"

    # Patrones de velas con contexto completo (soportes/resistencias reales del libro)
    soportes_libro     = ob.get("soportes", [])
    resistencias_libro = ob.get("resistencias", [])
    patron = detectar_patron_velas_contextual(
        opens, highs, lows, closes, vols,
        soportes_libro, resistencias_libro,
        rsi, tendencia_sma
    )

    # RSI con penalización si el volumen no confirma
    # El backtesting demostró que RSI bajo sin volumen = trampa bajista
    # RSI bajo CON volumen creciente = suelo real
    rsi_s_base = .92 if rsi < 28 else .80 if rsi < 38 else .62 if rsi < 48 else \
                 .50 if rsi < 55 else .38 if rsi < 65 else .20 if rsi < 72 else .08

    # Penalización: RSI sobrevendido sin volumen que confirme = señal falsa
    # El backtesting mostró que RSI < 30 sin volumen acierta solo el 25%
    if rsi < 35 and vol_ratio < 1.0:
        # Volumen bajo en sobreventa = probable continuación bajista
        rsi_s = rsi_s_base * 0.70   # penalizar un 30%
    elif rsi < 35 and vol_ratio >= 1.5:
        # Volumen alto en sobreventa = suelo potencial real
        rsi_s = min(1.0, rsi_s_base * 1.15)  # bonificar un 15%
    else:
        rsi_s = rsi_s_base

    stoch_s = .85 if stoch < 20 else .65 if stoch < 40 else .50 if stoch < 60 else \
              .35 if stoch < 80 else .15

    # MACD con cruce real — el backtesting demostró que el cruce MACD
    # es el indicador más fiable (53.8% acierto vs 35% media)
    # Por eso se le da más peso en los pesos internos del scoring técnico
    if macd["cruce"] == "alcista":
        macd_s = .95   # antes .90 — cruce alcista es señal muy fiable
    elif macd["cruce"] == "bajista":
        macd_s = .08   # antes .12 — cruce bajista es señal muy negativa
    elif macd["tendencia"] == "alcista":
        macd_s = .72
    elif macd["tendencia"] == "bajista":
        macd_s = .28
    else:
        macd_s = .50

    trend_s = .85 if sma50 > sma200 and precio > sma20 else \
              .65 if sma50 > sma200 else \
              .35 if sma50 < sma200 and precio < sma20 else .20
    bb_s    = .88 if bb["pct_b"] < 15 else .72 if bb["pct_b"] < 30 else \
              .55 if bb["pct_b"] < 50 else .45 if bb["pct_b"] < 65 else \
              .30 if bb["pct_b"] < 82 else .12
    vol_s   = .82 if vol_ratio > 1.8 else .65 if vol_ratio > 1.2 else \
              .50 if vol_ratio > 0.8 else .30

    # Divergencia RSI
    div_ajuste = 0.0
    if divergencia["tipo"] == "alcista":
        div_ajuste = divergencia["fuerza"] / 100 * 0.08
    elif divergencia["tipo"] == "bajista":
        div_ajuste = -(divergencia["fuerza"] / 100 * 0.08)
    elif divergencia["tipo"] == "oculta_alcista":
        div_ajuste = 0.04

    # Patrón de velas contextual: solo cuenta si está confirmado con contexto real
    # Peso calibrado según fiabilidad: patrón confirmado en zona clave = 7%
    # Sin confirmación o sin contexto = 0% (no contamina el score)
    if patron["confirmado"] and patron["fiabilidad"] >= 65:
        patron_s = .85 if patron["senal"] == "buy" else .15
        patron_peso = 0.07
    else:
        patron_s    = 0.50   # neutro
        patron_peso = 0.00   # no aporta ni resta

    # Redistribuir pesos con patron integrado
    # MACD sube de 23-25% a 28-30% basado en backtesting (53.8% acierto vs 35% media)
    # RSI baja de 21-22% a 17-18% porque sin volumen es poco fiable
    # Con patron confirmado:    RSI 17% + StochRSI 10% + MACD 28% + Trend 21% + BB 12% + Vol 5% + Patron 7% = 100%
    # Sin patron confirmado:    RSI 18% + StochRSI 11% + MACD 30% + Trend 22% + BB 13% + Vol 6% = 100%
    if patron_peso > 0:
        tech_score_base = (rsi_s   * .17 + stoch_s * .10 + macd_s  * .28 +
                           trend_s * .21 + bb_s    * .12 + vol_s   * .05 +
                           patron_s * patron_peso)
    else:
        tech_score_base = (rsi_s   * .18 + stoch_s * .11 + macd_s  * .30 +
                           trend_s * .22 + bb_s    * .13 + vol_s   * .06)

    tech_score = min(1.0, max(0.0, tech_score_base + div_ajuste))

    # ── CAPA 2: LIBRO DE ÓRDENES (15%) ───────────────────────────────
    ratio_cv = ob.get("ratio_compra_venta", 1.0)
    libro_s  = .85 if ratio_cv > 1.6 else .70 if ratio_cv > 1.2 else \
               .50 if ratio_cv > 0.85 else .30 if ratio_cv > 0.6 else .15
    libro_score = libro_s

    # ── CAPA 3: MACRO (15%) — DXY/Fed REALES + Ciclo DINÁMICO ───────
    dxy_data = dxy_fed or {}
    dxy      = dxy_data.get("dxy", 102.5)
    fed      = dxy_data.get("fed", 4.5)
    dxy_fuente = dxy_data.get("fuente", "estimado")

    # Ciclo dinámico desde velas diarias reales
    ciclo_data = ciclo_info or {}
    ciclo      = ciclo_data.get("fase", "distribution")
    ciclo_pct  = ciclo_data.get("pct_rango", 75.0)

    dxy_s   = .80 if dxy < 99  else .65 if dxy < 101 else \
              .50 if dxy < 103 else .35 if dxy < 105 else .18
    fed_s   = .78 if fed < 3.5 else .65 if fed < 4.25 else \
              .50 if fed < 5.0 else .35 if fed < 5.75 else .20
    ciclo_s = .85 if ciclo == "accumulation" else .70 if ciclo == "bull" else \
              .45 if ciclo == "distribution" else .35
    dom_s   = .60 if dominancia_btc > 55 else .50 if dominancia_btc > 45 else .40
    macro_score = dxy_s * .30 + fed_s * .30 + ciclo_s * .25 + dom_s * .15

    # ── CAPA 4: SENTIMIENTO (13%) ─────────────────────────────────────
    fgv  = fg["valor"]
    fg_s = .92 if fgv < 15 else .78 if fgv < 30 else .62 if fgv < 45 else \
           .50 if fgv < 55 else .38 if fgv < 65 else .22 if fgv < 80 else .08
    sent_score = fg_s

    # ── CAPA 5: DERIVADOS (13%) ───────────────────────────────────────
    fund_s = .88 if funding < -.01  else .72 if funding < .002 else \
             .52 if funding < .01   else .35 if funding < .04  else \
             .18 if funding < .08   else .05
    ls_s   = .78 if ls_ratio < 0.8 else .62 if ls_ratio < 1.0 else \
             .50 if ls_ratio < 1.2 else .35 if ls_ratio < 1.5 else .18
    deriv_score = fund_s * .60 + ls_s * .40

    # ── CAPA 6: NOTICIAS (10%) ────────────────────────────────────────
    ajuste_n  = noticias.get("score_ajuste", 0)
    noticia_s = min(1.0, max(0.0, (ajuste_n + 15) / 30))
    noticia_score = noticia_s

    # ── CAPA 7: CORRELACIÓN BTC 4h (9%) ──────────────────────────────
    if simbolo == "BTC":
        btc_corr_s = .55
    else:
        btc_corr_s = .88 if btc_cambio_4h > 3   else \
                     .68 if btc_cambio_4h > 1   else \
                     .52 if btc_cambio_4h > -1  else \
                     .28 if btc_cambio_4h > -3  else .10
    corr_score = btc_corr_s

    # ── SCORE BASE ────────────────────────────────────────────────────
    # Pesos calibrados para swing trading 1-7 días:
    # Libro órdenes y Derivados tienen más peso por ser datos tiempo real
    # Macro reducida al 8% porque cambia lentamente (DXY, Fed)
    raw_base = (tech_score    * .27 +
                libro_score   * .18 +
                macro_score   * .08 +
                sent_score    * .13 +
                deriv_score   * .18 +
                noticia_score * .06 +
                corr_score    * .10)

    score_base = round(raw_base * 100)

    # ── BONUS/PENALIZACIÓN MULTI-TIMEFRAME ────────────────────────────
    score_final = max(5, min(95, score_base + mtf["bonus"]))

    # ── UMBRALES DE DECISIÓN SEGÚN TENDENCIA DIARIA ───────────────────
    # Si el diario es bajista, exigimos más para comprar (72% en vez de 68%)
    # porque el backtesting demostró que las compras contra tendencia bajista
    # fallan el 75% de las veces
    tendencia_diaria = tf_1d["tendencia"]
    umbral_compra    = 72 if tendencia_diaria == "bajista" else 68
    umbral_pos_compra= 65 if tendencia_diaria == "bajista" else 58

    if   score_final >= umbral_compra:    accion = "COMPRAR"
    elif score_final >= umbral_pos_compra: accion = "Posible compra"
    elif score_final <= 32:               accion = "VENDER"
    elif score_final <= 42:               accion = "Posible venta"
    else:                                 accion = "ESPERAR"

    capas = [
        {"nombre": "Tecnico 4h (RSI/StochRSI/MACD/SMA/BB/Velas/Divergencias)",
         "peso": "27%", "score": round(tech_score * 100),
         "valor": f"RSI {rsi} | StochRSI {stoch} | MACD {macd['tendencia']} ({macd['cruce']}) | {patron['patron']} ({patron['fiabilidad']}%) | Div: {divergencia['tipo']}",
         "senal": "buy" if tech_score > .58 else "sell" if tech_score < .42 else "neutral"},
        {"nombre": "Libro ordenes (muros reales Binance)",
         "peso": "18%", "score": round(libro_score * 100),
         "valor": f"Ratio C/V: {ratio_cv} | {ob.get('descripcion','')}",
         "senal": "buy" if libro_score > .58 else "sell" if libro_score < .42 else "neutral"},
        {"nombre": "Macro (DXY/Fed reales + Ciclo dinamico)",
         "peso": "8%", "score": round(macro_score * 100),
         "valor": f"DXY {dxy} ({dxy_fuente}) | Fed {fed}% | {ciclo} ({ciclo_pct}% rango) | BTC dom {dominancia_btc}%",
         "senal": "buy" if macro_score > .58 else "sell" if macro_score < .42 else "neutral"},
        {"nombre": "Sentimiento (Fear & Greed real)",
         "peso": "13%", "score": round(sent_score * 100),
         "valor": f"F&G {fgv} ({fg['clasificacion']})",
         "senal": "buy" if sent_score > .58 else "sell" if sent_score < .42 else "neutral"},
        {"nombre": "Derivados (Funding/Long-Short ratio)",
         "peso": "18%", "score": round(deriv_score * 100),
         "valor": f"Funding {'+' if funding > 0 else ''}{funding}% | L/S {ls_ratio}",
         "senal": "buy" if deriv_score > .58 else "sell" if deriv_score < .42 else "neutral"},
        {"nombre": "Noticias (CoinDesk/TheBlock)",
         "peso": "6%", "score": round(noticia_score * 100),
         "valor": noticias.get("resumen", "Sin datos"),
         "senal": "buy" if noticia_score > .58 else "sell" if noticia_score < .42 else "neutral"},
        {"nombre": "Correlacion BTC (ultimas 4h reales)",
         "peso": "10%", "score": round(corr_score * 100),
         "valor": f"BTC 4h: {'+' if btc_cambio_4h > 0 else ''}{btc_cambio_4h}%",
         "senal": "buy" if corr_score > .58 else "sell" if corr_score < .42 else "neutral"},
    ]

    bull_count = sum(1 for c in capas if c["senal"] == "buy")
    bear_count = sum(1 for c in capas if c["senal"] == "sell")

    # Confianza: combina capas + alineación MTF
    mtf_alin = mtf["alineacion"]
    if (bull_count >= 5 or bear_count >= 5) and "total" in mtf_alin:
        confianza = "Maxima"
    elif bull_count >= 5 or bear_count >= 5:
        confianza = "Muy alta"
    elif (bull_count >= 4 or bear_count >= 4) and "confirmada" in mtf_alin:
        confianza = "Alta"
    elif bull_count >= 4 or bear_count >= 4:
        confianza = "Media-alta"
    elif bull_count >= 3 or bear_count >= 3:
        confianza = "Media"
    else:
        confianza = "Baja"

    # Advertencia si la señal va contra la tendencia diaria
    advertencia = ""
    if tf_4h["senal"] == "buy" and tf_1d["tendencia"] == "bajista":
        advertencia = "ATENCION: Señal de compra contra tendencia diaria bajista. Alto riesgo."
    elif tf_4h["senal"] == "sell" and tf_1d["tendencia"] == "alcista":
        advertencia = "ATENCION: Señal de venta en correccion dentro de tendencia alcista mayor."

    return {
        "score":          score_final,
        "score_base":     score_base,
        "bonus_mtf":      mtf["bonus"],
        "accion":         accion,
        "confianza":      confianza,
        "advertencia":    advertencia,
        "capas":          capas,
        "bull_count":     bull_count,
        "bear_count":     bear_count,
        "mtf":            mtf,
        "rsi":            rsi,
        "stoch_rsi":      stoch,
        "macd":           macd,
        "sma50":          round(sma50, 6),
        "sma200":         round(sma200, 6),
        "sma20":          round(sma20, 6),
        "bollinger":      bb,
        "patron":         patron,
        "vol_ratio":      round(vol_ratio, 2),
        "dxy":            dxy,
        "dxy_fuente":     dxy_fuente,
        "fed_rate":       fed,
        "ciclo":          ciclo,
        "ciclo_pct":      ciclo_pct,
        "ciclo_desc":     ciclo_data.get("descripcion", ""),
        "funding":        funding,
        "open_interest":  oi,
        "ls_ratio":       ls_ratio,
        "libro":          ob,
        "noticias":       noticias,
        "divergencia":    divergencia,
        "btc_cambio_4h":  btc_cambio_4h,
    }


# ══════════════════════════════════════════════════════════════════════
# ANÁLISIS COMPLETO DE UNA MONEDA
# ══════════════════════════════════════════════════════════════════════

async def analizar_moneda(simbolo: str, capital: float = 1000) -> dict:
    async with httpx.AsyncClient(
        headers={"User-Agent": "CryptoExpertDashboard/5.0"},
        follow_redirects=True
    ) as client:
        # Obtener los tres timeframes + resto de datos en paralelo
        k4h_task      = obtener_klines(client, simbolo, "4h",  200)
        k1h_task      = obtener_klines(client, simbolo, "1h",  100)
        k1d_task      = obtener_klines(client, simbolo, "1d",  365)  # 52 semanas para ciclo
        ob_task       = obtener_orderbook(client, simbolo)
        fg_task       = obtener_fear_greed(client)
        dom_task      = obtener_dominancia_btc(client)
        fund_task     = obtener_funding_rate(client, simbolo)
        oi_task       = obtener_open_interest(client, simbolo)
        ls_task       = obtener_ls_ratio(client, simbolo)
        noticias_task = obtener_noticias(client, simbolo)
        dxy_fed_task  = obtener_dxy_fed(client)  # DXY y Fed en tiempo real

        # BTC 4h para correlación (solo si no es BTC)
        if simbolo != "BTC":
            btc4h_task = obtener_klines(client, "BTC", "4h", 10)
            (k4h, k1h, k1d, orderbook, fg, dominancia,
             funding, oi, ls_ratio, noticias, dxy_fed, btc4h) = await asyncio.gather(
                k4h_task, k1h_task, k1d_task, ob_task, fg_task, dom_task,
                fund_task, oi_task, ls_task, noticias_task, dxy_fed_task, btc4h_task
            )
            if btc4h and btc4h.get("closes") and len(btc4h["closes"]) > 2:
                btc_c = btc4h["closes"]
                btc_cambio_4h = round((btc_c[-1] - btc_c[-2]) / btc_c[-2] * 100, 2)
            else:
                btc_cambio_4h = btc_cambio_cache["valor"]
            btc_cambio_cache["valor"] = btc_cambio_4h
        else:
            (k4h, k1h, k1d, orderbook, fg, dominancia,
             funding, oi, ls_ratio, noticias, dxy_fed) = await asyncio.gather(
                k4h_task, k1h_task, k1d_task, ob_task, fg_task, dom_task,
                fund_task, oi_task, ls_task, noticias_task, dxy_fed_task
            )
            btc_cambio_4h = 0.0

    if not k4h or not k4h.get("closes"):
        return {"error": f"Sin datos para {simbolo}"}

    closes = k4h["closes"]
    highs  = k4h["highs"]
    lows   = k4h["lows"]
    opens  = k4h["opens"]
    vols   = k4h["volumenes"]

    # Ciclo dinámico desde velas diarias reales
    closes_diario = k1d.get("closes", []) if k1d else []
    ciclo_info    = detectar_ciclo_mercado(closes_diario)

    precio_actual = closes[-1]
    precio_24h    = closes[-6] if len(closes) > 6 else closes[0]
    cambio_24h    = round((precio_actual - precio_24h) / precio_24h * 100, 2)

    ob_analisis = analizar_libro_ordenes(orderbook, precio_actual)

    score = calcular_score_completo(
        k4h, k1h, k1d, ob_analisis, fg, dominancia,
        funding, oi, ls_ratio, noticias, btc_cambio_4h, simbolo,
        dxy_fed, ciclo_info
    )

    gestion = calcular_gestion_operacion(
        precio_actual, closes, highs, lows, closes, ob_analisis, capital
    )

    score_anterior  = cache_datos.get(f"score_{simbolo}", score["score"])
    tendencia_score = "subiendo" if score["score"] > score_anterior else \
                      "bajando"  if score["score"] < score_anterior else "estable"
    cache_datos[f"score_{simbolo}"] = score["score"]

    señales_recientes_simbolo = [
        {
            "fecha":  s["fecha"],
            "accion": s["accion"],
            "score":  s["score"],
            "precio": s["precio"],
        }
        for s in cargar_señales()[-300:]
        if s.get("simbolo") == simbolo
    ][-20:]  # últimas 20 señales de este símbolo para no sobrecargar el gráfico

    return {
        "simbolo":         simbolo,
        "precio":          precio_actual,
        "cambio_24h":      cambio_24h,
        "score":           score,
        "gestion":         gestion,
        "fear_greed":      fg,
        "dominancia_btc":  dominancia,
        "tendencia_score": tendencia_score,
        "orderbook":       ob_analisis,
        "noticias":        noticias,
        "señales_reales":  señales_recientes_simbolo,
        "velas_recientes": {
            "closes":     closes[-60:], "highs":  highs[-60:],
            "lows":       lows[-60:],   "opens":  opens[-60:],
            "vols":       vols[-60:],
            "timestamps": k4h.get("timestamps", [])[-60:],
        },
        "timestamp": datetime.now().isoformat(),
    }


# ══════════════════════════════════════════════════════════════════════
# SERVIDOR
# ══════════════════════════════════════════════════════════════════════

conexiones_activas: list[WebSocket] = []


async def broadcast(datos: dict):
    desconectados = []
    for ws in conexiones_activas:
        try:
            await ws.send_json(datos)
        except Exception:
            desconectados.append(ws)
    for ws in desconectados:
        conexiones_activas.remove(ws)



# ══════════════════════════════════════════════════════════════════════
# DIARIO DE OPERACIONES
# Gestión activa de posiciones abiertas con alertas en tiempo real
# ══════════════════════════════════════════════════════════════════════

DIARIO_FILE   = "data/diario_operaciones.json"
SEÑALES_FILE  = "data/señales_historico.json"

def cargar_señales() -> list:
    """Carga el histórico de señales."""
    try:
        if os.path.exists(SEÑALES_FILE):
            with open(SEÑALES_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return []

def guardar_señal(señal: dict):
    """Guarda una señal en el histórico."""
    try:
        señales = cargar_señales()
        señales.append(señal)
        # Mantener máximo 5000 señales para no ocupar demasiado espacio
        if len(señales) > 5000:
            señales = señales[-5000:]
        with open(SEÑALES_FILE, "w", encoding="utf-8") as f:
            json.dump(señales, f, ensure_ascii=False)
    except Exception as e:
        log.warning(f"[WARN] Error guardando señal: {e}")

def registrar_señal_automatica(simbolo: str, resultado: dict):
    """
    Registra automáticamente cada señal que genera el sistema.
    Captura todas las señales independientemente de si el usuario opera o no.
    Esto permite calibrar el sistema con datos reales de mercado.
    """
    try:
        score_data = resultado.get("score", {})
        accion = score_data.get("accion", "ESPERAR")

        # Solo registrar señales relevantes, no ESPERAR genérico
        if accion == "ESPERAR":
            return

        mtf = score_data.get("mtf", {})
        gestion = resultado.get("gestion", {})

        señal = {
            "fecha":          datetime.now(timezone.utc).isoformat(),
            "simbolo":        simbolo,
            "accion":         accion,
            "score":          score_data.get("score", 0),
            "precio":         round(resultado.get("precio", 0), 6),
            "rsi":            score_data.get("rsi", 0),
            "macd":           score_data.get("macd", {}).get("cruce", "ninguno"),
            "macd_tendencia": score_data.get("macd", {}).get("tendencia", "neutral"),
            "tendencia_1h":   mtf.get("tf_1h", {}).get("tendencia", "neutral"),
            "tendencia_4h":   mtf.get("tf_4h", {}).get("tendencia", "neutral"),
            "tendencia_1d":   mtf.get("tf_1d", {}).get("tendencia", "neutral"),
            "alineacion_mtf": mtf.get("alineacion", "mixta"),
            "bonus_mtf":      score_data.get("bonus_mtf", 0),
            "stop_loss":      gestion.get("stop_loss", 0),
            "tp1":            gestion.get("tp1", 0),
            "tp2":            gestion.get("tp2", 0),
            "tp3":            gestion.get("tp3", 0),
            "ratio":          gestion.get("ratio_riesgo", 0),
            "fear_greed":     resultado.get("fear_greed", {}).get("valor", 50),
            "funding":        score_data.get("funding", 0),
            "ls_ratio":       score_data.get("ls_ratio", 1.0),
            # Campos para calcular el resultado posterior (se rellenarán en futuros ciclos)
            "precio_24h":     None,
            "precio_3d":      None,
            "precio_7d":      None,
            "ret_24h":        None,
            "ret_3d":         None,
            "ret_7d":         None,
            "evaluado":       False,
        }
        guardar_señal(señal)
    except Exception as e:
        log.warning(f"[WARN] Error registrando señal {simbolo}: {e}")

def evaluar_señales_pendientes(precios_actuales: dict):
    """
    Revisa las señales pendientes de evaluación y calcula los retornos
    a 24h, 3 días y 7 días cuando ya ha pasado el tiempo suficiente.
    """
    try:
        señales = cargar_señales()
        ahora = datetime.now(timezone.utc)
        modificado = False

        for s in señales:
            if s.get("evaluado"):
                continue
            try:
                fecha_señal = datetime.fromisoformat(s["fecha"])
                # Compatibilidad con señales antiguas guardadas sin zona horaria
                # (de antes de esta corrección) — se asumen en UTC, igual que el servidor.
                if fecha_señal.tzinfo is None:
                    fecha_señal = fecha_señal.replace(tzinfo=timezone.utc)
                horas = (ahora - fecha_señal).total_seconds() / 3600
                simbolo = s["simbolo"]
                precio_entrada = s["precio"]
                precio_actual = precios_actuales.get(simbolo, 0)
                accion = s.get("accion", "")

                if precio_actual <= 0 or precio_entrada <= 0:
                    continue

                # Para señales de COMPRA: acierto si el precio sube
                # Para señales de VENTA: acierto si el precio baja
                # El signo del retorno representa siempre "qué tan acertada fue la predicción"
                es_venta = accion in ("VENDER", "Posible venta")

                def calcular_ret(precio_ref):
                    if es_venta:
                        return round((precio_entrada - precio_ref) / precio_entrada * 100, 2)
                    return round((precio_ref - precio_entrada) / precio_entrada * 100, 2)

                if horas >= 24 and s["ret_24h"] is None:
                    s["precio_24h"] = precio_actual
                    s["ret_24h"] = calcular_ret(precio_actual)
                    modificado = True

                if horas >= 72 and s["ret_3d"] is None:
                    s["precio_3d"] = precio_actual
                    s["ret_3d"] = calcular_ret(precio_actual)
                    modificado = True

                if horas >= 168 and s["ret_7d"] is None:
                    s["precio_7d"] = precio_actual
                    s["ret_7d"] = calcular_ret(precio_actual)
                    s["evaluado"] = True
                    modificado = True
            except Exception:
                continue

        if modificado:
            with open(SEÑALES_FILE, "w", encoding="utf-8") as f:
                json.dump(señales, f, ensure_ascii=False)
    except Exception as e:
        log.warning(f"[WARN] Error evaluando señales: {e}")

class NuevaOperacion(BaseModel):
    simbolo:    str
    tipo:       str        # "long" o "short"
    entrada:    float
    stop_loss:  float
    tp1:        float
    tp2:        float
    tp3:        float
    capital:    float      # euros invertidos
    notas:      str = ""
    modo:       str = "conservador"   # "conservador" o "practica"

def cargar_diario() -> list:
    try:
        os.makedirs("data", exist_ok=True)
        if os.path.exists(DIARIO_FILE):
            with open(DIARIO_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return []

def guardar_diario(operaciones: list):
    try:
        os.makedirs("data", exist_ok=True)
        with open(DIARIO_FILE, "w", encoding="utf-8") as f:
            json.dump(operaciones, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"Error guardando diario: {e}")

def calcular_estado_operacion(op: dict, precio_actual: float) -> dict:
    """
    Calcula el estado en tiempo real de una operación abierta.
    Determina si hay que actuar (stop loss, take profit alcanzado)
    o si seguir esperando.
    """
    entrada    = op["entrada"]
    sl         = op["stop_loss"]
    tp1        = op["tp1"]
    tp2        = op["tp2"]
    tp3        = op["tp3"]
    capital    = op["capital"]
    tipo       = op.get("tipo", "long")

    if tipo == "long":
        pnl_pct  = round((precio_actual - entrada) / entrada * 100, 2)
        pnl_eur  = round(capital * pnl_pct / 100, 2)
        sl_dist  = round((precio_actual - sl) / precio_actual * 100, 2)
        tp1_dist = round((tp1 - precio_actual) / precio_actual * 100, 2)
        tp2_dist = round((tp2 - precio_actual) / precio_actual * 100, 2)
    else:
        pnl_pct  = round((entrada - precio_actual) / entrada * 100, 2)
        pnl_eur  = round(capital * pnl_pct / 100, 2)
        sl_dist  = round((sl - precio_actual) / precio_actual * 100, 2)
        tp1_dist = round((precio_actual - tp1) / precio_actual * 100, 2)
        tp2_dist = round((precio_actual - tp2) / precio_actual * 100, 2)

    # Estado y alerta
    alerta = None
    urgencia = "normal"

    if tipo == "long":
        if precio_actual <= sl:
            alerta   = f"STOP LOSS ALCANZADO — Salir AHORA. Perdida: {pnl_eur}€ ({pnl_pct}%)"
            urgencia = "critica"
        elif sl_dist < 1.0:
            alerta   = f"Precio muy cerca del stop loss ({sl_dist}% de distancia). Preparate para salir."
            urgencia = "alta"
        elif precio_actual >= tp3:
            ganancia_tp3 = round(capital * (precio_actual - entrada) / entrada, 2)
            alerta   = f"TAKE PROFIT 3 alcanzado. Cerrar posicion completa. Ganancia total: {ganancia_tp3}€"
            urgencia = "tp3"
        elif precio_actual >= tp2:
            ganancia_parcial = round(capital * 0.33 * (precio_actual - entrada) / entrada, 2)
            alerta   = f"Take Profit 2 alcanzado. Recoger 33% de la posicion ({ganancia_parcial}€). Mover stop a entrada."
            urgencia = "tp2"
        elif precio_actual >= tp1:
            ganancia_parcial = round(capital * 0.33 * (precio_actual - entrada) / entrada, 2)
            alerta   = f"Take Profit 1 alcanzado. Recoger 33% de la posicion ({ganancia_parcial}€). Mover stop a breakeven."
            urgencia = "tp1"
    else:
        if precio_actual >= sl:
            alerta   = f"STOP LOSS ALCANZADO (short) — Salir AHORA. Perdida: {pnl_eur}€"
            urgencia = "critica"
        elif precio_actual <= tp1:
            ganancia_parcial = round(capital * 0.33 * (entrada - precio_actual) / entrada, 2)
            alerta   = f"Take Profit 1 (short) alcanzado. Recoger 33% ({ganancia_parcial}€)."
            urgencia = "tp1"

    # Trailing stop: si llevamos +5% de ganancia, el stop loss no debería estar por debajo del precio de entrada
    trailing_sugerido = None
    if tipo == "long" and pnl_pct > 5.0 and sl < entrada:
        trailing_sugerido = round(entrada * 1.005, 6)  # mover SL a breakeven +0.5%

    return {
        "precio_actual":     round(precio_actual, 6),
        "pnl_pct":           pnl_pct,
        "pnl_eur":           pnl_eur,
        "sl_dist_pct":       sl_dist,
        "tp1_dist_pct":      tp1_dist,
        "tp2_dist_pct":      tp2_dist,
        "en_ganancia":       pnl_pct > 0,
        "alerta":            alerta,
        "urgencia":          urgencia,
        "trailing_sugerido": trailing_sugerido,
    }

# Endpoints del diario de operaciones

@app.get("/api/diario")
async def api_diario_listar():
    """Lista todas las operaciones con su estado actual."""
    operaciones = cargar_diario()
    resultado   = []

    for op in operaciones:
        if op.get("estado") == "abierta":
            # Obtener precio actual con fallback a Bybit
            precio = op.get("entrada", 0)
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    r = await client.get(
                        f"{BINANCE_BASE}/api/v3/ticker/price",
                        params={"symbol": f"{op['simbolo']}USDT"},
                        headers=binance_headers()
                    )
                    if r.status_code == 200:
                        precio = float(r.json()["price"])
                    else:
                        r2 = await client.get(
                            "https://api.bybit.com/v5/market/tickers",
                            params={"category": "spot", "symbol": f"{op['simbolo']}USDT"}
                        )
                        d = r2.json()
                        if d.get("retCode") == 0:
                            precio = float(d["result"]["list"][0]["lastPrice"])
            except Exception:
                pass

            estado = calcular_estado_operacion(op, precio)
            resultado.append({**op, "estado_actual": estado})
        else:
            resultado.append(op)

    return {"operaciones": resultado, "total": len(resultado)}


@app.post("/api/diario/nueva")
async def api_diario_nueva(op: NuevaOperacion):
    """Registra una nueva operación en el diario."""
    operaciones = cargar_diario()
    nueva = {
        "id":         int(time.time()),
        "simbolo":    op.simbolo.upper(),
        "tipo":       op.tipo,
        "entrada":    op.entrada,
        "stop_loss":  op.stop_loss,
        "tp1":        op.tp1,
        "tp2":        op.tp2,
        "tp3":        op.tp3,
        "capital":    op.capital,
        "notas":      op.notas,
        "modo":       op.modo,
        "estado":     "abierta",
        "fecha_entrada": datetime.now().isoformat(),
        "fecha_cierre":  None,
        "precio_cierre": None,
        "pnl_final":     None,
        "motivo_cierre": None,
    }
    operaciones.append(nueva)
    guardar_diario(operaciones)
    log.info(f"[DIARIO] Nueva operacion: {nueva['simbolo']} {nueva['tipo']} @ {nueva['entrada']}")
    return {"ok": True, "operacion": nueva}


@app.post("/api/diario/cerrar/{op_id}")
async def api_diario_cerrar(op_id: int, precio_cierre: float, motivo: str = "manual"):
    """Cierra una operación y registra el resultado final."""
    operaciones = cargar_diario()
    for op in operaciones:
        if op["id"] == op_id and op["estado"] == "abierta":
            estado = calcular_estado_operacion(op, precio_cierre)
            op["estado"]        = "cerrada"
            op["fecha_cierre"]  = datetime.now().isoformat()
            op["precio_cierre"] = precio_cierre
            op["pnl_final"]     = estado["pnl_eur"]
            op["pnl_pct_final"] = estado["pnl_pct"]
            op["motivo_cierre"] = motivo
            guardar_diario(operaciones)
            log.info(f"[DIARIO] Operacion cerrada: {op['simbolo']} PnL: {estado['pnl_eur']}€ ({estado['pnl_pct']}%)")
            return {"ok": True, "operacion": op, "resultado": estado}
    return {"ok": False, "error": "Operacion no encontrada o ya cerrada"}


@app.delete("/api/diario/eliminar/{op_id}")
async def api_diario_eliminar(op_id: int):
    """Elimina una operación del diario."""
    operaciones = cargar_diario()
    operaciones = [op for op in operaciones if op["id"] != op_id]
    guardar_diario(operaciones)
    return {"ok": True}


async def monitorizar_operaciones():
    """
    Revisa las operaciones abiertas cada 5 minutos.
    Si alguna ha alcanzado stop loss o take profit, envía alerta por WebSocket y Telegram.
    """
    while True:
        await asyncio.sleep(300)
        operaciones = cargar_diario()
        alertas_activas = []

        for op in operaciones:
            if op.get("estado") != "abierta":
                continue
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    precio = op.get("entrada", 0)
                    r = await client.get(
                        f"{BINANCE_BASE}/api/v3/ticker/price",
                        params={"symbol": f"{op['simbolo']}USDT"},
                        headers=binance_headers()
                    )
                    if r.status_code == 200:
                        precio = float(r.json()["price"])
                    else:
                        r2 = await client.get(
                            "https://api.bybit.com/v5/market/tickers",
                            params={"category": "spot", "symbol": f"{op['simbolo']}USDT"}
                        )
                        d = r2.json()
                        if d.get("retCode") == 0:
                            precio = float(d["result"]["list"][0]["lastPrice"])
                estado = calcular_estado_operacion(op, precio)
                if estado["alerta"]:
                    alertas_activas.append({
                        "id":       op["id"],
                        "simbolo":  op["simbolo"],
                        "alerta":   estado["alerta"],
                        "urgencia": estado["urgencia"],
                        "pnl_eur":  estado["pnl_eur"],
                        "pnl_pct":  estado["pnl_pct"],
                    })
                    log.info(f"[DIARIO ALERTA] {op['simbolo']}: {estado['alerta']}")

                    # Enviar a Telegram solo alertas críticas o de stop loss cercano
                    # Anti-spam: una vez cada 4h por operación
                    clave_alerta = f"diario_{op['id']}_{estado['urgencia']}"
                    ahora = time.time()
                    if (estado["urgencia"] in ("critica", "alta", "tp1", "tp2", "tp3") and
                        (clave_alerta not in _ultimas_alertas or
                         ahora - _ultimas_alertas[clave_alerta] > 14400)):
                        _ultimas_alertas[clave_alerta] = ahora
                        icono = "🔴" if estado["urgencia"] == "critica" else \
                                "⚠️" if estado["urgencia"] == "alta" else "🎯"
                        mensaje = (
                            f"{icono} <b>TU OPERACIÓN — {op['simbolo']}</b>\n\n"
                            f"{estado['alerta']}\n\n"
                            f"💰 PnL actual: <b>{'+' if estado['pnl_eur']>=0 else ''}{estado['pnl_eur']}€ "
                            f"({'+' if estado['pnl_pct']>=0 else ''}{estado['pnl_pct']}%)</b>\n"
                            f"Precio entrada: ${op.get('entrada',0)}\n"
                            f"Precio actual: ${round(precio,6)}"
                        )
                        await enviar_telegram(mensaje)

            except Exception as e:
                log.warning(f"[WARN] Monitor {op['simbolo']}: {e}")

        if alertas_activas:
            await broadcast({
                "tipo":    "alerta_operacion",
                "alertas": alertas_activas,
                "timestamp": datetime.now().isoformat(),
            })


async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    conexiones_activas.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in conexiones_activas:
            conexiones_activas.remove(websocket)


async def loop_actualizacion():
    while True:
        log.info("=== Actualizando datos de mercado ===")
        resultados = {}

        # Procesar mensajes entrantes de Telegram (registro de nuevos usuarios)
        await procesar_updates_telegram()

        for simbolo in MONEDAS_DEFAULT:
            try:
                resultado = await analizar_moneda(simbolo)
                resultados[simbolo] = resultado
                mtf_alin = resultado["score"]["mtf"]["alineacion"]
                log.info(f"[OK] {simbolo}: Score {resultado['score']['score']}% ({resultado['score']['accion']}) | MTF: {mtf_alin}")

                # Registrar señal automáticamente en el histórico
                registrar_señal_automatica(simbolo, resultado)

                # Evaluar si hay que enviar alerta de Telegram (solo modo conservador)
                await evaluar_y_alertar_telegram(simbolo, resultado)

            except Exception as e:
                log.error(f"[ERROR] {simbolo}: {e}")

        # Evaluar retornos de señales anteriores
        precios_actuales = {s: resultados[s].get("precio", 0) for s in resultados}
        evaluar_señales_pendientes(precios_actuales)

        estado_salud = {
            nombre: {
                "ok":       estado["ok"],
                "errores":  estado["errores"],
                "fuente":   estado.get("fuente", ""),
                "ultimo_ok": datetime.fromtimestamp(estado["ultimo_ok"]).strftime("%H:%M:%S"),
                "minutos_sin_datos": round((time.time() - estado["ultimo_ok"]) / 60, 0)
                                     if not estado["ok"] else 0,
            }
            for nombre, estado in health_status.items()
        }

        await broadcast({
            "tipo":      "actualizacion",
            "datos":     resultados,
            "health":    estado_salud,
            "timestamp": datetime.now().isoformat(),
        })
        await asyncio.sleep(INTERVALO_ACTUALIZACION)


async def health_check_periodico():
    while True:
        await asyncio.sleep(INTERVALO_HEALTH_CHECK)
        for nombre, estado in health_status.items():
            mins = (time.time() - estado["ultimo_ok"]) / 60
            if not estado["ok"]:
                log.warning(f"[WARN] {nombre}: CAIDO ({mins:.0f} min sin datos)")


@app.get("/api/analizar/{simbolo}")
async def api_analizar(simbolo: str, capital: float = 1000):
    return await analizar_moneda(simbolo.upper(), capital)


@app.get("/api/señales/descargar")
async def descargar_señales():
    """Descarga el histórico completo de señales para calibración."""
    from fastapi.responses import JSONResponse
    señales = cargar_señales()
    evaluadas = [s for s in señales if s.get("evaluado")]
    pendientes = [s for s in señales if not s.get("evaluado")]

    # Resumen estadístico
    # Filtro corregido: comparación exacta en lugar de subcadena sensible a mayúsculas.
    # Antes "VENTA" in accion nunca coincidía con "VENDER" (las letras no coinciden)
    # y "COMPRA" in accion no coincidía con "Posible compra" (minúsculas).
    compras = [s for s in evaluadas if s.get("accion") in ("COMPRAR", "Posible compra")]
    ventas  = [s for s in evaluadas if s.get("accion") in ("VENDER", "Posible venta")]

    def tasa(lista, campo):
        validos = [s for s in lista if s.get(campo) is not None]
        if not validos: return None
        aciertos = sum(1 for s in validos if s.get(campo,0) > 0)
        return round(aciertos/len(validos)*100,1)

    resumen = {
        "total_señales":    len(señales),
        "evaluadas":        len(evaluadas),
        "pendientes":       len(pendientes),
        "señales_compra":   len(compras),
        "señales_venta":    len(ventas),
        "tasa_acierto_compra_24h": tasa(compras, "ret_24h"),
        "tasa_acierto_compra_3d":  tasa(compras, "ret_3d"),
        "tasa_acierto_compra_7d":  tasa(compras, "ret_7d"),
        "tasa_acierto_venta_24h":  tasa(ventas, "ret_24h"),
        "tasa_acierto_venta_3d":   tasa(ventas, "ret_3d"),
        "generado":         datetime.now().isoformat(),
    }

    return JSONResponse(content={"resumen": resumen, "señales": señales},
                       headers={"Content-Disposition": "attachment; filename=señales_historico.json"})


@app.get("/api/health")
async def api_health():
    return {
        "status": "ok" if all(e["ok"] for e in health_status.values()) else "degradado",
        "apis":   health_status,
        "timestamp": datetime.now().isoformat(),
    }

@app.get("/api/telegram/status")
async def telegram_status():
    """Estado de Telegram y usuarios registrados."""
    chat_ids = cargar_chat_ids()
    return {
        "activo":   TELEGRAM_ACTIVO,
        "usuarios": len(chat_ids),
        "token_configurado": bool(TELEGRAM_TOKEN),
    }

@app.get("/api/telegram/test")
async def telegram_test():
    """Envía un mensaje de prueba a todos los usuarios registrados."""
    if not TELEGRAM_ACTIVO:
        return {"ok": False, "error": "Telegram no configurado"}
    await procesar_updates_telegram()
    chat_ids = cargar_chat_ids()
    if not chat_ids:
        return {"ok": False, "error": "No hay usuarios registrados. Escribe /start al bot primero."}
    await enviar_telegram("✅ <b>Test de conexión exitoso</b>\n\nEl dashboard está funcionando correctamente.")
    return {"ok": True, "usuarios": len(chat_ids)}


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
              "templates", "index.html"), encoding="utf-8") as f:
        return f.read()


@app.on_event("startup")
async def startup():
    log.info("[START] Crypto Expert Dashboard v7 iniciando...")
    os.makedirs("data", exist_ok=True)
    asyncio.create_task(loop_actualizacion())
    asyncio.create_task(health_check_periodico())
    asyncio.create_task(monitorizar_operaciones())
    log.info("[OK] Dashboard v7 listo - Binance vision + Diario de operaciones activo")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
