"""
╔══════════════════════════════════════════════════════════════════════╗
║         BACKTESTING — Crypto Expert Dashboard v7                    ║
║  Evalúa el sistema de señales sobre datos históricos reales         ║
║  de Binance (últimos 365 días de velas de 4h)                      ║
║                                                                      ║
║  USO:                                                                ║
║    python backtesting.py                                             ║
║    python backtesting.py --simbolo BTC --dias 180                   ║
║    python backtesting.py --simbolo XRP --dias 365                   ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import os
import statistics
import sys
import time
from datetime import datetime, timedelta

import httpx

# ── CONFIGURACIÓN ──────────────────────────────────────────────────────
SIMBOLO_DEFAULT = "BTC"
DIAS_DEFAULT    = 365
UMBRAL_COMPRA   = 58    # score mínimo para señal de compra
UMBRAL_VENTA    = 42    # score máximo para señal de venta
HORIZONTE_1D    = 6     # velas de 4h en 24 horas
HORIZONTE_3D    = 18    # velas de 4h en 3 días
HORIZONTE_7D    = 42    # velas de 4h en 7 días

BINANCE_BASE = "https://data-api.binance.vision"
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "")

# ── INDICADORES TÉCNICOS (mismos que main.py) ──────────────────────────

def calcular_rsi(precios: list, periodo: int = 14) -> float:
    if len(precios) < periodo + 1:
        return 50.0
    deltas = [precios[i] - precios[i-1] for i in range(1, len(precios))]
    avg_g = sum(d for d in deltas[:periodo] if d > 0) / periodo
    avg_p = sum(-d for d in deltas[:periodo] if d < 0) / periodo
    for delta in deltas[periodo:]:
        g = delta if delta > 0 else 0
        p = -delta if delta < 0 else 0
        avg_g = (avg_g * (periodo - 1) + g) / periodo
        avg_p = (avg_p * (periodo - 1) + p) / periodo
    if avg_p == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_g / avg_p)), 2)


def calcular_macd_cruce(precios: list) -> dict:
    if len(precios) < 35:
        return {"cruce": "ninguno", "tendencia": "neutral", "histograma": 0}
    k12, k26, k9 = 2/13, 2/27, 2/10
    ema12 = ema26 = precios[0]
    macds = []
    for p in precios:
        ema12 = p * k12 + ema12 * (1 - k12)
        ema26 = p * k26 + ema26 * (1 - k26)
        macds.append(ema12 - ema26)
    signal = macds[0]
    signals = []
    for m in macds:
        signal = m * k9 + signal * (1 - k9)
        signals.append(signal)
    histo_actual = macds[-1] - signals[-1]
    histo_prev   = macds[-2] - signals[-2] if len(macds) >= 2 else 0
    cruce = "ninguno"
    if histo_prev <= 0 and histo_actual > 0: cruce = "alcista"
    if histo_prev >= 0 and histo_actual < 0: cruce = "bajista"
    tend = "alcista" if macds[-1] > 0 and histo_actual > 0 else \
           "bajista" if macds[-1] < 0 and histo_actual < 0 else "cruzando"
    return {"cruce": cruce, "tendencia": tend, "histograma": histo_actual}


def calcular_sma(precios: list, periodo: int) -> float:
    if len(precios) < periodo:
        return precios[-1] if precios else 0
    return sum(precios[-periodo:]) / periodo


def calcular_bollinger_pctb(precios: list, periodo: int = 20) -> float:
    if len(precios) < periodo:
        return 50.0
    seg = precios[-periodo:]
    media = sum(seg) / periodo
    desv = statistics.stdev(seg)
    sup = media + 2 * desv
    inf = media - 2 * desv
    if sup == inf:
        return 50.0
    return max(0, min(100, (precios[-1] - inf) / (sup - inf) * 100))


def calcular_atr(highs, lows, closes, periodo=14) -> float:
    if len(closes) < 2:
        return 0.0
    trs = []
    for i in range(1, min(periodo + 1, len(closes))):
        tr = max(highs[-i] - lows[-i],
                 abs(highs[-i] - closes[-i-1]),
                 abs(lows[-i]  - closes[-i-1]))
        trs.append(tr)
    return sum(trs) / len(trs) if trs else 0.0


# ── SCORING SIMPLIFICADO PARA BACKTESTING ──────────────────────────────

def calcular_score_backtest(closes: list, highs: list, lows: list,
                             vols: list, idx: int) -> dict:
    """
    Calcula el score técnico en un punto histórico concreto.
    Usa solo los datos disponibles hasta ese índice (sin lookahead bias).
    """
    if idx < 50:
        return {"score": 50, "rsi": 50, "macd": "neutral", "bb": 50}

    hist_closes = closes[:idx+1]
    hist_highs  = highs[:idx+1]
    hist_lows   = lows[:idx+1]
    hist_vols   = vols[:idx+1]

    rsi   = calcular_rsi(hist_closes)
    macd  = calcular_macd_cruce(hist_closes)
    bb    = calcular_bollinger_pctb(hist_closes)
    sma20 = calcular_sma(hist_closes, 20)
    sma50 = calcular_sma(hist_closes, 50)
    sma200= calcular_sma(hist_closes, 200)
    precio = hist_closes[-1]
    vol_medio = sum(hist_vols[-20:]) / 20 if len(hist_vols) >= 20 else hist_vols[-1]
    vol_ratio = hist_vols[-1] / vol_medio if vol_medio > 0 else 1.0

    # Scoring técnico simplificado
    rsi_s = 0.92 if rsi < 28 else 0.80 if rsi < 38 else 0.62 if rsi < 48 else \
            0.50 if rsi < 55 else 0.38 if rsi < 65 else 0.20 if rsi < 72 else 0.08

    if macd["cruce"] == "alcista":   macd_s = 0.90
    elif macd["cruce"] == "bajista": macd_s = 0.12
    elif macd["tendencia"] == "alcista": macd_s = 0.72
    elif macd["tendencia"] == "bajista": macd_s = 0.28
    else: macd_s = 0.50

    trend_s = 0.85 if sma50 > sma200 and precio > sma20 else \
              0.65 if sma50 > sma200 else \
              0.35 if sma50 < sma200 and precio < sma20 else 0.20

    bb_s = 0.88 if bb < 15 else 0.72 if bb < 30 else \
           0.55 if bb < 50 else 0.45 if bb < 65 else \
           0.30 if bb < 82 else 0.12

    vol_s = 0.82 if vol_ratio > 1.8 else 0.65 if vol_ratio > 1.2 else \
            0.50 if vol_ratio > 0.8 else 0.30

    score = round((rsi_s * 0.28 + macd_s * 0.30 + trend_s * 0.22 +
                   bb_s * 0.13 + vol_s * 0.07) * 100)

    return {
        "score":     score,
        "rsi":       rsi,
        "macd":      macd["tendencia"],
        "cruce":     macd["cruce"],
        "bb":        round(bb, 1),
        "sma_trend": "alcista" if sma50 > sma200 else "bajista",
        "vol_ratio": round(vol_ratio, 2),
    }


# ── DESCARGA DE DATOS HISTÓRICOS ──────────────────────────────────────

async def descargar_historico(simbolo: str, dias: int) -> dict:
    """Descarga velas de 4h de Binance para los últimos N días."""
    limit = min(dias * 6, 1000)  # 6 velas de 4h por día, máx 1000
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY} if BINANCE_API_KEY else {}

    print(f"Descargando {limit} velas de 4h para {simbolo}USDT...")

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(
            f"{BINANCE_BASE}/api/v3/klines",
            params={"symbol": f"{simbolo}USDT", "interval": "4h", "limit": limit},
            headers=headers
        )
        if r.status_code != 200:
            raise Exception(f"Error Binance: {r.status_code} — {r.text[:200]}")
        data = r.json()

    return {
        "opens":     [float(k[1]) for k in data],
        "highs":     [float(k[2]) for k in data],
        "lows":      [float(k[3]) for k in data],
        "closes":    [float(k[4]) for k in data],
        "volumenes": [float(k[5]) for k in data],
        "timestamps": [int(k[0]) for k in data],
    }


# ── BACKTESTING PRINCIPAL ──────────────────────────────────────────────

def ejecutar_backtest(datos: dict, simbolo: str) -> dict:
    """
    Evalúa las señales del sistema sobre el historial completo.
    Sin lookahead bias: cada señal solo usa datos anteriores al punto.
    Incluye filtro de tendencia: no compra contra tendencia bajista mayor.
    """
    closes = datos["closes"]
    highs  = datos["highs"]
    lows   = datos["lows"]
    vols   = datos["volumenes"]
    ts     = datos["timestamps"]
    n      = len(closes)

    señales_compra = []
    señales_venta  = []
    señales_filtradas = 0  # señales descartadas por tendencia bajista

    print(f"Analizando {n} velas de 4h ({round(n/6)} días)...")

    for i in range(50, n - HORIZONTE_7D - 1):
        indicadores = calcular_score_backtest(closes, highs, lows, vols, i)
        score = indicadores["score"]

        # Calcular tendencia de largo plazo (SMA50 vs SMA200 en el punto i)
        sma50_actual  = sum(closes[max(0,i-49):i+1]) / min(50, i+1)
        sma200_actual = sum(closes[max(0,i-199):i+1]) / min(200, i+1)
        tendencia_lp  = "alcista" if sma50_actual > sma200_actual else "bajista"

        # Umbral diferenciado según tendencia (igual que en main.py)
        umbral_compra = 72 if tendencia_lp == "bajista" else 68

        # Señal de COMPRA
        if score >= 58:  # mínimo para registrar
            precio_entrada = closes[i]
            precio_1d = closes[i + HORIZONTE_1D]
            precio_3d = closes[i + HORIZONTE_3D]
            precio_7d = closes[i + HORIZONTE_7D]

            ret_1d = round((precio_1d - precio_entrada) / precio_entrada * 100, 2)
            ret_3d = round((precio_3d - precio_entrada) / precio_entrada * 100, 2)
            ret_7d = round((precio_7d - precio_entrada) / precio_entrada * 100, 2)

            # Filtrar señales contra tendencia bajista que no alcanzan el umbral elevado
            if tendencia_lp == "bajista" and score < umbral_compra:
                señales_filtradas += 1
                continue  # descartar esta señal

            señales_compra.append({
                "fecha":       datetime.fromtimestamp(ts[i]/1000).strftime("%Y-%m-%d %H:%M"),
                "precio":      round(precio_entrada, 4),
                "score":       score,
                "rsi":         indicadores["rsi"],
                "macd":        indicadores["macd"],
                "cruce":       indicadores["cruce"],
                "bb":          indicadores["bb"],
                "tendencia_lp": tendencia_lp,
                "ret_1d":      ret_1d,
                "ret_3d":      ret_3d,
                "ret_7d":      ret_7d,
                "acierto_1d":  ret_1d > 0,
                "acierto_3d":  ret_3d > 0,
                "acierto_7d":  ret_7d > 0,
            })

        # Señal de VENTA
        elif score <= UMBRAL_VENTA:
            precio_entrada = closes[i]
            precio_1d = closes[i + HORIZONTE_1D]
            precio_3d = closes[i + HORIZONTE_3D]
            precio_7d = closes[i + HORIZONTE_7D]

            ret_1d = round((precio_entrada - precio_1d) / precio_entrada * 100, 2)
            ret_3d = round((precio_entrada - precio_3d) / precio_entrada * 100, 2)
            ret_7d = round((precio_entrada - precio_7d) / precio_entrada * 100, 2)

            señales_venta.append({
                "fecha":       datetime.fromtimestamp(ts[i]/1000).strftime("%Y-%m-%d %H:%M"),
                "precio":      round(precio_entrada, 4),
                "score":       score,
                "rsi":         indicadores["rsi"],
                "macd":        indicadores["macd"],
                "ret_1d":      ret_1d,
                "ret_3d":      ret_3d,
                "ret_7d":      ret_7d,
                "acierto_1d":  ret_1d > 0,
                "acierto_3d":  ret_3d > 0,
                "acierto_7d":  ret_7d > 0,
            })

    print(f"  Señales filtradas por tendencia bajista: {señales_filtradas}")
    return {"compra": señales_compra, "venta": señales_venta}


def analizar_resultados(señales: dict, simbolo: str) -> dict:
    """Calcula estadísticas completas de rendimiento."""
    compras = señales["compra"]
    ventas  = señales["venta"]

    def stats(lista, horizonte):
        key = f"acierto_{horizonte}"
        ret_key = f"ret_{horizonte}"
        if not lista:
            return {"total": 0, "aciertos": 0, "tasa": 0, "ret_medio": 0, "ret_mejor": 0, "ret_peor": 0}
        aciertos = sum(1 for s in lista if s[key])
        rets = [s[ret_key] for s in lista]
        return {
            "total":     len(lista),
            "aciertos":  aciertos,
            "tasa":      round(aciertos / len(lista) * 100, 1),
            "ret_medio": round(sum(rets) / len(rets), 2),
            "ret_mejor": round(max(rets), 2),
            "ret_peor":  round(min(rets), 2),
        }

    # Análisis por condición técnica
    def filtrar_por_rsi(lista, rsi_max):
        return [s for s in lista if s["rsi"] < rsi_max]

    def filtrar_por_cruce(lista):
        return [s for s in lista if s["cruce"] == "alcista"]

    compras_rsi30  = filtrar_por_rsi(compras, 30)
    compras_rsi35  = filtrar_por_rsi(compras, 35)
    compras_cruce  = filtrar_por_cruce(compras)

    return {
        "simbolo": simbolo,
        "periodo": f"{len(señales['compra']) + len(señales['venta'])} señales analizadas",
        "señales_compra": {
            "total": len(compras),
            "por_horizonte": {
                "24h": stats(compras, "1d"),
                "3d":  stats(compras, "3d"),
                "7d":  stats(compras, "7d"),
            },
            "con_rsi_bajo_30": {
                "total": len(compras_rsi30),
                "7d":    stats(compras_rsi30, "7d"),
            },
            "con_rsi_bajo_35": {
                "total": len(compras_rsi35),
                "7d":    stats(compras_rsi35, "7d"),
            },
            "con_cruce_macd": {
                "total": len(compras_cruce),
                "7d":    stats(compras_cruce, "7d"),
            },
        },
        "señales_venta": {
            "total": len(ventas),
            "por_horizonte": {
                "24h": stats(ventas, "1d"),
                "3d":  stats(ventas, "3d"),
                "7d":  stats(ventas, "7d"),
            },
        },
        "mejores_señales_compra": sorted(
            [s for s in compras if s["ret_7d"] > 0],
            key=lambda x: x["ret_7d"], reverse=True
        )[:5],
        "peores_señales_compra": sorted(
            [s for s in compras if s["ret_7d"] < 0],
            key=lambda x: x["ret_7d"]
        )[:5],
    }


def imprimir_informe(resultados: dict):
    """Imprime el informe de backtesting de forma clara y legible."""
    s = resultados["simbolo"]
    c = resultados["señales_compra"]
    v = resultados["señales_venta"]

    print("\n" + "═"*65)
    print(f"  BACKTESTING — {s}USDT")
    print(f"  {resultados['periodo']}")
    print("═"*65)

    print(f"\n📈 SEÑALES DE COMPRA (score ≥ {UMBRAL_COMPRA}%)")
    print(f"   Total señales generadas: {c['total']}")
    if c["total"] > 0:
        for h, label in [("24h","24 horas"),("3d","3 días"),("7d","7 días")]:
            st = c["por_horizonte"][h]
            if st["total"] > 0:
                emoji = "✅" if st["tasa"] >= 55 else "⚠️" if st["tasa"] >= 45 else "❌"
                print(f"\n   Horizonte {label}:")
                print(f"   {emoji} Tasa de acierto: {st['tasa']}% ({st['aciertos']}/{st['total']})")
                print(f"      Retorno medio:  {'+' if st['ret_medio']>=0 else ''}{st['ret_medio']}%")
                print(f"      Mejor señal:    +{st['ret_mejor']}%")
                print(f"      Peor señal:     {st['ret_peor']}%")

        print(f"\n   📊 ANÁLISIS POR CONDICIÓN TÉCNICA (horizonte 7 días):")
        for key, label in [("con_rsi_bajo_30","RSI < 30"),("con_rsi_bajo_35","RSI < 35"),("con_cruce_macd","Cruce MACD alcista")]:
            datos = c[key]
            if datos["total"] > 0:
                st = datos["7d"]
                emoji = "✅" if st["tasa"] >= 60 else "⚠️" if st["tasa"] >= 50 else "❌"
                print(f"   {emoji} {label}: {st['tasa']}% acierto ({datos['total']} señales) | ret. medio: {'+' if st['ret_medio']>=0 else ''}{st['ret_medio']}%")

    print(f"\n📉 SEÑALES DE VENTA (score ≤ {UMBRAL_VENTA}%)")
    print(f"   Total señales generadas: {v['total']}")
    if v["total"] > 0:
        for h, label in [("24h","24 horas"),("3d","3 días"),("7d","7 días")]:
            st = v["por_horizonte"][h]
            if st["total"] > 0:
                emoji = "✅" if st["tasa"] >= 55 else "⚠️" if st["tasa"] >= 45 else "❌"
                print(f"\n   Horizonte {label}:")
                print(f"   {emoji} Tasa de acierto: {st['tasa']}% ({st['aciertos']}/{st['total']})")
                print(f"      Retorno medio:  {'+' if st['ret_medio']>=0 else ''}{st['ret_medio']}%")

    if resultados["mejores_señales_compra"]:
        print(f"\n🏆 MEJORES 5 SEÑALES DE COMPRA (7 días):")
        for s in resultados["mejores_señales_compra"]:
            print(f"   {s['fecha']} | Precio: {s['precio']} | Score: {s['score']}% | RSI: {s['rsi']} | +{s['ret_7d']}%")

    if resultados["peores_señales_compra"]:
        print(f"\n⚠️  PEORES 5 SEÑALES DE COMPRA (7 días):")
        for s in resultados["peores_señales_compra"]:
            print(f"   {s['fecha']} | Precio: {s['precio']} | Score: {s['score']}% | RSI: {s['rsi']} | {s['ret_7d']}%")

    print("\n" + "═"*65)
    print("  CONCLUSIONES PARA CALIBRACIÓN")
    print("═"*65)

    c7 = c["por_horizonte"]["7d"]
    if c7["total"] > 0:
        if c7["tasa"] >= 60:
            print(f"  ✅ Sistema FIABLE en compras a 7 días ({c7['tasa']}% acierto)")
        elif c7["tasa"] >= 50:
            print(f"  ⚠️  Sistema MODERADO en compras ({c7['tasa']}% acierto) — revisar pesos")
        else:
            print(f"  ❌ Sistema POCO FIABLE en compras ({c7['tasa']}% acierto) — recalibrar")

        rsi30 = c["con_rsi_bajo_30"]
        if rsi30["total"] > 0 and rsi30["7d"]["tasa"] > c7["tasa"] + 5:
            print(f"  💡 RSI < 30 mejora la fiabilidad: {rsi30['7d']['tasa']}% vs {c7['tasa']}% general")
            print(f"     → Considera subir el peso del RSI en el scoring")

        cruce = c["con_cruce_macd"]
        if cruce["total"] > 0 and cruce["7d"]["tasa"] > c7["tasa"] + 5:
            print(f"  💡 Cruce MACD alcista mejora la fiabilidad: {cruce['7d']['tasa']}%")
            print(f"     → El MACD está bien calibrado")

    print("═"*65)


async def main():
    # Parsear argumentos simples
    simbolo = SIMBOLO_DEFAULT
    dias    = DIAS_DEFAULT

    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == "--simbolo" and i+1 < len(args):
            simbolo = args[i+1].upper()
        if arg == "--dias" and i+1 < len(args):
            dias = int(args[i+1])

    print(f"\n{'='*65}")
    print(f"  BACKTESTING — {simbolo}USDT — últimos {dias} días")
    print(f"{'='*65}\n")

    try:
        # Descargar datos históricos
        datos = await descargar_historico(simbolo, dias)
        print(f"✅ Descargadas {len(datos['closes'])} velas de 4h")

        # Ejecutar backtesting
        señales = ejecutar_backtest(datos, simbolo)
        print(f"✅ Analizadas {len(señales['compra'])} señales de compra y {len(señales['venta'])} de venta")

        # Calcular estadísticas
        resultados = analizar_resultados(señales, simbolo)

        # Guardar resultados en JSON para análisis posterior
        os.makedirs("data", exist_ok=True)
        archivo = f"data/backtest_{simbolo}_{dias}d.json"
        with open(archivo, "w", encoding="utf-8") as f:
            json.dump(resultados, f, ensure_ascii=False, indent=2)
        print(f"✅ Resultados guardados en {archivo}")

        # Imprimir informe
        imprimir_informe(resultados)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
