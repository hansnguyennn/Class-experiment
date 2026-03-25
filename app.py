from __future__ import annotations

import csv
import io
import json
import math
import os
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "experiment.db"
DATA_DIR.mkdir(parents=True, exist_ok=True)

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "research2026")
PARTICIPANT_CODES = [
    code.strip().upper()
    for code in os.getenv(
        "PARTICIPANT_CODES",
        ",".join(f"P{i:02d}" for i in range(1, 51)),
    ).split(",")
    if code.strip()
]

app = FastAPI(title="Asset Market Experiment")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS round_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_num INTEGER NOT NULL,
                phase TEXT NOT NULL,
                clear_price REAL NOT NULL,
                fundamental_value REAL NOT NULL,
                deviation REAL NOT NULL,
                volume INTEGER NOT NULL,
                screenshot_message TEXT,
                created_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS player_rounds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_num INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                participant_code TEXT NOT NULL,
                order_type TEXT,
                order_qty INTEGER,
                order_price REAL,
                bought_qty INTEGER NOT NULL,
                sold_qty INTEGER NOT NULL,
                cash_after REAL NOT NULL,
                units_after INTEGER NOT NULL,
                wealth_after REAL NOT NULL,
                round_profit REAL NOT NULL,
                created_at REAL NOT NULL
            )
            """
        )
        conn.commit()


init_db()


@dataclass
class Player:
    name: str
    code: str
    cash: float
    units: int
    ws: WebSocket | None = None
    order: dict[str, Any] | None = None
    last_round_profit: float = 0.0
    bought_last: int = 0
    sold_last: int = 0

    def wealth(self, fv: float) -> float:
        return round(self.cash + self.units * fv, 4)


@dataclass
class ExperimentState:
    total_rounds: int = 10
    screenshot_start_round: int = 6
    dividend: float = 0.50
    redemption: float = 3.00
    interest_rate: float = 0.05
    start_cash: float = 20.0
    start_units: int = 4
    current_round: int = 0
    round_open: bool = False
    is_screenshot_phase: bool = False
    players: dict[str, Player] = field(default_factory=dict)
    code_to_name: dict[str, str] = field(default_factory=dict)
    round_history: list[dict[str, Any]] = field(default_factory=list)
    last_screenshot_msg: str | None = None
    admin_clients: list[WebSocket] = field(default_factory=list)
    last_price: float | None = None

    def current_fv(self, next_round: int | None = None) -> float:
        round_num = self.current_round if next_round is None else next_round
        round_num = max(round_num, 1)
        remaining = max(self.total_rounds - round_num + 1, 0)
        fv = 0.0
        for t in range(1, remaining + 1):
            fv += self.dividend / ((1 + self.interest_rate) ** t)
        if remaining > 0:
            fv += self.redemption / ((1 + self.interest_rate) ** remaining)
        return round(fv, 2)

    def admin_state(self) -> dict[str, Any]:
        return {
            "currentRound": self.current_round,
            "totalRounds": self.total_rounds,
            "screenshotStartRound": self.screenshot_start_round,
            "dividend": self.dividend,
            "redemption": self.redemption,
            "interestRate": self.interest_rate,
            "startCash": self.start_cash,
            "startUnits": self.start_units,
            "roundOpen": self.round_open,
            "isScreenshotPhase": self.is_screenshot_phase,
            "lastScreenshotMsg": self.last_screenshot_msg,
            "players": {
                p.name: {
                    "cash": round(p.cash, 2),
                    "units": p.units,
                    "code": p.code,
                }
                for p in sorted(self.players.values(), key=lambda x: x.name.lower())
            },
            "allOrders": [
                {
                    "player": p.name,
                    "type": p.order["type"],
                    "qty": p.order["qty"],
                    "price": p.order["price"],
                }
                for p in self.players.values()
                if p.order is not None
            ],
            "roundHistory": self.round_history,
        }


STATE = ExperimentState()


async def send_json(ws: WebSocket, payload: dict[str, Any]) -> None:
    await ws.send_text(json.dumps(payload))


async def broadcast_admin_state() -> None:
    payload = {"type": "ADMIN_STATE", "state": STATE.admin_state()}
    alive: list[WebSocket] = []
    for ws in STATE.admin_clients:
        try:
            await send_json(ws, payload)
            alive.append(ws)
        except Exception:
            pass
    STATE.admin_clients = alive


async def broadcast_to_players(payload: dict[str, Any]) -> None:
    for player in list(STATE.players.values()):
        if player.ws is None:
            continue
        try:
            await send_json(player.ws, payload)
        except Exception:
            player.ws = None


async def send_portfolio(player: Player) -> None:
    if player.ws is None:
        return
    await send_json(
        player.ws,
        {
            "type": "PORTFOLIO_UPDATE",
            "cash": round(player.cash, 2),
            "units": player.units,
        },
    )


def phase_label(round_num: int) -> str:
    return "screenshot" if round_num >= STATE.screenshot_start_round else "baseline"


def make_screenshot_message() -> str | None:
    if not STATE.players:
        return None
    ranked = sorted(
        STATE.players.values(),
        key=lambda p: (p.last_round_profit, p.name.lower()),
        reverse=True,
    )
    leader = ranked[0]
    return f"Screenshot: the top trader last round earned ${leader.last_round_profit:.2f} (anonymous)."


@app.get("/", response_class=HTMLResponse)
async def home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "default_codes": ", ".join(PARTICIPANT_CODES[:8]) + ("..." if len(PARTICIPANT_CODES) > 8 else ""),
            "admin_password_hint": ADMIN_PASSWORD,
        },
    )


@app.get("/healthz", response_class=PlainTextResponse)
async def health() -> str:
    return "ok"


@app.get("/export/full_results.csv")
async def export_full_results() -> StreamingResponse:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "round",
        "phase",
        "clear_price",
        "fundamental_value",
        "deviation",
        "volume",
        "player_name",
        "participant_code",
        "order_type",
        "order_qty",
        "order_price",
        "bought_qty",
        "sold_qty",
        "cash_after",
        "units_after",
        "wealth_after",
        "round_profit",
    ])
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT rr.round_num, rr.phase, rr.clear_price, rr.fundamental_value, rr.deviation, rr.volume,
                   pr.player_name, pr.participant_code, pr.order_type, pr.order_qty, pr.order_price,
                   pr.bought_qty, pr.sold_qty, pr.cash_after, pr.units_after, pr.wealth_after, pr.round_profit
            FROM player_rounds pr
            JOIN round_results rr ON rr.round_num = pr.round_num
            ORDER BY rr.round_num, pr.player_name
            """
        ).fetchall()
    for row in rows:
        writer.writerow(row)
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=full_results.csv"})


async def handle_admin_start_round(ws: WebSocket, msg: dict[str, Any]) -> None:
    if STATE.round_open or STATE.current_round >= STATE.total_rounds:
        return
    if STATE.current_round == 0:
        STATE.total_rounds = int(msg.get("totalRounds", STATE.total_rounds))
        STATE.screenshot_start_round = int(msg.get("screenshotStartRound", STATE.screenshot_start_round))
        STATE.dividend = float(msg.get("dividend", STATE.dividend))
        STATE.redemption = float(msg.get("redemption", STATE.redemption))
        STATE.interest_rate = float(msg.get("interestRate", STATE.interest_rate))
        STATE.start_cash = float(msg.get("startCash", STATE.start_cash))
        STATE.start_units = int(msg.get("startUnits", STATE.start_units))
        for player in STATE.players.values():
            player.cash = STATE.start_cash
            player.units = STATE.start_units
    STATE.current_round += 1
    STATE.round_open = True
    STATE.is_screenshot_phase = STATE.current_round >= STATE.screenshot_start_round
    STATE.last_screenshot_msg = make_screenshot_message() if STATE.is_screenshot_phase else None
    fv = STATE.current_fv()
    for player in STATE.players.values():
        player.order = None
        player.bought_last = 0
        player.sold_last = 0
    await broadcast_to_players(
        {
            "type": "ROUND_START",
            "round": STATE.current_round,
            "totalRounds": STATE.total_rounds,
            "fv": fv,
            "isScreenshotPhase": STATE.is_screenshot_phase,
            "screenshotMsg": STATE.last_screenshot_msg,
        }
    )
    await broadcast_admin_state()


async def handle_admin_clear() -> None:
    if not STATE.round_open:
        return
    fv = STATE.current_fv()
    buys: list[dict[str, Any]] = []
    sells: list[dict[str, Any]] = []
    pre_wealth = {p.name: p.wealth(fv) for p in STATE.players.values()}
    order_snapshot: dict[str, dict[str, Any] | None] = {p.name: dict(p.order) if p.order else None for p in STATE.players.values()}

    for player in STATE.players.values():
        if not player.order:
            continue
        order = {**player.order, "player": player.name, "remaining": int(player.order["qty"])}
        if order["type"] == "buy":
            buys.append(order)
        else:
            sells.append(order)

    buys.sort(key=lambda o: (-o["price"], o["submitted_at"]))
    sells.sort(key=lambda o: (o["price"], o["submitted_at"]))

    transaction_prices: list[tuple[int, float]] = []
    volume = 0
    i = j = 0
    while i < len(buys) and j < len(sells):
        bid = buys[i]
        ask = sells[j]
        if bid["price"] < ask["price"]:
            break
        qty = min(bid["remaining"], ask["remaining"])
        txn_price = round((bid["price"] + ask["price"]) / 2, 2)
        transaction_prices.append((qty, txn_price))
        volume += qty
        bid["remaining"] -= qty
        ask["remaining"] -= qty
        if bid["remaining"] == 0:
            i += 1
        if ask["remaining"] == 0:
            j += 1

    if volume > 0:
        clear_price = round(sum(q * p for q, p in transaction_prices) / volume, 2)
    elif STATE.last_price is not None:
        clear_price = STATE.last_price
    else:
        clear_price = fv

    # execute fills at uniform clearing price
    remaining_volume = volume
    for buy in buys:
        if remaining_volume <= 0:
            break
        fill = min(buy["qty"], remaining_volume)
        player = STATE.players[buy["player"]]
        cost = round(fill * clear_price, 2)
        if fill > 0:
            player.cash -= cost
            player.units += fill
            player.bought_last = fill
            remaining_volume -= fill

    remaining_volume = volume
    for sell in sells:
        if remaining_volume <= 0:
            break
        fill = min(sell["qty"], remaining_volume)
        player = STATE.players[sell["player"]]
        revenue = round(fill * clear_price, 2)
        if fill > 0:
            player.cash += revenue
            player.units -= fill
            player.sold_last = fill
            remaining_volume -= fill

    # end-of-round cash growth + dividend
    for player in STATE.players.values():
        player.cash = round(player.cash * (1 + STATE.interest_rate) + player.units * STATE.dividend, 2)

    next_fv = STATE.current_fv(next_round=min(STATE.current_round + 1, STATE.total_rounds))
    top_msg = STATE.last_screenshot_msg if STATE.is_screenshot_phase else None
    phase = phase_label(STATE.current_round)
    deviation = round(clear_price - fv, 2)
    STATE.last_price = clear_price

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO round_results (round_num, phase, clear_price, fundamental_value, deviation, volume, screenshot_message, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (STATE.current_round, phase, clear_price, fv, deviation, volume, top_msg, time.time()),
        )
        for player in STATE.players.values():
            wealth_after = round(player.wealth(next_fv), 2)
            round_profit = round(wealth_after - pre_wealth[player.name], 2)
            player.last_round_profit = round_profit
            original_order = order_snapshot[player.name] or {}
            conn.execute(
                """
                INSERT INTO player_rounds (
                    round_num, player_name, participant_code, order_type, order_qty, order_price,
                    bought_qty, sold_qty, cash_after, units_after, wealth_after, round_profit, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    STATE.current_round,
                    player.name,
                    player.code,
                    original_order.get("type"),
                    original_order.get("qty"),
                    original_order.get("price"),
                    player.bought_last,
                    player.sold_last,
                    round(player.cash, 2),
                    player.units,
                    wealth_after,
                    round_profit,
                    time.time(),
                ),
            )
        conn.commit()

    history_item = {
        "round": STATE.current_round,
        "phase": phase,
        "price": clear_price,
        "fv": fv,
        "dev": deviation,
        "volume": volume,
    }
    STATE.round_history.append(history_item)
    STATE.round_open = False

    for player in STATE.players.values():
        if player.ws is None:
            continue
        await send_json(
            player.ws,
            {
                "type": "ROUND_RESULT",
                "clearPrice": clear_price,
                "bought": player.bought_last,
                "sold": player.sold_last,
                "cash": round(player.cash, 2),
                "units": player.units,
                "ssMsg": top_msg,
            },
        )
        await send_portfolio(player)
        player.order = None

    await broadcast_admin_state()


async def handle_player_join(ws: WebSocket, msg: dict[str, Any]) -> None:
    name = str(msg.get("name", "")).strip()
    code = str(msg.get("code", "")).strip().upper()
    if not name:
        await send_json(ws, {"type": "JOIN_FAIL", "reason": "Please enter your name."})
        return
    if not code:
        await send_json(ws, {"type": "JOIN_FAIL", "reason": "Please enter your participant code."})
        return
    if code not in PARTICIPANT_CODES:
        await send_json(ws, {"type": "JOIN_FAIL", "reason": "That code is not on the participant list."})
        return
    existing_name = STATE.code_to_name.get(code)
    if existing_name and existing_name != name:
        await send_json(ws, {"type": "JOIN_FAIL", "reason": f"Code {code} is already in use."})
        return
    if name in STATE.players and STATE.players[name].code != code:
        await send_json(ws, {"type": "JOIN_FAIL", "reason": "That name is already in use. Add a last initial."})
        return

    if name not in STATE.players:
        player = Player(name=name, code=code, cash=STATE.start_cash, units=STATE.start_units, ws=ws)
        STATE.players[name] = player
        STATE.code_to_name[code] = name
    else:
        player = STATE.players[name]
        player.ws = ws

    await send_json(ws, {"type": "JOIN_OK", "name": name, "cash": round(player.cash, 2), "units": player.units})
    await broadcast_admin_state()
    if STATE.round_open:
        await send_json(
            ws,
            {
                "type": "ROUND_START",
                "round": STATE.current_round,
                "totalRounds": STATE.total_rounds,
                "fv": STATE.current_fv(),
                "isScreenshotPhase": STATE.is_screenshot_phase,
                "screenshotMsg": STATE.last_screenshot_msg,
            },
        )


async def handle_submit_order(ws: WebSocket, msg: dict[str, Any]) -> None:
    player = next((p for p in STATE.players.values() if p.ws is ws), None)
    if player is None:
        return
    if not STATE.round_open:
        await send_json(ws, {"type": "ORDER_FAIL", "reason": "This round is not open right now."})
        return
    order_type = str(msg.get("orderType", "")).lower()
    qty = int(msg.get("qty", 0))
    price = float(msg.get("price", 0))
    if order_type not in {"buy", "sell"}:
        await send_json(ws, {"type": "ORDER_FAIL", "reason": "Order type must be buy or sell."})
        return
    if qty <= 0 or price <= 0:
        await send_json(ws, {"type": "ORDER_FAIL", "reason": "Quantity and price must be positive."})
        return
    if order_type == "buy":
        max_cost = qty * price
        if player.cash < max_cost:
            await send_json(ws, {"type": "ORDER_FAIL", "reason": f"Not enough cash for a {qty}-unit bid at ${price:.2f}."})
            return
    if order_type == "sell" and player.units < qty:
        await send_json(ws, {"type": "ORDER_FAIL", "reason": "You do not have that many units to sell."})
        return
    player.order = {"type": order_type, "qty": qty, "price": round(price, 2), "submitted_at": time.time()}
    await send_json(ws, {"type": "ORDER_ACK", "order": {"type": order_type, "qty": qty, "price": round(price, 2)}})
    await broadcast_admin_state()


async def handle_admin_login(ws: WebSocket, msg: dict[str, Any]) -> None:
    if msg.get("password") == ADMIN_PASSWORD:
        STATE.admin_clients.append(ws)
        await send_json(ws, {"type": "AUTH_OK", "role": "admin"})
        await send_json(ws, {"type": "ADMIN_STATE", "state": STATE.admin_state()})
    else:
        await send_json(ws, {"type": "AUTH_FAIL"})


async def handle_reset() -> None:
    global STATE
    old_players = list(STATE.players.values())
    old_admins = list(STATE.admin_clients)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM round_results")
        conn.execute("DELETE FROM player_rounds")
        conn.commit()
    for player in old_players:
        if player.ws is not None:
            try:
                await send_json(player.ws, {"type": "RESET"})
            except Exception:
                pass
    for ws in old_admins:
        try:
            await send_json(ws, {"type": "RESET"})
        except Exception:
            pass
    STATE = ExperimentState()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            mtype = msg.get("type")
            if mtype == "ADMIN_LOGIN":
                await handle_admin_login(websocket, msg)
            elif mtype == "PLAYER_JOIN":
                await handle_player_join(websocket, msg)
            elif mtype == "SUBMIT_ORDER":
                await handle_submit_order(websocket, msg)
            elif mtype == "ADMIN_START_ROUND":
                await handle_admin_start_round(websocket, msg)
            elif mtype == "ADMIN_CLEAR":
                await handle_admin_clear()
            elif mtype == "ADMIN_TOGGLE_SCREENSHOT":
                STATE.is_screenshot_phase = not STATE.is_screenshot_phase
                STATE.last_screenshot_msg = make_screenshot_message() if STATE.is_screenshot_phase else None
                await broadcast_admin_state()
            elif mtype == "ADMIN_RESET":
                await handle_reset()
    except WebSocketDisconnect:
        if websocket in STATE.admin_clients:
            STATE.admin_clients = [ws for ws in STATE.admin_clients if ws is not websocket]
        for player in STATE.players.values():
            if player.ws is websocket:
                player.ws = None
                break


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
