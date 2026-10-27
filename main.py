import json
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, List

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory="static"), name="static")

# Очередь ожидающих и активные пары
waiting_users: List[WebSocket] = []
active_pairs: Dict[WebSocket, WebSocket] = {}

# Простое определение страны по IP (для продакшена лучше использовать базу GeoIP)
async def get_country(ip: str) -> str:
    if ip in ("127.0.0.1", "::1"):
        return "Local"
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://ip-api.com/json/{ip}?fields=countryCode", timeout=2.0)
            return r.json().get("countryCode", "??")
    except:
        return "??"

@app.get("/")
async def root():
    return {"status": "ok"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_ip = websocket.client.host
    country = await get_country(client_ip)
    
    # Отправляем информацию о себе сразу
    await websocket.send_json({"type": "init", "country": country})

    # Матчинг
    if waiting_users:
        partner = waiting_users.pop(0)
        active_pairs[websocket] = partner
        active_pairs[partner] = websocket
        await websocket.send_json({"type": "matched", "initiator": False})
        await partner.send_json({"type": "matched", "initiator": True})
    else:
        waiting_users.append(websocket)
        await websocket.send_json({"type": "waiting"})

    try:
        while True:
            data = await websocket.receive_json()
            
            # Обработка пинга (эхо-ответ)
            if data.get("type") == "ping":
                await websocket.send_json({"type": "pong", "timestamp": data["timestamp"]})
                continue
            
            # Пересылка сигналов (offer/answer/ice) партнеру
            partner = active_pairs.get(websocket)
            if partner:
                await partner.send_json(data)
                
    except WebSocketDisconnect:
        if websocket in waiting_users:
            waiting_users.remove(websocket)
        partner = active_pairs.pop(websocket, None)
        if partner:
            active_pairs.pop(partner, None)
            await partner.send_json({"type": "partner_disconnected"})