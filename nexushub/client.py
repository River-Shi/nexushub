import asyncio
import msgspec
from nexushub.constants import SubscriptionRequest
from picows import ws_connect, WSFrame, WSTransport, WSListener, WSMsgType

class ClientListener(WSListener):
    def on_ws_connected(self, transport: WSTransport):
        print("Client connected")

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame):
        raw = frame.get_payload_as_ascii_text()
        print(raw)
        
def subscribe(transport: WSTransport):
    payload = msgspec.json.encode(SubscriptionRequest(symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"], event_type="bookTicker"))
    transport.send(WSMsgType.TEXT, payload)


async def main(url):
    try:
        transport, client = await ws_connect(ClientListener, url)
        subscribe(transport)
        await transport.wait_disconnected()
    except asyncio.CancelledError:
        pass


if __name__ == '__main__':
    asyncio.run(main("ws://127.0.0.1:9001"))
