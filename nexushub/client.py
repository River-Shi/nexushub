import asyncio
import msgspec
from nexushub.utils import LiveClock
from picows import ws_connect, WSFrame, WSTransport, WSListener, WSMsgType

clock = LiveClock()

class ClientListener(WSListener):
    def on_ws_connected(self, transport: WSTransport):
        print("Client connected")
    
    def on_ws_disconnected(self, transport: WSTransport):
        print("Client disconnected")

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame):
        if frame.msg_type == WSMsgType.TEXT:
            raw = frame.get_payload_as_ascii_text()
            print(raw)
        elif frame.msg_type == WSMsgType.CLOSE:
            code = frame.get_close_code()
            message = frame.get_close_message()
            print(f"code: {code}, message: {message.decode('utf-8')}")
        
def subscribe(transport: WSTransport):
    payload = msgspec.json.encode(
        {
            "method": "SUBSCRIBE",
            "params": ["btcusdt@bookTicker"],
            "id": clock.timestamp_ms()
        }
    ) 
    transport.send(WSMsgType.TEXT, payload)


async def main(url):
    try:
        transport, client = await ws_connect(ClientListener, url)
        subscribe(transport)
        await transport.wait_disconnected()
    except asyncio.CancelledError:
        pass


if __name__ == '__main__':
    asyncio.run(main("ws://127.0.0.1:9001/linear"))
