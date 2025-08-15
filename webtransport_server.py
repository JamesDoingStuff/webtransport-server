import argparse
import asyncio
import logging
import random
from typing import Optional, Dict

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.connection import stream_is_unidirectional
from aioquic.quic.events import QuicEvent, ProtocolNegotiated, StreamReset
from aioquic.h3.connection import H3Connection, H3_ALPN
from aioquic.h3.events import (
    WebTransportStreamDataReceived,
    DatagramReceived,
    H3Event,
    HeadersReceived,
)
from aioquic.quic.configuration import QuicConfiguration
from aioquic.asyncio.server import serve

BIND_ADDRESS = "::1"
BIND_PORT = 4433


async def apply_randomness(temp):
    """Return temperature with normal-distributed noise."""
    return random.normalvariate(temp, 0.5)


class Handler:
    def __init__(self, protocol, id, http: H3Connection) -> None:
        self._id = id
        self._http = http
        self.current_temp = 20.0  # Default temperature
        self.stream_id: Optional[int] = None
        self._running = True
        self._periodic_task: Optional[asyncio.Task] = None
        self._protocol: WebTransportProtocol = protocol

    def start_periodic_updates(self, stream_id: int):
        """Start sending periodic temperature updates every 1 second."""
        self.stream_id = stream_id
        loop = asyncio.get_event_loop()
        self._periodic_task = loop.create_task(self._send_loop())

    async def _send_loop(self):
        """Background task to push temperature updates."""
        while self._running and self.stream_id is not None:
            temp_with_noise = await apply_randomness(self.current_temp)
            payload = f"{temp_with_noise:.2f}".encode()
            self._http._quic.send_stream_data(self.stream_id, payload)
            self._protocol.transmit()
            await asyncio.sleep(1)

    def h3_event_received(self, event: H3Event) -> None:
        if isinstance(event, DatagramReceived):
            print("Datagram received\n")

        if isinstance(event, WebTransportStreamDataReceived):
            print("Stream data received:", event.data)
            try:
                # Update current_temp if message is numeric
                new_temp = float(event.data.decode().strip())
                self.current_temp = new_temp
                print(f"Updated base temperature to {self.current_temp}")
            except ValueError:
                print("Non-numeric data received")

            if self.stream_id is None:
                # First time we get data, start periodic updates
                self.start_periodic_updates(event.stream_id)

            if event.stream_ended:
                self.stream_closed(event.stream_id)

    def stream_closed(self, stream_id: int) -> None:
        print("Stream closed by client.")
        self._running = False
        if self._periodic_task:
            self._periodic_task.cancel()


class WebTransportProtocol(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._http: Optional[H3Connection] = None
        self._handler: Optional[Handler] = None

    def quic_event_received(self, event: QuicEvent) -> None:
        if isinstance(event, ProtocolNegotiated):
            self._http = H3Connection(self._quic, enable_webtransport=True)
        elif isinstance(event, StreamReset) and self._handler is not None:
            self._handler.stream_closed(event.stream_id)

        if self._http is not None:
            for h3_event in self._http.handle_event(event):
                self._h3_event_received(h3_event)

    def _h3_event_received(self, event: H3Event) -> None:
        if isinstance(event, HeadersReceived):
            headers = {header: value for header, value in event.headers}
            if (
                headers.get(b":method") == b"CONNECT"
                and headers.get(b":protocol") == b"webtransport"
            ):
                self._handshake_webtransport(event.stream_id, headers)
            else:
                self._send_response(event.stream_id, 400, end_stream=True)

        if self._handler:
            self._handler.h3_event_received(event)

    def _handshake_webtransport(
        self, stream_id: int, request_headers: Dict[bytes, bytes]
    ) -> None:
        authority = request_headers.get(b":authority")
        path = request_headers.get(b":path")
        if authority is None or path is None:
            self._send_response(stream_id, 400, end_stream=True)
            return
        if path == b"/tempcontroller" and self._http:
            assert self._handler is None
            self._handler = Handler(self, stream_id, self._http)
            self._send_response(stream_id, 200)
        else:
            self._send_response(stream_id, 404, end_stream=True)

    def _send_response(
        self, stream_id: int, status_code: int, end_stream=False
    ) -> None:
        headers = [(b":status", str(status_code).encode())]
        if status_code == 200:
            headers.append((b"sec-webtransport-http3-draft", b"draft02"))
        if self._http:
            self._http.send_headers(
                stream_id=stream_id, headers=headers, end_stream=end_stream
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("certificate")
    parser.add_argument("key")
    args = parser.parse_args()

    configuration = QuicConfiguration(
        alpn_protocols=H3_ALPN,
        is_client=False,
        max_datagram_frame_size=65536,
    )
    configuration.load_cert_chain(args.certificate, args.key)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        serve(
            BIND_ADDRESS,
            BIND_PORT,
            configuration=configuration,
            create_protocol=WebTransportProtocol,
        )
    )
    try:
        logging.info(
            "Listening on https://{}:{}".format(BIND_ADDRESS, BIND_PORT)
        )
        print("Listening on https://{}:{}".format(BIND_ADDRESS, BIND_PORT))
        loop.run_forever()
    except KeyboardInterrupt:
        pass
