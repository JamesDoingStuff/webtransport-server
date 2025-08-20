from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.connection import stream_is_unidirectional
from aioquic.quic.events import QuicEvent, ProtocolNegotiated, StreamReset
from aioquic.h3.connection import H3Connection, H3_ALPN
from aioquic.h3.events import WebTransportStreamDataReceived, DatagramReceived, H3Event, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.asyncio.server import serve

import argparse
import asyncio
import logging
from typing import Optional, Dict
import random
import numpy as np
import json


BIND_ADDRESS = '::1'
BIND_PORT = 4433

DATASET_1 = np.linspace(0,100,100) 
DATASET_2 = np.linspace(100,200,100)


class Handler:
    def __init__(self, protocol, id, http: H3Connection) -> None:
        self._id = id
        self._http = http
        self._current_temp = 0.0
        self._update_task = None
        self._running = False
        self._protocol: WebTransportProtocol = protocol
        self._pv_directory: dict = {}

    async def _apply_randomness(self, id):
        random.seed()
        while self._running:
            self._current_temp = random.normalvariate(self._current_temp, 0.5)
            payload = str(round(self._current_temp, 3)).encode()
            self._http._quic.send_stream_data(id, payload)
            #self._http.send_datagram(id, payload)
            self._protocol.transmit()
            print(f"Sent new temp: {self._current_temp}")
            await asyncio.sleep(10)


    def h3_event_received(self, event: H3Event) -> None:
        self._running = True
        if isinstance(event, DatagramReceived):
            if self._update_task:
                self._update_task.cancel()
            print("Datagram received\n")
            self._current_temp = float(event.data)
            loop = asyncio.get_event_loop()
            self._update_task = loop.create_task(self._apply_randomness(event.stream_id))
            # When connection ends, a final message is sent to client - if the stream was unidirectional, this requires opening a new return stream.

        if isinstance(event, WebTransportStreamDataReceived):
            # When connection ends, a final message is sent to client - if the stream was unidirectional, this requires opening a new return stream.
            if event.stream_ended:
               self.stream_closed()
            else:
                print("Stream data received: {}".format(event.data))
                json_dict = json.loads(event.data)
                print("Stream ID: ", event.stream_id)
                self._pv_directory.update({f"{json_dict['pv']}": f"{event.stream_id}"})
                print(self._pv_directory)



    def stream_closed(self) -> None:
        #self._http._quic.send_stream_data()
        if self._update_task:
            self._update_task.cancel()
        print("Stream has been closed.\n")


class WebTransportProtocol(QuicConnectionProtocol):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._http: Optional[H3Connection] = None
        self._handler: Optional[Handler] = None

    def quic_event_received(self, event: QuicEvent) -> None:
        if isinstance(event, ProtocolNegotiated):
            self._http = H3Connection(self._quic, enable_webtransport=True)
        elif isinstance(event, StreamReset) and self._handler is not None:
            # Streams in QUIC can be closed in two ways: normal (FIN) and
            # abnormal (resets).  FIN is handled by the handler; the code
            # below handles the resets.
            self._handler.stream_closed()

        if self._http is not None:
            for h3_event in self._http.handle_event(event):
                self._h3_event_received(h3_event)

    def _h3_event_received(self, event: H3Event) -> None:
        if isinstance(event, HeadersReceived):
            headers = {}
            for header, value in event.headers:
                headers[header] = value
            if (headers.get(b":method") == b"CONNECT" and
                    headers.get(b":protocol") == b"webtransport"):
                self._handshake_webtransport(event.stream_id, headers)
            else:
                self._send_response(event.stream_id, 400, end_stream=True)

        if self._handler:
            self._handler.h3_event_received(event)

    def _handshake_webtransport(self,
                                stream_id: int,
                                request_headers: Dict[bytes, bytes]) -> None:
        authority = request_headers.get(b":authority")
        path = request_headers.get(b":path")
        if authority is None or path is None:
            # `:authority` and `:path` must be provided.
            self._send_response(stream_id, 400, end_stream=True)
            return
        if path == b"/tempcontroller" and self._http:
            assert(self._handler is None)
            self._handler = Handler(self, stream_id, self._http)
            self._send_response(stream_id, 200)
        else:
            self._send_response(stream_id, 404, end_stream=True)

    def _send_response(self,
                       stream_id: int,
                       status_code: int,
                       end_stream=False) -> None:
        headers = [(b":status", str(status_code).encode())]
        if status_code == 200:
            headers.append((b"sec-webtransport-http3-draft", b"draft02"))
        
        if self._http:
            self._http.send_headers(
                stream_id=stream_id, headers=headers, end_stream=end_stream)
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('certificate')
    parser.add_argument('key')
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
        ))
    try:
        logging.info(
            "Listening on https://{}:{}".format(BIND_ADDRESS, BIND_PORT))
        print("Listening on https://{}:{}".format(BIND_ADDRESS, BIND_PORT))
        loop.run_forever()
    except KeyboardInterrupt:
        pass

