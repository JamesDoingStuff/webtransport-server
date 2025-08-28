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
from math import isclose


BIND_ADDRESS = '::1'
BIND_PORT = 4433


class Pv:
    def __init__(self, name, initial_value=0) -> None:
        self._name: str = name
        self._initial_value = initial_value
        self._updater_task: asyncio.Task | None = None
        self._current_value: float | None = None
        self._stream_id: int | None = None


class Handler:
    def __init__(self, protocol, id, http: H3Connection) -> None:
        self._id = id
        self._http = http
        self._update_task = None
        self._running = False
        self._protocol: WebTransportProtocol = protocol
        self._pv_directory: dict[str, Pv] = {} # Dictionary with PV names as keys and PV objects as values
    
    async def _update_value(self, event: WebTransportStreamDataReceived):
        received_data = json.loads(event.data) # Everything sent by the client

        pv_name: str = received_data["pv"] # Which PV is the client after?
        target_value = float(received_data["value"]) # TO-DO: This should be optional, or tied to the SET command

        target_pv: Pv = self._pv_directory[pv_name] # TO-DO: Deal with new PVs? Should PVs be created here or by client?

        # temp = float(self._pv_directory[pv_name].get(pv_name, 0)) # This is outdated, from when the directory stored the current value

        current_value = target_pv._current_value if target_pv._current_value else target_pv._initial_value

        RESOLUTION = 0.1

        while not isclose(current_value, target_value, abs_tol=0.5*RESOLUTION):
            current_value += RESOLUTION if current_value < target_value else -RESOLUTION
            payload = str(round(current_value, 3)).encode()
            self._http._quic.send_stream_data(event.stream_id, payload)
            self._protocol.transmit()
            print(f"Sent new value: {current_value}")
            target_pv._current_value = current_value
            await asyncio.sleep(0.1)



    def h3_event_received(self, event: H3Event) -> None:
        self._running = True
        if isinstance(event, DatagramReceived):
            print("Datagram received - ignoring")
            pass
            # if self._update_task:
            #     self._update_task.cancel()
            # print("Datagram received\n")
            # self._current_temp = float(event.data)
            # loop = asyncio.get_event_loop()
            # self._update_task = loop.create_task(self._apply_randomness(event.stream_id))
            # When connection ends, a final message is sent to client - if the stream was unidirectional, this requires opening a new return stream.

        if isinstance(event, WebTransportStreamDataReceived):
            # When connection ends, a final message is sent to client - if the stream was unidirectional, this requires opening a new return stream.
            if event.stream_ended:
               self.stream_closed()
            else:
                received_payload = event.data.decode()
                print("Stream {} data received: {}".format(event.stream_id, received_payload))
                received_data: dict = json.loads(received_payload)
                if "command" in received_data.keys():
                    if received_data["pv"] not in self._pv_directory: # TO-DO: Add key checking
                        print("Unrecognised PV name - creating new PV")
                        self._pv_directory.update({received_data["pv"]: Pv(name=received_data["pv"])})

                    pv_name = received_data["pv"] # Which PV is the client after?
                    target_pv: Pv = self._pv_directory[pv_name] # TO-DO: Deal with new PVs? Should PVs be created here or by client?


                    if received_data["command"] == "set":
                        if target_pv._updater_task:
                            target_pv._updater_task.cancel()
                        print("Set command received - initiating updater")
                        loop = asyncio.get_event_loop()
                        target_pv._updater_task = loop.create_task(self._update_value(event))

    def stream_closed(self) -> None:
        #self._http._quic.send_stream_data()
        if self._update_task:
            self._update_task.cancel()
        print("Stream has been closed.\n")


class MassHandler:
    def __init__(self, protocol, id, http: H3Connection) -> None:
        self._id = id
        self._http = http
        self._update_task = None
        self._running = False
        self._protocol: WebTransportProtocol = protocol

    def h3_event_received(self, event: H3Event) -> None:
        self._running = True
        if isinstance(event, DatagramReceived):
            print("Datagram received - ignoring")
            pass
            

        if isinstance(event, WebTransportStreamDataReceived):
            if event.stream_ended:
               self.stream_closed()
            else:
                received_payload = event.data.decode()
                print("Mass data received on {}: {}".format(event.stream_id, received_payload))


    def stream_closed(self) -> None:
        #self._http._quic.send_stream_data()
        if self._update_task:
            self._update_task.cancel()
        print("Stream has been closed.\n")


class WebTransportProtocol(QuicConnectionProtocol):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._http: Optional[H3Connection] = None
        self._handler: Optional[Handler|MassHandler] = None

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
        elif path == b"/stress" and self._http:
            assert(self._handler is None)
            self._handler = MassHandler(self, stream_id, self._http)
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

