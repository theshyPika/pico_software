"""One native XR SDK session, using the unmodified official wheel's library.

ABI: XR-Robotics/XRoboToolkit-PC-Service, commit 85bac4d,
RoboticsService/SDK/include/PXREARobotSDK.h. Only the three public C entry points
are used; the Python extension is not initialized. Callback buffers belong to
C++ and must be copied before returning.
"""

from __future__ import annotations

import ctypes
import importlib.util
import json
import os
import threading
from pathlib import Path


class _DeviceState(ctypes.Structure):
    _fields_ = [
        ("device_id", ctypes.c_char * 32),
        ("state_json", ctypes.c_char * 16352),
    ]


_Callback = ctypes.CFUNCTYPE(
    None, ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_void_p
)
_SERVER_DISCONNECT = 1 << 3
_DEVICE_FIND = 1 << 4
_DEVICE_MISSING = 1 << 5
_DEVICE_STATE = 1 << 25


def _load_library():
    spec = importlib.util.find_spec("xrobotoolkit_sdk")
    if spec is None or spec.origin is None:
        raise RuntimeError(
            "Install the official SDK: pip install xrobotoolkit-sdk==1.1.0"
        )
    path = Path(spec.origin)
    if spec.submodule_search_locations:
        name = "PXREARobotSDK.dll" if os.name == "nt" else "libPXREARobotSDK.so"
        path = path.parent / name
    # Legacy wheels are a single extension; CDLL resolves its native dependency's
    # public C symbols too. Modern wheels contain the native library alongside it.
    return ctypes.CDLL(str(path))


class XRClient:
    """Cache device events and the latest frame from one native callback.

    The publisher initializes the session before starting the haptic worker and
    closes it only after that worker has joined. Control calls belong to the
    haptic worker. The callback copies data under a short lock; no SDK call holds
    that lock, and CDLL releases the GIL during C calls.
    """

    def __init__(self):
        self._library = _load_library()
        self._library.PXREAInit.argtypes = [ctypes.c_void_p, _Callback, ctypes.c_uint]
        self._library.PXREAInit.restype = ctypes.c_int
        self._library.PXREADeinit.argtypes = []
        self._library.PXREADeinit.restype = ctypes.c_int
        self._library.PXREADeviceControlJson.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p,
        ]
        self._library.PXREADeviceControlJson.restype = ctypes.c_int
        self._lock = threading.Lock()
        self._running = False
        self._devices: set[str] = set()
        self._frame: tuple[str, bytes] | None = None
        self._callback = _Callback(
            self._on_event
        )  # Keep alive until native close ends.

    def init(self) -> None:
        """Start the single SDK session; reject duplicate initialization."""
        with self._lock:
            if self._running:
                raise RuntimeError("VR SDK session is already running")
            self._devices.clear()
            self._frame = None
            self._running = True
        mask = _SERVER_DISCONNECT | _DEVICE_FIND | _DEVICE_MISSING | _DEVICE_STATE
        rc = self._library.PXREAInit(None, self._callback, mask)
        if rc != 0:
            with self._lock:
                self._running = False
                self._devices.clear()
                self._frame = None
            raise RuntimeError(f"PXREAInit returned {rc}")

    def close(self) -> None:
        """Reject late callbacks and release the native session exactly once."""
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._devices.clear()
            self._frame = None
        self._library.PXREADeinit()

    def _on_event(self, context, event, status, data) -> None:
        with self._lock:
            if not self._running:
                return
            if event == _SERVER_DISCONNECT:
                self._devices.clear()
                self._frame = None
            elif data and event in (_DEVICE_FIND, _DEVICE_MISSING):
                device_id = ctypes.string_at(data).decode("utf-8", errors="replace")
                if event == _DEVICE_FIND and device_id:
                    self._devices.add(device_id)
                else:
                    self._devices.discard(device_id)
                # Any topology change requires a new frame; never revive old input.
                self._frame = None
            elif data and event == _DEVICE_STATE:
                state = ctypes.cast(data, ctypes.POINTER(_DeviceState)).contents
                device_id = state.device_id.decode("utf-8", errors="replace")
                if self._devices == {device_id}:
                    self._frame = (device_id, bytes(state.state_json))

    def get_connected_device_ids(self) -> list[str]:
        """Return IDs from this session, without a query or second subscription."""
        with self._lock:
            return sorted(self._devices)

    def get_tracking_data(self) -> dict | None:
        """Decode one coherent frame; no frame is available for ambiguous devices."""
        with self._lock:
            frame = self._frame
        if frame is None:
            return None
        envelope = json.loads(frame[1])
        value = envelope["value"]
        return json.loads(value) if isinstance(value, str) else value

    def device_control_json(self, device_id: str, payload: str) -> int:
        """Use the existing public C control API; return submission status only."""
        if not self._running:
            raise RuntimeError("VR SDK session is not running")
        return self._library.PXREADeviceControlJson(
            device_id.encode(), payload.encode()
        )
