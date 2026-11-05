"""Bounded haptic relay owned by the publisher's existing XR SDK session."""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, replace
from typing import Any

import zmq

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HapticCommand:
    """An impulse with a sender timestamp, in the same host's wall clock."""

    device_id: str | None
    hand: str
    amplitude: float
    duration_ms: int
    frequency_hz: int
    timestamp_ns: int

    def remaining(self, now_ns: int, max_age_ms: int) -> HapticCommand:
        """Turn delayed/expired starts into stops; never extend a queued pulse."""
        age_ms = max(0, (now_ns - self.timestamp_ns) / 1_000_000)
        remaining = max(0, self.duration_ms - math.ceil(age_ms))
        if age_ms > max_age_ms or remaining == 0 or self.amplitude == 0:
            return replace(self, amplitude=0.0, duration_ms=0)
        return replace(self, duration_ms=remaining)

    def payload(self) -> str:
        """Keep the existing Unity envelope, including its string-valued payload."""
        return json.dumps(
            {
                "functionName": "Haptic",
                "value": json.dumps(
                    {
                        "hand": self.hand,
                        "amplitude": self.amplitude,
                        "duration": self.duration_ms,
                        "frequency": self.frequency_hz,
                    }
                ),
            }
        )


def parse_command(raw: bytes) -> list[HapticCommand]:
    """Validate the RLinf PUSH envelope and expand both hands independently."""
    if len(raw) > 8192:
        raise ValueError("Haptic message exceeds 8192 bytes")
    envelope = json.loads(raw)
    if not isinstance(envelope, dict) or envelope.get("functionName") != "Haptic":
        raise ValueError("Expected a Haptic envelope")
    device_id = envelope.get("device_id")
    timestamp = envelope.get("timestamp_ns")
    if device_id is not None and (
        not isinstance(device_id, str) or not device_id.strip() or len(device_id) > 128
    ):
        raise ValueError("device_id must be a nonempty string when provided")
    if type(timestamp) is not int or timestamp <= 0:
        raise ValueError("timestamp_ns must be a positive integer from time.time_ns()")
    value = envelope.get("value")
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("Haptic value must be an object or JSON string")
    hand = value.get("hand", "right")
    if hand not in ("left", "right", "both"):
        raise ValueError("hand must be left, right or both")
    amplitude = value.get("amplitude", 0.4)
    duration = value.get("duration", 200)
    frequency = value.get("frequency", 150)
    if (
        type(amplitude) not in (int, float)
        or not math.isfinite(amplitude)
        or not 0 <= amplitude <= 1
        or type(duration) is not int
        or not 0 <= duration <= 65535
        or type(frequency) is not int
        or not 50 <= frequency <= 500
    ):
        raise ValueError("Invalid amplitude, duration or frequency")
    return [
        HapticCommand(
            device_id.strip() if device_id is not None else None,
            side,
            float(amplitude),
            duration,
            frequency,
            timestamp,
        )
        for side in (("left", "right") if hand == "both" else (hand,))
    ]


class HapticRelay:
    """Relay commands on one owner thread, sharing the publisher's SDK session.

    The owner creates, uses and closes the ZMQ socket. Each tick reads at most
    64 messages and submits at most two native requests. Pending values are
    coalesced per hand for the sole connected headset. This bounds
    queue work, not the native SDK's execution time; slow calls are measured.
    """

    MAX_RECEIVE = 64
    MAX_SEND = 2

    def __init__(self, context: zmq.Context, sdk: Any, config: dict):
        self._sdk = sdk
        self._max_age_ms = int(config.get("max_command_age_ms", 250))
        if self._max_age_ms <= 0:
            raise ValueError("haptic.max_command_age_ms must be positive")
        self._configured_device_id = config.get("device_id")
        self._device_id: str | None = None
        self._devices: tuple[str, ...] | None = None
        self._not_before_ns = 0
        self._pending: OrderedDict[tuple[str, str], HapticCommand] = OrderedDict()
        self._latest: dict[tuple[str, str], int] = {}
        self._active: dict[tuple[str, str], HapticCommand] = {}
        self.received = self.submitted = self.invalid = self.coalesced = 0
        self.failures = self.slow_calls = self.dropped = 0
        self.max_sdk_ms = 0.0
        self.last_error = ""
        self._last_report = time.monotonic()
        self._last_counts = None
        self._socket = context.socket(zmq.PULL)
        try:
            self._socket.setsockopt(zmq.LINGER, 0)
            self._socket.setsockopt(zmq.RCVHWM, self.MAX_RECEIVE)
            self._socket.setsockopt(zmq.MAXMSGSIZE, 8192)
            address = config.get("cmd_addr", "ipc:///tmp/vr_haptic.ipc")
            self._socket.bind(address)
        except Exception:
            self._socket.close(linger=0)
            raise
        logger.info("Haptic PULL bound to %s (publisher owns the SDK session)", address)

    def service(self, stop_event: threading.Event | None = None) -> None:
        """Do bounded queue work; stop dispatching when shutdown is requested."""
        self._select_device()
        deadline = time.monotonic() + 0.002
        for _ in range(self.MAX_RECEIVE):
            if time.monotonic() >= deadline:
                break
            try:
                raw = self._socket.recv(flags=zmq.NOBLOCK)
            except zmq.Again:
                break
            self.received += 1
            try:
                commands = parse_command(raw)
                if (
                    self._device_id is None
                    or commands[0].timestamp_ns < self._not_before_ns
                ):
                    self.dropped += 1
                    continue
                now_ns = time.time_ns()
                for command in commands:
                    if command.device_id not in (None, self._device_id):
                        raise ValueError("Command is for another headset")
                    command = replace(command, device_id=self._device_id)
                    # IPC normally shares one clock. Reject a far-future timestamp.
                    if command.timestamp_ns > now_ns + 1_000_000_000:
                        raise ValueError("Haptic timestamp is in the future")
                    key = (command.device_id, command.hand)
                    if command.timestamp_ns < self._latest.get(key, 0):
                        continue
                    self._latest[key] = command.timestamp_ns
                    if key in self._pending:
                        self.coalesced += 1
                    self._pending[key] = command
            except (ValueError, TypeError, OverflowError, UnicodeError, RecursionError):
                self.invalid += 1
        # Send budget is independent of the receive budget: floods cannot starve stops.
        for _ in range(self.MAX_SEND):
            if not self._pending or (stop_event is not None and stop_event.is_set()):
                break
            key, command = self._pending.popitem(last=False)
            command = command.remaining(time.time_ns(), self._max_age_ms)
            self._submit(key, command)
        self._report()

    def _select_device(self) -> None:
        """Read the SDK event cache; never open another session or scan the network."""
        try:
            devices = tuple(sorted(self._sdk.get_connected_device_ids()))
        except Exception as exc:
            devices = ()
            self.last_error = str(exc)
        if devices == self._devices:
            return
        if self._devices is not None:
            self._not_before_ns = time.time_ns()
        self._devices = devices
        selected = devices[0] if len(devices) == 1 else None
        if self._configured_device_id and selected != self._configured_device_id:
            selected = None
        self._device_id = selected
        self._pending.clear()
        self._latest.clear()
        # Stops share the normal two-call budget and precede new starts.
        for key, command in self._active.items():
            self._pending[key] = replace(command, amplitude=0.0, duration_ms=0)
        self._active.clear()
        if selected:
            logger.info("Haptic target selected: %s", selected)
        else:
            logger.warning(
                "Haptic target unavailable: connected=%s; need exactly one headset "
                "matching haptic.device_id if configured; dropping commands",
                devices,
            )

    def _submit(self, key: tuple[str, str], command: HapticCommand) -> None:
        """Submit once, without replaying failures or claiming a physical ACK."""
        start = time.monotonic()
        # Track even a failed start: a transport failure need not mean no delivery.
        if command.amplitude > 0:
            self._active[key] = command
        try:
            rc = self._sdk.device_control_json(command.device_id, command.payload())
            if rc not in (None, 0):
                raise RuntimeError(f"device_control_json returned {rc}")
            self.submitted += 1
            if command.amplitude == 0:
                self._active.pop(key, None)
        except Exception as exc:
            self.failures += 1
            self.last_error = str(exc)
        finally:
            elapsed_ms = (time.monotonic() - start) * 1000
            self.max_sdk_ms = max(self.max_sdk_ms, elapsed_ms)
            if elapsed_ms > 8:
                self.slow_calls += 1

    def _report(self) -> None:
        now = time.monotonic()
        if now - self._last_report < 5:
            return
        self._last_report = now
        counts = (
            self.received,
            self.submitted,
            self.invalid,
            self.failures,
            self.dropped,
        )
        if counts == self._last_counts:
            return
        self._last_counts = counts
        logger.info(
            "Haptic relay received=%d submitted=%d coalesced=%d invalid=%d "
            "failures=%d dropped=%d slow_calls=%d max_sdk_ms=%.2f last_error=%s (submission is not an ACK)",
            self.received,
            self.submitted,
            self.coalesced,
            self.invalid,
            self.failures,
            self.dropped,
            self.slow_calls,
            self.max_sdk_ms,
            self.last_error,
        )

    def close(self) -> None:
        """Discard pending starts, stop touched hands, then release the PULL socket."""
        if self._socket is None:
            return
        self._pending.clear()
        for key, command in list(self._active.items()):
            self._submit(key, replace(command, amplitude=0.0, duration_ms=0))
        self._active.clear()
        self._socket.close(linger=0)
        self._socket = None


class HapticWorker:
    """Own the relay on a dedicated thread; never initialize or close the SDK.

    Start after SDK initialization and stop before SDK/context teardown. Stop
    waits for in-flight native calls; the SDK does not expose cancellation.
    """

    def __init__(self, context: zmq.Context, sdk: Any, config: dict, interval: float):
        self._context = context
        self._sdk = sdk
        self._config = config
        self._interval = interval
        self._stop_event = threading.Event()
        self._ready = threading.Event()
        self._error: Exception | None = None
        self._thread = threading.Thread(
            target=self._run, name="pico-haptic", daemon=False
        )

    def start(self) -> None:
        """Wait for the command socket to bind and propagate startup failures."""
        self._thread.start()
        self._ready.wait()
        if self._error is not None:
            raise self._error

    def stop(self) -> None:
        """Stop accepting work and wait until the owner releases its socket."""
        self._stop_event.set()
        if self._thread.ident is not None:
            self._thread.join()

    def _run(self) -> None:
        relay = None
        try:
            relay = HapticRelay(self._context, self._sdk, self._config)
            self._ready.set()
            while not self._stop_event.is_set():
                start = time.monotonic()
                relay.service(self._stop_event)
                # A slow native call does not cause catch-up bursts.
                self._stop_event.wait(
                    max(0.0, self._interval - (time.monotonic() - start))
                )
        except Exception as exc:
            self._error = exc
            logger.exception("Haptic worker stopped; feedback is unavailable")
        finally:
            self._ready.set()
            if relay is not None:
                relay.close()
