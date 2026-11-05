#!/usr/bin/env python3
"""VR数据发布服务。

运行在Python 3.10环境中，通过ZeroMQ发布VR数据。
与Master进程（Python 3.8 + ROS）通过IPC通信。

Usage:
    python3 vr_data_publisher.py [--config CONFIG_PATH]
"""

import argparse
import logging
import math
import signal
import sys
import time
from pathlib import Path
from typing import Optional

import yaml

from vr_message import VRDataMessage, VRControllerData
from xr_sdk_client import XRClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

try:
    import zmq
    from vr_haptic_relay import HapticWorker
except ImportError:
    logger.error("pyzmq is required. Install with: pip install pyzmq")
    sys.exit(1)


class VRDataPublisher:
    """VR数据发布器，采集VR数据并通过ZeroMQ发布。"""

    def __init__(self, config: dict):
        self._config = config
        self._running = False
        self._sdk_started = False
        self._haptic_worker = None
        self._sdk = None

        zmq_cfg = config.get("zmq", {})
        self._ipc_addr = zmq_cfg.get("ipc_addr", "ipc:///tmp/vr_data.ipc")
        self._publish_rate = config.get("publish_rate", 80)
        if self._publish_rate <= 0:
            raise ValueError("publish_rate must be positive")
        self._interval = 1.0 / self._publish_rate

        self._context: Optional[zmq.Context] = None
        self._socket: Optional[zmq.Socket] = None

    def start(self) -> None:
        """初始化VR SDK和ZeroMQ。"""
        if self._sdk_started:
            raise RuntimeError("VR SDK session is already running")
        logger.info("Initializing VR SDK...")
        try:
            self._sdk = XRClient()
            self._sdk.init()
            self._sdk_started = True
            logger.info("VR SDK initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize VR SDK: {e}")
            raise

        logger.info("Initializing ZeroMQ...")
        try:
            self._context = zmq.Context()
            self._socket = self._context.socket(zmq.PUB)
            self._socket.setsockopt(zmq.LINGER, 0)
            self._socket.set_hwm(10)  # 设置高水位标记，防止消息积压
            self._socket.bind(self._ipc_addr)
            logger.info(f"ZeroMQ PUB socket bound to {self._ipc_addr}")
            haptic_cfg = self._config.get("haptic", {})
            if haptic_cfg.get("enabled", True):
                self._haptic_worker = HapticWorker(
                    self._context, self._sdk, haptic_cfg, self._interval
                )
                self._haptic_worker.start()
        except Exception as e:
            logger.error(f"Failed to initialize ZeroMQ: {e}")
            self.stop()
            raise

        self._running = True
        logger.info(f"VR Data Publisher started at {self._publish_rate}Hz")

    def stop(self) -> None:
        """停止发布器并清理资源。"""
        logger.info("Stopping VR Data Publisher...")
        self._running = False

        if self._haptic_worker is not None:
            self._haptic_worker.stop()
            self._haptic_worker = None

        if self._socket:
            self._socket.close(linger=0)
            self._socket = None

        if self._context:
            self._context.term()
            self._context = None

        if self._sdk_started:
            self._sdk_started = False
            try:
                self._sdk.close()
                logger.info("VR SDK closed")
            except Exception as e:
                logger.warning(f"Error closing VR SDK: {e}")

    def _collect_vr_data(self) -> Optional[VRDataMessage]:
        """采集VR数据。"""
        try:
            data = self._sdk.get_tracking_data()
            if data is None:
                return None
            timestamp_ns = int(data["timeStampNs"])
            controllers = data["Controller"]
            left, right = controllers["left"], controllers["right"]
            head = data.get("Head", {"pose": "0,0,0,0,0,0,0"})
            if any(part.get("valid") is False for part in (left, right, head)):
                return None

            def pose(part):
                values = [float(value) for value in part["pose"].split(",")]
                if len(values) != 7 or not all(math.isfinite(v) for v in values):
                    raise ValueError("Expected seven finite pose components")
                return values

            def controller(part):
                values = pose(part)
                return VRControllerData(
                    timestamp_ns=timestamp_ns,
                    position=values[:3],
                    orientation=values[3:],
                    trigger=float(part["trigger"]),
                    grip=float(part["grip"]),
                    axis=[float(part["axisX"]), float(part["axisY"])],
                )

            buttons = {
                "A": bool(right["primaryButton"]),
                "B": bool(right["secondaryButton"]),
                "X": bool(left["primaryButton"]),
                "Y": bool(left["secondaryButton"]),
                "left_menu_button": bool(left["menuButton"]),
                "right_menu_button": bool(right["menuButton"]),
                "left_axis_click": bool(left["axisClick"]),
                "right_axis_click": bool(right["axisClick"]),
            }
            return VRDataMessage(
                timestamp_ns=timestamp_ns,
                headset_pose=pose(head),
                left_controller=controller(left),
                right_controller=controller(right),
                buttons=buttons,
            )

        except Exception as e:
            logger.warning(f"Error collecting VR data: {e}")
            import traceback

            traceback.print_exc()
            return None

    def run(self) -> None:
        """主循环：采集并发布VR数据。"""
        logger.info("Starting main publish loop...")

        while self._running:
            start_time = time.monotonic()

            # 采集数据
            msg = self._collect_vr_data()
            if msg is not None:
                try:
                    self._socket.send(msg.to_json().encode("utf-8"), flags=zmq.NOBLOCK)
                    logger.debug(f"Published VR data at {msg.timestamp_ns}")
                except Exception as e:
                    logger.warning(f"Failed to publish message: {e}")

            # 控制频率
            elapsed = time.monotonic() - start_time
            sleep_time = self._interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    def _signal_handler(self, signum, frame):
        """信号处理器。"""
        logger.info(f"Received signal {signum}, shutting down...")
        self._running = False


def load_config(path: str) -> dict:
    """加载配置文件。"""
    with open(path) as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description="VR Data Publisher")
    parser.add_argument(
        "--config",
        "-c",
        default=str(Path(__file__).resolve().parent / "configs" / "vr_bridge.yaml"),
        help="Path to config file",
    )
    args = parser.parse_args()

    # 加载配置
    config_path = Path(args.config)
    if config_path.exists():
        logger.info(f"Loading config from {config_path}")
        config = load_config(str(config_path))
    else:
        logger.warning(f"Config file not found: {config_path}, using defaults")
        config = {}

    # 创建并启动发布器
    publisher = VRDataPublisher(config)
    signal.signal(signal.SIGINT, publisher._signal_handler)
    signal.signal(signal.SIGTERM, publisher._signal_handler)

    try:
        publisher.start()
        publisher.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Error running publisher: {e}")
    finally:
        publisher.stop()


if __name__ == "__main__":
    main()
