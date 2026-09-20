"""``python -m plantkeeper.iot_simulator`` entry point."""

from __future__ import annotations

import sys

from plantkeeper.iot_simulator.cli import main

if __name__ == "__main__":
    sys.exit(main())
