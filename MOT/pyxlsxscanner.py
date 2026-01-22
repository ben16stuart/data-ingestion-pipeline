#!/usr/bin/env python3
"""MOT Ingestion Pipeline - Main entrypoint."""

import sys
from src.mot_ingestion.__main__ import main

if __name__ == "__main__":
    sys.exit(main())
