# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Fixed

- Replaced incorrect `pip install senzing senzing-core orjson` with correct PYTHONPATH/LD_LIBRARY_PATH setup for senzingsdk-runtime
- Changed primary install package from senzingsdk-poc to senzingsdk-runtime
- Documented that senzing/senzing_core Python modules ship with the SDK, not via pip
- Updated mem_load.py docstring with correct SDK requirements

### Added

- Comprehensive README.md with developer documentation covering prerequisites, configuration, usage, architecture, and Senzing V4 SDK component reference
- Full Python docstrings for all functions and the module in mem_load.py
- Inline code comments documenting each step of the 8-phase execution flow
- CHANGELOG.md
