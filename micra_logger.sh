#!/bin/bash

journalctl -f \
-u micra_control.service \
> output/log/micra_control.log
