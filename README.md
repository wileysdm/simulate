# simulate

Real-time follower simulation engine for Polymarket-style market data.

## Structure

- `simulate_main.py`: CLI entrypoint
- `sim_core.py`: simulation engine
- `sim_config.py`: runtime and strategy config
- `sim_price.py`: price utilities
- `sim_types.py`: shared types
- `sim_fetch/`: fetch and decode helpers

## Run

Install dependencies into user space:

```bash
python3 -m pip install --user --break-system-packages -r requirements.txt
```

From the parent directory of this repository:

```bash
python -m simulate.simulate_main --max-polls 1
```

Or inside this repository:

```bash
python simulate_main.py --max-polls 1
```

## Notes

- Runtime outputs are written to `/data/deming/simulate/runtime/`
- Raw fills are stored under `raw/YYYY-MM-DD/HH.csv`
- Only completed UTC hours are written to disk; the current open hour stays in memory until it closes
- Cache files and runtime artifacts are excluded from git by `.gitignore`
- `--max-polls N` limits the live polling loop to `N` rounds. `--max-polls 1` is a one-shot validation run that starts the engine, performs one live polling pass, then exits after pending work drains.
