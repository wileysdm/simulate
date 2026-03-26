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

From the parent directory of this repository:

```bash
python -m simulate.simulate_main --max-polls 1
```

Or inside this repository:

```bash
python simulate_main.py --max-polls 1
```

## Notes

- Runtime outputs are written to `runtime/`
- Cache files and runtime artifacts are excluded from git by `.gitignore`
