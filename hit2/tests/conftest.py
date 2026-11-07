import sys
from pathlib import Path

# El escalador y el servicio usan el reparto y los logs del Hit #1.
sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parents[2] / "hit1")]
