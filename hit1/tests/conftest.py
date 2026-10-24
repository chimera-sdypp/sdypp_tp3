import os
import tempfile

# Los módulos configuran el log al importarse: que escriban fuera del repo.
os.environ.setdefault("SOBEL_LOGS", tempfile.mkdtemp(prefix="sobel-logs-"))
