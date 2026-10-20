from pathlib import Path


def model_source(model_dir: str | Path, bundled_dir: str | Path = "./models") -> str:
    """Identify whether a loaded model came from the bundled model directory."""
    if Path(model_dir).resolve() == Path(bundled_dir).resolve():
        return "bundled"
    return "azure-blob-cache"
