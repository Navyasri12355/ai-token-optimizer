"""
cloud/azure_config.py
======================
Central Azure configuration — all storage paths and service endpoints.
Replace cloud/gcp_config.py entirely.

Set these via environment variables or edit the defaults below.

Usage:
    from cloud.azure_config import cfg
    blob_client = cfg.get_blob_client()
    print(cfg.OUTPUT_PARQUET_PATH)   # wasbs:// path for Spark
"""

import os
from pathlib import Path


class AzureConfig:
    # ── Edit these to match your Azure for Students account ───────────────────
    STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "aitokenoptimizer")
    STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY", "")  # from portal
    CONTAINER = os.getenv("AZURE_CONTAINER", "pipeline-data")
    RESOURCE_GROUP = os.getenv("AZURE_RESOURCE_GROUP", "ai-token-optimizer-rg")
    LOCATION = os.getenv("AZURE_LOCATION", "centralindia")

    # Azure Container Apps
    CONTAINER_APP = os.getenv("AZURE_CONTAINER_APP", "token-optimizer-api")
    CONTAINER_ENV = os.getenv("AZURE_CONTAINER_ENV", "token-optimizer-env")
    ACR_NAME = os.getenv("AZURE_ACR_NAME", "aitokenoptimizerregistry")

    # ELK on Azure Container Instances (deploy with cloud/aci_elk_setup.sh)
    ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
    KIBANA_URL = os.getenv("KIBANA_URL", "http://localhost:5601")

    # Azure Databricks (Spark pipeline)
    DATABRICKS_WORKSPACE_URL = os.getenv("DATABRICKS_WORKSPACE_URL", "")

    # ── Azure Blob paths (wasbs:// for Spark, blob path for SDK) ─────────────
    @property
    def WASBS_ROOT(self):
        """wasbs:// prefix for use in Spark reads/writes."""
        return f"wasbs://{self.CONTAINER}@{self.STORAGE_ACCOUNT}.blob.core.windows.net"

    @property
    def RAW_JSONL_BLOB(self):
        """Blob path (for azure-storage-blob SDK)."""
        return "data/raw.jsonl"

    @property
    def OUTPUT_PARQUET_BLOB(self):
        return "output/processed"

    @property
    def SAMPLE_PARQUET_BLOB(self):
        return "output/sample"

    @property
    def MODELS_BLOB(self):
        return "models"

    @property
    def STATS_BLOB(self):
        return "output/stats.json"

    @property
    def EVAL_BLOB(self):
        return "output/evaluation.json"

    @property
    def COST_REPORT_BLOB(self):
        return "output/cost_report.json"

    # ── Spark wasbs:// paths ──────────────────────────────────────────────────
    @property
    def RAW_JSONL_PATH(self):
        return f"{self.WASBS_ROOT}/data/raw.jsonl"

    @property
    def OUTPUT_PARQUET(self):
        return f"{self.WASBS_ROOT}/output/processed"

    @property
    def SAMPLE_PARQUET(self):
        return f"{self.WASBS_ROOT}/output/sample"

    @property
    def MODELS_DIR(self):
        return f"{self.WASBS_ROOT}/models"

    # ── Azure Blob SDK client helpers ─────────────────────────────────────────
    def get_blob_service_client(self):
        """Returns azure.storage.blob.BlobServiceClient."""
        from azure.storage.blob import BlobServiceClient

        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={self.STORAGE_ACCOUNT};"
            f"AccountKey={self.STORAGE_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
        return BlobServiceClient.from_connection_string(conn_str)

    def get_container_client(self):
        return self.get_blob_service_client().get_container_client(self.CONTAINER)

    def upload_file(self, local_path: str, blob_name: str, overwrite=True):
        """Upload a local file to Azure Blob Storage."""
        cc = self.get_container_client()
        with open(local_path, "rb") as f:
            cc.upload_blob(name=blob_name, data=f, overwrite=overwrite)
        print(f"Uploaded {local_path} → {blob_name}")

    def upload_directory(
        self, local_dir: str, blob_prefix: str, overwrite: bool = True
    ) -> int:
        """Upload a local directory tree under a blob prefix."""
        local_root = Path(local_dir)
        if not local_root.exists():
            raise FileNotFoundError(f"Local directory not found: {local_dir}")

        count = 0
        for path in local_root.rglob("*"):
            if not path.is_file():
                continue

            relative_path = path.relative_to(local_root).as_posix()
            blob_name = f"{blob_prefix.rstrip('/')}/{relative_path}"
            self.upload_file(str(path), blob_name, overwrite=overwrite)
            count += 1

        return count

    def download_file(self, blob_name: str, local_path: str):
        """Download a blob to a local file."""
        import os

        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        cc = self.get_container_client()
        with open(local_path, "wb") as f:
            f.write(cc.download_blob(blob_name).readall())
        print(f"Downloaded {blob_name} → {local_path}")

    def download_prefix(
        self, blob_prefix: str, local_dir: str, overwrite: bool = True
    ) -> int:
        """Download all blobs under a prefix into a local directory tree."""
        cc = self.get_container_client()
        local_root = Path(local_dir)
        local_root.mkdir(parents=True, exist_ok=True)

        prefix = blob_prefix.rstrip("/") + "/"
        count = 0

        for blob in cc.list_blobs(name_starts_with=prefix):
            relative_path = blob.name[len(prefix) :]
            if not relative_path:
                continue

            destination = local_root / relative_path
            destination.parent.mkdir(parents=True, exist_ok=True)

            if destination.exists() and not overwrite:
                continue

            with open(destination, "wb") as f:
                f.write(cc.download_blob(blob.name).readall())
            count += 1

        return count

    def upload_json(self, data: dict, blob_name: str):
        """Upload a dict as a JSON blob."""
        import json

        cc = self.get_container_client()
        cc.upload_blob(
            name=blob_name, data=json.dumps(data, indent=2).encode(), overwrite=True
        )

    def download_json(self, blob_name: str) -> dict:
        import json

        cc = self.get_container_client()
        return json.loads(cc.download_blob(blob_name).readall())

    # ── Spark hadoop config for wasbs:// ─────────────────────────────────────
    def spark_hadoop_conf(self) -> dict:
        """
        Dict of hadoop config keys to pass to SparkSession.
        Enables wasbs:// access from Spark.
        """
        return {
            f"fs.azure.account.key.{self.STORAGE_ACCOUNT}.blob.core.windows.net": self.STORAGE_KEY,
            "fs.azure": "org.apache.hadoop.fs.azure.NativeAzureFileSystem",
        }


# Singleton
cfg = AzureConfig()


if __name__ == "__main__":
    print("Azure Config:")
    print(f"  Storage Account : {cfg.STORAGE_ACCOUNT}")
    print(f"  Container       : {cfg.CONTAINER}")
    print(f"  Location        : {cfg.LOCATION}")
    print(f"  JSONL path      : {cfg.RAW_JSONL_PATH}")
    print(f"  Parquet out     : {cfg.OUTPUT_PARQUET}")
    print(f"  Models          : {cfg.MODELS_DIR}")
    print(f"  ES              : {cfg.ES_HOST}")
