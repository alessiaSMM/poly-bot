import os

GCP_PROJECT_ID   = os.getenv("GCP_PROJECT_ID", "")
KMS_LOCATION     = os.getenv("KMS_LOCATION", "global")
KMS_KEY_RING     = os.getenv("KMS_KEY_RING", "")
KMS_KEY_NAME     = os.getenv("KMS_KEY_NAME", "")
KMS_KEY_VERSION  = os.getenv("KMS_KEY_VERSION", "1")

RPC_URL          = os.getenv("RPC_URL", "")
CHAIN_ID         = int(os.getenv("CHAIN_ID", "137"))  # 137 = Polygon mainnet
