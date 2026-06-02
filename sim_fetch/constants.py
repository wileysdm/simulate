from __future__ import annotations
from web3 import Web3

CTF_EXCHANGE = Web3.to_checksum_address("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")
NEG_RISK_CTF_EXCHANGE = Web3.to_checksum_address("0xC5d563A36AE78145C45a50134d48A1215220f80a")
CONDITIONAL_TOKENS = Web3.to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")

ORDERFILLED_TOPIC0 = Web3.keccak(
    text="OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
).hex()

DECIMALS = 1_000_000
GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"

ANKR_RPC_URL = "https://rpc.ankr.com/polygon/877f9cb8d9d92d13779a501eea9911c0f384badb8b3c4c0bee4808b97bb46979"
INFURA_RPC_URL = "https://polygon-mainnet.infura.io/v3/628b31c5420941f6aff191bab1f329dd"
ALCHEMY_RPC_URL = "https://polygon-mainnet.g.alchemy.com/v2/QUcRSWkvrRWFSM_CB5xTr"
POCKET_RPC_URL = "https://poly.api.pocket.network"
TATUM_RPC_URL = "https://polygon-mainnet.gateway.tatum.io/"
DRPC_RPC_URL = "https://lb.drpc.live/polygon/Ar1psUtX1kkus1VHy4P6m0roHCrFGDYR8ZzRtuZZzRRv"
RPCFAST_RPC_URL = "https://polygon-mainnet.rpcfast.com?api_key=xbhWBI1Wkguk8SNMu1bvvLurPGLXmgwYeC4S6g2H7WdwFigZSmPWVZRxrskEQwIf"
SUBQUERY_RPC_URL = "https://polygon.rpc.subquery.network/public"
NODIES_RPC_URL = "https://polygon-public.nodies.app"
ONFINALITY_RPC_URL = "https://polygon.api.onfinality.io/public"
QUIKNODE_RPC_URL = "https://rpc-mainnet.matic.quiknode.pro"
CHAINSTACK_RPC_URL = "https://polygon-mainnet.core.chainstack.com/feb2d63806742349f5d182028c575a68"
PUBLICNODE_RPC_URL = "https://polygon.publicnode.com"
TENDERLY_RPC_URL = "https://tenderly.rpc.polygon.community"

LIVE_LOGS_RPC_URLS = [
    CHAINSTACK_RPC_URL,
    ALCHEMY_RPC_URL,
]

HISTORY_LOGS_RPC_URLS = [
    DRPC_RPC_URL,
    ANKR_RPC_URL,
    INFURA_RPC_URL,
    QUIKNODE_RPC_URL,
    NODIES_RPC_URL,
    ONFINALITY_RPC_URL,
    POCKET_RPC_URL,
    SUBQUERY_RPC_URL,
    TENDERLY_RPC_URL,
]

TS_RPC_URLS = [
    TATUM_RPC_URL,
    PUBLICNODE_RPC_URL,
    RPCFAST_RPC_URL,
]

LOGS_RPC_URLS = list(dict.fromkeys(LIVE_LOGS_RPC_URLS + HISTORY_LOGS_RPC_URLS))

RPC_SLEEP_OVERRIDES = {
    ANKR_RPC_URL: 0.5,
    INFURA_RPC_URL: 0.5,
    ALCHEMY_RPC_URL: 0.5,
    CHAINSTACK_RPC_URL: 0.5,
    POCKET_RPC_URL: 0.8,
    QUIKNODE_RPC_URL: 0.5,
    DRPC_RPC_URL: 0.5,
    TENDERLY_RPC_URL: 0.7,
    PUBLICNODE_RPC_URL: 0.7,
    NODIES_RPC_URL: 0.7,
    ONFINALITY_RPC_URL: 0.7,
    SUBQUERY_RPC_URL: 0.7,
    RPCFAST_RPC_URL: 0.7,
}

NEW_RPC_SLEEP_OVERRIDES = {
    TATUM_RPC_URL: 2.0,
}

SLEEP_EXTRA = 0.25
ONCHAIN_RESOLVE_WORKERS = 14
ONCHAIN_RESOLVE_TIMEOUT_SEC = 12
ONCHAIN_RESOLVE_RETRIES = 3

ANCHOR_TS = 1772323200
ANCHOR_BN = 83601585
BLOCKS_PER_SEC_EST = 0.50

PREFETCH_RPC_TIMEOUT_SEC = 30
PREFETCH_RPC_SLOW_SECONDS = 30
PREFETCH_RPC_BAN_SECONDS = 30 * 60
PREFETCH_MAX_RETRIES = 5

# If a single RPC call exceeds this wallclock time, fail fast and retry.
RPC_WALLCLOCK_RETRY_SEC = 18.0
LOGS_RPC_CALL_TIMEOUT_SEC = 18
