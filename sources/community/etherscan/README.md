# Etherscan Source

[Etherscan](https://etherscan.io) is the leading blockchain explorer for EVM chains. This community source exposes ERC-20 token transfer events as read-only SQL tables via [Coral](https://withcoral.com), using the Etherscan V2 multichain API.

One API key covers all EVM chains via the `chainid` parameter.

---

## Setup

### 1. Get an Etherscan API Key

1. Create an account at [etherscan.io](https://etherscan.io)
2. Navigate to [My API Keys](https://etherscan.io/myapikey)
3. Generate a free API key (supports Ethereum mainnet; other chains may require a paid plan)

### 2. Set the API Key

```bash
export ETHERSCAN_API_KEY=your-api-key-here
```

### 3. Add the Source to Coral

```bash
coral source add --file sources/community/etherscan/manifest.yaml --interactive
```

### 4. Verify Connection

```bash
coral source test etherscan
```

---

## Tables

### `etherscan.token_transfers`

ERC-20 token transfer events for a wallet address on a given EVM chain.

| Filter | Type | Required | Description |
|--------|------|----------|-------------|
| `chainid` | Int64 | Yes | EVM chain ID (1 = Ethereum, 8453 = Base, 10 = Optimism, 42161 = Arbitrum) |
| `address` | Utf8 | Yes | Wallet address to query transfers for |
| `contract_address` | Utf8 | No | Token contract address to filter by (e.g. USDC: `0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48`) |
| `sort_order` | Utf8 | No | Sort order: `asc` (oldest first, default) or `desc` (newest first) |
| `startblock` | Int64 | No | Only return transfers at or after this block number |
| `endblock` | Int64 | No | Only return transfers at or before this block number |

| Column | Type | Description |
|--------|------|-------------|
| `chainid` | Int64 | EVM chain ID the transfers were queried on (echoed from the filter) |
| `address` | Utf8 | Wallet address the transfers were queried for (echoed from the filter) |
| `sort_order` | Utf8 | Sort order applied to the request (echoed; null if unset) |
| `startblock` | Int64 | Lower block bound applied to the request (echoed; null if unset) |
| `endblock` | Int64 | Upper block bound applied to the request (echoed; null if unset) |
| `hash` | Utf8 | Transaction hash |
| `from_address` | Utf8 | Sender address |
| `to_address` | Utf8 | Recipient address |
| `contract_address` | Utf8 | Token contract address. Also the pushdown filter — `WHERE contract_address = '0x...'` filters API-side |
| `value` | Utf8 | Transfer value in token's smallest unit (cast to DOUBLE and divide by 10^decimals) |
| `token_name` | Utf8 | Token name (e.g. 'USD Coin') |
| `token_symbol` | Utf8 | Token symbol (e.g. 'USDC') |
| `token_decimal` | Int64 | Token decimal places (e.g. 6 for USDC, 18 for WETH) |
| `block_number` | Utf8 | Block number |
| `block_time` | Utf8 | Block timestamp as Unix epoch string |
| `gas_used` | Utf8 | Gas used by the transaction |
| `gas_price` | Utf8 | Gas price in wei |

**Note:** `contract_address` is both the API-side filter and the output column — `WHERE contract_address = '0x...'` is pushed to Etherscan for efficient token-specific queries and also appears in results. The request-echo columns (`chainid`, `address`, `sort_order`, `startblock`, `endblock`) report the filter values applied to the request.

---

## SQL Examples

### Recent USDC transfers for an address

```sql
SELECT hash, from_address, to_address, value, block_time
FROM etherscan.token_transfers
WHERE chainid = 1
  AND address = '0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c'
  AND sort_order = 'desc'
LIMIT 10;
```

### USDC transfer volume (push token filter to API)

```sql
SELECT
  SUM(CAST(value AS DOUBLE) / 1e6) AS total_usdc,
  COUNT(*) AS transfer_count
FROM etherscan.token_transfers
WHERE chainid = 1
  AND address = '0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c'
  AND contract_address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48';
```

### Cross-source JOIN with a grantee registry

`token_transfers` requires an `address` filter, so each query targets one wallet. Join a single grantee's wallet to label its inbound USDC, and iterate (CTE/loop) for the full registry:

```sql
SELECT g.recipient_name, SUM(CAST(tx.value AS DOUBLE) / 1e6) AS usdc_received
FROM grantees.registry g
JOIN etherscan.token_transfers tx ON tx.address = g.wallet
WHERE tx.chainid = 1
  AND tx.address = '0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c'  -- one grantee wallet (required filter)
  AND tx.contract_address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
  AND tx.to_address = tx.address                                 -- inbound transfers only
GROUP BY g.recipient_name
ORDER BY usdc_received DESC;
```

---

## Key API Limitations

### Etherscan V2 API behavior

- **Error responses are HTTP 200 and cannot be auto-detected.** Etherscan signals failures as `{"status":"0","message":"NOTOK","result":"<reason>"}` inside an HTTP 200 body. Coral's HTTP backend only treats a JSON-boolean `ok_path` as success, but Etherscan's `status` is the string `"1"`/`"0"`, so it cannot be modelled — setting `ok_path: [status]` would reject *successful* responses too. As a result, an auth, rate-limit, or bad-parameter failure surfaces as a single row with null transfer fields (the echoed `chainid`/`address` still populate), not a query error. **If a query returns one all-null row or zero rows unexpectedly, verify your API key, `chainid`, and `address`.** (A Coral feature request for string/value `ok_path` matching would let this be modelled properly.)
- **Rate limits** — Free tier: 3 calls/sec, Lite tier: 5 calls/sec. Use conservative page sizes and cache aggressively.
- **Chain support** — Free-tier API keys cover Ethereum mainnet (chainid=1) and Arbitrum One (42161). Base (8453) and OP Mainnet (10) are **not** free-tier chains and require a paid plan. See [Etherscan supported chains](https://docs.etherscan.io/supported-chains). A query that fails only on a specific chain is likely a plan-tier limit rather than source behavior.
- **Pagination** — Uses offset-based pagination (page/offset params). Default page size is 50, max 100.

### Value precision

Token values are returned as strings in the token's smallest unit (e.g. 6 decimals for USDC, 18 for WETH). Always cast and divide in SQL:

```sql
CAST(value AS DOUBLE) / 1e6  -- for USDC (6 decimals)
CAST(value AS DOUBLE) / 1e18 -- for WETH (18 decimals)
```

**Precision caveat:** `DOUBLE` has ~15-17 significant digits. For tokens with 18 decimals and large circulating supply (e.g., SHIB, PEPE), very large raw values may lose precision when cast to DOUBLE. For most standard tokens (USDC, WETH, DAI) this is not an issue. If you need exact arithmetic for high-supply tokens, consider keeping values as Utf8 and converting in your application layer.

---

## Source

- [Etherscan API docs](https://docs.etherscan.io)
- [Etherscan V2 API](https://api.etherscan.io/v2)
- [Get a free API key](https://etherscan.io/myapikey)
