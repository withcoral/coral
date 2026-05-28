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

| Column | Type | Description |
|--------|------|-------------|
| `hash` | Utf8 | Transaction hash |
| `from_address` | Utf8 | Sender address |
| `to_address` | Utf8 | Recipient address |
| `contract_address` | Utf8 | Token contract address |
| `value` | Utf8 | Transfer value in token's smallest unit (cast to DOUBLE and divide by 10^decimals) |
| `token_name` | Utf8 | Token name (e.g. 'USD Coin') |
| `token_symbol` | Utf8 | Token symbol (e.g. 'USDC') |
| `token_decimal` | Int64 | Token decimal places (e.g. 6 for USDC, 18 for WETH) |
| `block_number` | Utf8 | Block number |
| `block_time` | Utf8 | Block timestamp as Unix epoch string |
| `gas_used` | Utf8 | Gas used by the transaction |
| `gas_price` | Utf8 | Gas price in wei |

---

## SQL Examples

### Recent USDC transfers for an address

```sql
SELECT hash, from_address, to_address, value, block_time
FROM etherscan.token_transfers
WHERE chainid = 1
  AND address = '0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c'
ORDER BY block_time DESC
LIMIT 10;
```

### USDC transfer volume (in USDC, not raw units)

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

```sql
SELECT g.recipient_name, SUM(CAST(tx.value AS DOUBLE) / 1e6) AS usdc_received
FROM grantees.registry g
JOIN etherscan.token_transfers tx ON tx.to_address = g.wallet
WHERE tx.chainid = 1
  AND tx.contract_address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
GROUP BY g.recipient_name
ORDER BY usdc_received DESC;
```

---

## Key API Limitations

### Etherscan V2 API behavior

- **Error responses are HTTP 200** — Etherscan signals errors as `{"status":"0","message":"NOTOK","result":"..."}` inside a successful HTTP response. Coral handles this via `ok_path`/`error_path`.
- **Rate limits** — Free tier: 5 calls/sec. Use conservative page sizes and cache aggressively.
- **Chain support** — Free API key supports Ethereum mainnet (chainid=1). Base (8453), Optimism (10), and Arbitrum (42161) may require a paid plan.
- **Pagination** — Uses offset-based pagination (page/offset params). Default page size is 50, max 100.

### Value precision

Token values are returned as strings in the token's smallest unit (e.g. 6 decimals for USDC, 18 for WETH). Always cast and divide in SQL:

```sql
CAST(value AS DOUBLE) / 1e6  -- for USDC (6 decimals)
CAST(value AS DOUBLE) / 1e18 -- for WETH (18 decimals)
```

---

## Source

- [Etherscan API docs](https://docs.etherscan.io)
- [Etherscan V2 API](https://api.etherscan.io/v2)
- [Get a free API key](https://etherscan.io/myapikey)
