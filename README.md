# near-connect-ledger

Ledger hardware wallet executor for [near-connect](https://github.com/hot-dao/near-connect). A standalone JavaScript file that runs inside the near-connect sandboxed iframe to provide Ledger device integration for NEAR Protocol.

Supports USB (WebUSB with WebHID fallback) and Bluetooth (Web Bluetooth) transports in the browser, as well as native BLE bridges for iOS (WKWebView) and Android (WebView).

## Features

- **Sign in** with Ledger (with optional message signing during sign-in via NEP-413)
- **Add function call access keys** during sign-in (`signInWithFunctionCallKey`)
- **Sign and send transactions** (single and batch)
- **Sign delegate actions** (NEP-366 meta-transactions, returns base64-encoded borsh-serialized `SignedDelegate`)
- **Sign messages** (NEP-413)
- **Automatic account discovery** via [FastNear API](https://api.fastnear.com) — finds accounts associated with the Ledger's public key
- **Automatic NEAR app management** — detects and opens the NEAR app on the Ledger device
- **Custom derivation paths** — defaults to `44'/397'/0'/0'/1'`, configurable per session
- **Account verification** — validates that the Ledger public key is registered on-chain

## Transport support

| Transport | Environment | Connection |
|-----------|-------------|------------|
| WebUSB (preferred) | Chrome, Edge | USB cable |
| WebHID (fallback) | Chrome, Edge | USB cable |
| Web Bluetooth | Chrome, Edge | Bluetooth (Nano X, Nano S Plus, Stax, Flex) |
| Native BLE | iOS (WKWebView), Android (WebView) | Bluetooth via native bridge |

The executor auto-detects available transports and presents the appropriate options. For USB, it tries WebUSB first — which supports silent reconnection to previously authorized devices via `getDevices()` — and falls back to WebHID if `claimInterface` fails (e.g. on macOS where the OS HID driver holds the interface).

## How it works

The executor is loaded by near-connect into a sandboxed iframe (`sandbox="allow-scripts"`). It communicates with the Ledger device directly via browser APIs (WebUSB/WebHID/Web Bluetooth) without any npm dependencies — all protocol handling (APDU framing, BLE MTU negotiation, USB/HID packet framing, borsh serialization, base58 encoding) is implemented from scratch.

### USB protocol details

- Uses the standard Ledger framing (channel `0x0101`, tag `0x05`, 64-byte packets)
- WebUSB: bulk/interrupt transfers via `transferIn`/`transferOut`
- WebHID: HID reports via `sendReport`/`inputreport` events
- Checks APDU status words and surfaces meaningful error messages (locked device, missing app, user declined, etc.)
- WebUSB `getDevices()` enables silent reconnection across iframe lifecycles without a device chooser popup

### BLE protocol details

- Negotiates MTU with the Ledger device (tag `0x08`)
- Implements the Ledger BLE framing protocol (tag `0x05`, chunked by MTU size)
- Handles first-pairing reconnect workaround (disconnect + 4s delay + reconnect)
- Reconnects to previously paired devices across iframe lifecycles via a user-initiated "Connect" button (required because `requestDevice()` needs a user gesture)

## Wallet manifest

To register this executor with near-connect, add the following entry to your wallet manifest:

```json
{
  "id": "ledger",
  "name": "Ledger",
  "icon": "...",
  "description": "Sign transactions with your Ledger hardware wallet via USB or Bluetooth",
  "website": "https://www.ledger.com",
  "version": "1.0.0",
  "executor": "/path/to/ledger-executor.js",
  "type": "sandbox",
  "keepAlive": true,
  "platform": {},
  "features": {
    "signMessage": true,
    "signInWithoutAddKey": true,
    "signInAndSignMessage": true,
    "signInWithFunctionCallKey": true,
    "signAndSendTransaction": true,
    "signAndSendTransactions": true,
    "signDelegateActions": true,
    "mainnet": true,
    "testnet": true
  },
  "permissions": {
    "storage": true,
    "usb": true,
    "hid": true,
    "bluetooth": true
  }
}
```

### `keepAlive`

near-connect normally creates a fresh sandbox iframe for every wallet call and
destroys it afterwards, which drops the device connection each time. Over
Web Bluetooth that means the device chooser on every request (an opaque-origin
sandbox can't use `navigator.bluetooth.getDevices()`). With `"keepAlive": true`
(supported by near-connect builds that include it) the iframe stays alive,
hidden, between calls, so the connection — and the remembered Bluetooth
device — persist until sign-out. If the link drops (the Ledger sleeps or goes
out of range), the executor reconnects to the remembered device with
`gatt.connect()`, without the chooser.

The flag is read from this wallet's entry in near-connect's wallet manifest
(`repository/manifest.json` in near-connect), not from this repository.

A lost Bluetooth link can look connected: after the Ledger locks or sleeps,
writes fail with errors such as "GATT Service no longer exists". The executor
then drops the session, asks the user to unlock the Ledger and open the NEAR
app, and reconnects before the next attempt (Try Again).

### Review timeout

APDU responses are awaited for up to 7 minutes (`DEVICE_REVIEW_TIMEOUT_MS`):
signing responses only arrive after the user has reviewed and approved on the
device, so this is the user's review time.

### Required permissions

The manifest `permissions` ensure that near-connect delegates the necessary browser features to the sandboxed iframe via the `allow` attribute:

- `usb` — WebUSB access for USB transport (preferred)
- `hid` — WebHID access for USB transport (fallback)
- `bluetooth` — Web Bluetooth access for BLE transport
- `storage` — persisting accounts, derivation path, and transport mode

The hosting page must also serve the `Permissions-Policy` HTTP header to enable these features:

```
Permissions-Policy: bluetooth=*, hid=*, usb=*
```

## Native BLE bridge (iOS/Android)

For native apps that embed near-connect in a WebView, the executor communicates with the Ledger device through a postMessage relay:

1. The executor sends `near-connect:ledger-ble:request` messages to the parent frame
2. The parent frame relays these to native code (via `webkit.messageHandlers` on iOS or `@JavascriptInterface` on Android)
3. Native code performs the BLE operation and sends the result back via `near-connect:ledger-ble:response`

Supported native BLE actions: `scan`, `stopScan`, `getDevices`, `connect`, `disconnect`, `exchange`, `isConnected`.

## File structure

```
ledger-executor.js    # Standalone executor (no build step, no dependencies)
```
