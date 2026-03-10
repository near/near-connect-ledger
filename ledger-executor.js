// Ledger Hardware Wallet Executor for near-connect
//
// This script provides Ledger device integration for NEAR Protocol.
// Supports WebHID (USB), WebUSB, Web Bluetooth, and native BLE bridges.
// Used by both browser and native (iOS/Android) environments.

// ============================================================================
// Browser Transport Detection
// ============================================================================

function isWebHidSupported() {
    try {
        return typeof navigator !== "undefined" && !!navigator.hid && typeof navigator.hid.requestDevice === "function";
    } catch { return false; }
}

function isWebUsbSupported() {
    try {
        return typeof navigator !== "undefined" && !!navigator.usb && typeof navigator.usb.requestDevice === "function";
    } catch { return false; }
}

function isWebBleSupported() {
    try {
        return typeof navigator !== "undefined" && !!navigator.bluetooth && typeof navigator.bluetooth.requestDevice === "function";
    } catch { return false; }
}

// ============================================================================
// Native BLE Bridge Transport
// ============================================================================

// Message types for parent frame relay
const LEDGER_BLE_REQUEST = "near-connect:ledger-ble:request";
const LEDGER_BLE_RESPONSE = "near-connect:ledger-ble:response";

// Pending callback map for async native bridge responses
const _pendingCallbacks = new Map();
let _callbackId = 0;

// Listen for responses from the parent frame (relayed from native bridge)
window.addEventListener("message", (event) => {
    if (!event.data || event.data.type !== LEDGER_BLE_RESPONSE) return;
    const { id, result, error } = event.data;
    const cb = _pendingCallbacks.get(id);
    if (cb) {
        _pendingCallbacks.delete(id);
        if (error) {
            cb.reject(new Error(error));
        } else {
            cb.resolve(result);
        }
    }
});

function nativeBLE(action, params = {}) {
    return new Promise((resolve, reject) => {
        const id = String(++_callbackId);
        _pendingCallbacks.set(id, { resolve, reject });
        try {
            // Post to all ancestor frames so the native bridge relay receives
            // the message regardless of iframe nesting depth.
            const msg = { type: LEDGER_BLE_REQUEST, id, action, params };
            let target = window.parent;
            while (target && target !== window) {
                try { target.postMessage(msg, "*"); } catch {}
                if (target === target.parent) break;
                target = target.parent;
            }
        } catch (e) {
            _pendingCallbacks.delete(id);
            reject(new Error("Native BLE bridge unavailable: " + e.message));
        }

        // Timeout after 60 seconds
        setTimeout(() => {
            if (_pendingCallbacks.has(id)) {
                _pendingCallbacks.delete(id);
                reject(new Error("Ledger BLE operation timed out"));
            }
        }, 60000);
    });
}

// Probe the native BLE bridge by sending a lightweight request and waiting for a response.
// Returns a cached result after the first successful probe.
let _nativeBLEProbeResult = null;
async function isNativeBLEAvailable() {
    if (_nativeBLEProbeResult !== null) return _nativeBLEProbeResult;
    try {
        await Promise.race([
            nativeBLE("isConnected"),
            new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 500)),
        ]);
        _nativeBLEProbeResult = true;
    } catch {
        _nativeBLEProbeResult = false;
    }
    return _nativeBLEProbeResult;
}

// ============================================================================
// USB Transport (WebUSB preferred, WebHID fallback)
// ============================================================================

const LEDGER_VENDOR_ID = 0x2c97;
const USB_PACKET_SIZE = 64;
const USB_CHANNEL = 0x0101;
const USB_TAG = 0x05;

// --- WebUSB ---

let _usbDevice = null;
let _usbEndpointIn = 0;
let _usbEndpointOut = 0;

async function usbOpen(device) {
    await device.open();

    // Select configuration (required before claimInterface)
    if (!device.configuration) {
        await device.selectConfiguration(1);
    }

    // Reset to release any kernel/OS driver claims
    try { await device.reset(); } catch {}

    // Log available interfaces for debugging
    const cfg = device.configuration;
    console.log("[USB] configuration:", cfg.configurationValue, "interfaces:", cfg.interfaces.map(i => ({
        num: i.interfaceNumber,
        alternates: i.alternates.map(a => ({
            class: a.interfaceClass, subclass: a.interfaceSubclass, protocol: a.interfaceProtocol,
            endpoints: a.endpoints.map(e => ({ num: e.endpointNumber, dir: e.direction, type: e.type })),
        })),
    })));

    // Find the Ledger interface: prefer class 0xFF (vendor-specific), then any with interrupt endpoints
    let iface = null;
    for (const intf of cfg.interfaces) {
        for (const alt of intf.alternates) {
            if (alt.interfaceClass === 0xFF) { iface = intf; break; }
        }
        if (iface) break;
    }
    if (!iface) {
        for (const intf of cfg.interfaces) {
            for (const alt of intf.alternates) {
                const hasIn = alt.endpoints.some(e => e.direction === "in");
                const hasOut = alt.endpoints.some(e => e.direction === "out");
                if (hasIn && hasOut) { iface = intf; break; }
            }
            if (iface) break;
        }
    }
    if (!iface) throw new Error("No suitable USB interface found on Ledger device.");

    console.log("[USB] claiming interface", iface.interfaceNumber);
    await device.claimInterface(iface.interfaceNumber);
    const alt = iface.alternates[0];
    _usbEndpointIn = alt.endpoints.find(e => e.direction === "in").endpointNumber;
    _usbEndpointOut = alt.endpoints.find(e => e.direction === "out").endpointNumber;
    _usbDevice = device;
}

async function usbExchange(apdu) {
    if (!_usbDevice || !_usbDevice.opened) throw new Error("USB device not open");

    // Frame the APDU — same framing as HID (channel 0x0101, tag 0x05, 64-byte packets)
    const dataLen = apdu.length;
    let offset = 0;
    let seq = 0;

    while (offset < dataLen || seq === 0) {
        const packet = new Uint8Array(USB_PACKET_SIZE);
        packet[0] = (USB_CHANNEL >> 8) & 0xff;
        packet[1] = USB_CHANNEL & 0xff;
        packet[2] = USB_TAG;
        packet[3] = (seq >> 8) & 0xff;
        packet[4] = seq & 0xff;
        let headerLen = 5;
        if (seq === 0) {
            packet[5] = (dataLen >> 8) & 0xff;
            packet[6] = dataLen & 0xff;
            headerLen = 7;
        }
        const chunkLen = Math.min(dataLen - offset, USB_PACKET_SIZE - headerLen);
        if (chunkLen > 0) {
            packet.set(apdu.subarray(offset, offset + chunkLen), headerLen);
            offset += chunkLen;
        }
        await _usbDevice.transferOut(_usbEndpointOut, packet);
        seq++;
    }

    // Receive response packets
    let responseLen = 0;
    let responseOffset = 0;
    let responseData = null;
    seq = 0;

    while (true) {
        const result = await _usbDevice.transferIn(_usbEndpointIn, USB_PACKET_SIZE);
        const report = new Uint8Array(result.data.buffer);

        let rOffset = 0;
        const rChannel = (report[rOffset] << 8) | report[rOffset + 1]; rOffset += 2;
        const rTag = report[rOffset]; rOffset += 1;
        if (rChannel !== USB_CHANNEL || rTag !== USB_TAG) continue;
        rOffset += 2; // seq

        if (seq === 0) {
            responseLen = (report[rOffset] << 8) | report[rOffset + 1]; rOffset += 2;
            responseData = new Uint8Array(responseLen);
        }

        const chunkLen = Math.min(responseLen - responseOffset, report.length - rOffset);
        responseData.set(report.subarray(rOffset, rOffset + chunkLen), responseOffset);
        responseOffset += chunkLen;
        seq++;

        if (responseOffset >= responseLen) break;
    }

    return responseData;
}

// --- WebHID (fallback) ---

let _hidDevice = null;

async function hidExchange(apdu) {
    if (!_hidDevice || !_hidDevice.opened) throw new Error("HID device not open");

    const dataLen = apdu.length;
    let offset = 0;
    let seq = 0;

    while (offset < dataLen || seq === 0) {
        const packet = new Uint8Array(USB_PACKET_SIZE);
        packet[0] = (USB_CHANNEL >> 8) & 0xff;
        packet[1] = USB_CHANNEL & 0xff;
        packet[2] = USB_TAG;
        packet[3] = (seq >> 8) & 0xff;
        packet[4] = seq & 0xff;
        let headerLen = 5;
        if (seq === 0) {
            packet[5] = (dataLen >> 8) & 0xff;
            packet[6] = dataLen & 0xff;
            headerLen = 7;
        }
        const chunkLen = Math.min(dataLen - offset, USB_PACKET_SIZE - headerLen);
        if (chunkLen > 0) {
            packet.set(apdu.subarray(offset, offset + chunkLen), headerLen);
            offset += chunkLen;
        }
        await _hidDevice.sendReport(0, packet);
        seq++;
    }

    let responseLen = 0;
    let responseOffset = 0;
    let responseData = null;
    seq = 0;

    while (true) {
        const report = await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                _hidDevice.removeEventListener("inputreport", handler);
                reject(new Error("HID response timed out"));
            }, 60000);
            function handler(event) {
                clearTimeout(timeout);
                _hidDevice.removeEventListener("inputreport", handler);
                resolve(new Uint8Array(event.data.buffer));
            }
            _hidDevice.addEventListener("inputreport", handler);
        });

        let rOffset = 0;
        const rChannel = (report[rOffset] << 8) | report[rOffset + 1]; rOffset += 2;
        const rTag = report[rOffset]; rOffset += 1;
        if (rChannel !== USB_CHANNEL || rTag !== USB_TAG) continue;
        rOffset += 2; // seq

        if (seq === 0) {
            responseLen = (report[rOffset] << 8) | report[rOffset + 1]; rOffset += 2;
            responseData = new Uint8Array(responseLen);
        }

        const chunkLen = Math.min(responseLen - responseOffset, report.length - rOffset);
        responseData.set(report.subarray(rOffset, rOffset + chunkLen), responseOffset);
        responseOffset += chunkLen;
        seq++;

        if (responseOffset >= responseLen) break;
    }

    return responseData;
}

// ============================================================================
// Web Bluetooth Transport (BLE)
// ============================================================================

// Ledger BLE service UUIDs for all supported device models
// UUID pattern: 13D63400-2C97-{model}04-{role}-4C6564676572
const LEDGER_BLE_SERVICES = [
    "13d63400-2c97-0004-0000-4c6564676572", // Nano X
    "13d63400-2c97-6004-0000-4c6564676572", // Stax
    "13d63400-2c97-3004-0000-4c6564676572", // Flex
    "13d63400-2c97-8004-0000-4c6564676572", // Gen5 (1)
    "13d63400-2c97-9004-0000-4c6564676572", // Gen5 (2)
];

let _bleDevice = null;
let _bleWriteChar = null;
let _bleNotifyChar = null;
let _bleMtuSize = 20;
let _bleNotifyQueue = [];
let _bleNotifyResolve = null;

function _bleOnNotification(event) {
    const value = new Uint8Array(event.target.value.buffer, event.target.value.byteOffset, event.target.value.byteLength);
    console.log("[BLE] notification received:", Array.from(value.slice(0, 10)), "len:", value.length);
    if (_bleNotifyResolve) {
        const resolve = _bleNotifyResolve;
        _bleNotifyResolve = null;
        resolve(value);
    } else {
        _bleNotifyQueue.push(value);
    }
}

function _bleWaitNotification() {
    if (_bleNotifyQueue.length > 0) return Promise.resolve(_bleNotifyQueue.shift());
    return new Promise(resolve => { _bleNotifyResolve = resolve; });
}

async function _bleWrite(data) {
    if (_bleWriteChar.properties.writeWithoutResponse) {
        await _bleWriteChar.writeValueWithoutResponse(data);
    } else {
        await _bleWriteChar.writeValue(data);
    }
}

async function _bleNegotiateMTU() {
    try {
        console.log("[BLE] negotiating MTU...");
        await _bleWrite(new Uint8Array([0x08, 0, 0, 0, 0]));
        const mtuResponse = await Promise.race([
            _bleWaitNotification(),
            new Promise((_, reject) => setTimeout(() => reject(new Error("MTU negotiation timed out")), 5000)),
        ]);
        if (mtuResponse[0] === 0x08 && mtuResponse.length > 5) {
            _bleMtuSize = mtuResponse[5];
            if (_bleMtuSize < 20) _bleMtuSize = 20;
            console.log("[BLE] MTU negotiated:", _bleMtuSize);
        }
    } catch (e) {
        console.warn("[BLE] MTU negotiation failed, using default 20:", e.message);
        _bleMtuSize = 20;
        // Match @ledgerhq behavior: disconnect and throw on MTU failure
        // This forces a clean reconnect
        throw e;
    }
}

async function bleExchange(apdu) {
    if (!_bleWriteChar || !_bleNotifyChar) throw new Error("BLE device not connected");

    const mtu = _bleMtuSize;
    const dataLen = apdu.length;
    let offset = 0;
    let seq = 0;

    console.log("[BLE] exchange: sending APDU len:", dataLen, "mtu:", mtu);

    while (offset < dataLen || seq === 0) {
        let headerLen = seq === 0 ? 5 : 3;
        const chunkLen = Math.min(dataLen - offset, mtu - headerLen);
        const packet = new Uint8Array(headerLen + chunkLen);
        packet[0] = 0x05;
        packet[1] = (seq >> 8) & 0xff;
        packet[2] = seq & 0xff;
        if (seq === 0) {
            packet[3] = (dataLen >> 8) & 0xff;
            packet[4] = dataLen & 0xff;
        }
        if (chunkLen > 0) {
            packet.set(apdu.subarray(offset, offset + chunkLen), headerLen);
            offset += chunkLen;
        }
        console.log("[BLE] write packet seq:", seq, "len:", packet.length, "data:", Array.from(packet));
        await _bleWrite(packet);
        seq++;
    }
    console.log("[BLE] all packets sent, waiting for response...");

    let responseLen = 0;
    let responseOffset = 0;
    let responseData = null;
    seq = 0;

    while (true) {
        const packet = await Promise.race([
            _bleWaitNotification(),
            new Promise((_, reject) => setTimeout(() => reject(new Error("BLE response timed out")), 30000)),
        ]);

        const tag = packet[0];
        if (tag !== 0x05) {
            console.log("[BLE] skipping non-data packet, tag:", tag);
            continue;
        }

        let rOffset = 3;

        if (seq === 0) {
            responseLen = (packet[rOffset] << 8) | packet[rOffset + 1];
            rOffset += 2;
            responseData = new Uint8Array(responseLen);
            console.log("[BLE] response total length:", responseLen);
        }

        const chunkLen = Math.min(responseLen - responseOffset, packet.length - rOffset);
        responseData.set(packet.subarray(rOffset, rOffset + chunkLen), responseOffset);
        responseOffset += chunkLen;
        seq++;

        if (responseOffset >= responseLen) break;
    }

    console.log("[BLE] exchange complete, response len:", responseData.length);
    return responseData;
}

// ============================================================================
// Transport Abstraction
// ============================================================================

// Native BLE is useful for non-browser environments, that can provide BLE support without WebBLE,
// e.g. Electron, Tauri, native iOS / Android wrappers.
const TRANSPORT_NATIVE_BLE = "NativeBLE";
const TRANSPORT_WEB_USB = "WebUSB";
const TRANSPORT_WEB_HID = "WebHID";
const TRANSPORT_WEB_BLE = "WebBLE";

let _activeTransport = null;
let _activeTransportMode = null;

async function transportExchange(apdu) {
    if (_activeTransportMode === TRANSPORT_WEB_USB) {
        return await usbExchange(apdu);
    }
    if (_activeTransportMode === TRANSPORT_WEB_HID) {
        return await hidExchange(apdu);
    }
    if (_activeTransportMode === TRANSPORT_WEB_BLE) {
        return await bleExchange(apdu);
    }
    if (_activeTransportMode === TRANSPORT_NATIVE_BLE) {
        const response = await nativeBLE("exchange", { command: Array.from(apdu) });
        return new Uint8Array(response);
    }
    throw new Error("No active transport");
}

async function transportConnect(mode) {
    if (mode === TRANSPORT_WEB_USB || mode === TRANSPORT_WEB_HID) {
        // Strategy: try WebUSB first (supports silent reconnect via getDevices),
        // fall back to WebHID if WebUSB fails (e.g. OS HID driver holds the interface on macOS).
        if (isWebUsbSupported()) {
            try {
                // Try silent reconnect to previously authorized device
                let device = null;
                try {
                    const devices = await navigator.usb.getDevices();
                    device = devices.find(d => d.vendorId === LEDGER_VENDOR_ID) || null;
                } catch {}
                if (!device) {
                    device = await navigator.usb.requestDevice({
                        filters: [{ vendorId: LEDGER_VENDOR_ID }],
                    });
                }
                if (device) {
                    await usbOpen(device);
                    _activeTransport = _usbDevice;
                    _activeTransportMode = TRANSPORT_WEB_USB;
                    return;
                }
            } catch (e) {
                // WebUSB failed (likely claimInterface) — fall through to WebHID
                console.log("[USB] WebUSB failed, falling back to WebHID:", e.message);
            }
        }
        // WebHID fallback
        if (isWebHidSupported()) {
            let devices;
            try {
                devices = await navigator.hid.requestDevice({
                    filters: [{ vendorId: LEDGER_VENDOR_ID }],
                });
            } catch (e) {
                if (e.name === "SecurityError") throw new Error("WebHID access is blocked. Please check browser permissions.");
                throw e;
            }
            if (!devices || devices.length === 0) throw new Error("No Ledger device selected.");
            _hidDevice = devices[0];
            if (!_hidDevice.opened) await _hidDevice.open();
            _activeTransport = _hidDevice;
            _activeTransportMode = TRANSPORT_WEB_HID;
            return;
        }
        throw new Error("Neither WebUSB nor WebHID is available in this browser.");
    } else if (mode === TRANSPORT_WEB_BLE) {
        const serviceUuids = LEDGER_BLE_SERVICES;
        let device;
        try {
            device = await navigator.bluetooth.requestDevice({
                filters: serviceUuids.map(uuid => ({ services: [uuid] })),
            });
        } catch (e) {
            if (e.name === "SecurityError") throw new Error("Web Bluetooth access is blocked. Please check browser permissions.");
            throw e;
        }
        if (!device) throw new Error("No Ledger device selected.");

        async function bleOpen(device, needsReconnect) {
            console.log("[BLE] connecting GATT (needsReconnect:", needsReconnect, ")...");

            // Force-disconnect stale connections before connecting
            if (device.gatt.connected) {
                console.log("[BLE] disconnecting stale GATT session...");
                device.gatt.disconnect();
                await new Promise(r => setTimeout(r, 600));
            }

            await device.gatt.connect();
            console.log("[BLE] GATT connected, discovering services...");

            // Get primary services (match @ledgerhq: get all, take first)
            let service = null;
            try {
                const services = await device.gatt.getPrimaryServices();
                service = services[0] || null;
            } catch {
                for (const uuid of serviceUuids) {
                    try { service = await device.gatt.getPrimaryService(uuid); break; } catch {}
                }
            }
            if (!service) throw new Error("Ledger BLE service not found on device.");
            console.log("[BLE] service found:", service.uuid);

            // Find write and notify characteristics
            const chars = await service.getCharacteristics();
            _bleWriteChar = null;
            _bleNotifyChar = null;
            for (const char of chars) {
                const uuid = char.uuid.toLowerCase();
                if (uuid.endsWith("-0002-4c6564676572")) _bleWriteChar = char;
                if (uuid.endsWith("-0001-4c6564676572")) _bleNotifyChar = char;
            }
            if (!_bleWriteChar || !_bleNotifyChar) throw new Error("Ledger BLE characteristics not found.");

            const wp = _bleWriteChar.properties;
            console.log("[BLE] write char:", _bleWriteChar.uuid,
                "write:", wp.write, "writeWithoutResponse:", wp.writeWithoutResponse);

            // Start notifications — drain any stale data
            _bleNotifyQueue = [];
            _bleNotifyResolve = null;
            _bleNotifyChar.addEventListener("characteristicvaluechanged", _bleOnNotification);
            await _bleNotifyChar.startNotifications();
            console.log("[BLE] notifications started");

            // Small delay to let any stale notifications arrive, then drain them
            await new Promise(r => setTimeout(r, 100));
            _bleNotifyQueue = [];
            _bleNotifyResolve = null;

            _bleDevice = device;
            _activeTransport = device;
            _activeTransportMode = TRANSPORT_WEB_BLE;

            // Negotiate MTU (match @ledgerhq: on failure, disconnect and throw)
            const beforeMTU = Date.now();
            try {
                await _bleNegotiateMTU();
            } catch (e) {
                console.log("[BLE] MTU failed, disconnecting");
                device.gatt.disconnect();
                throw e;
            }
            const afterMTU = Date.now();

            // Firmware workaround (match @ledgerhq):
            // On first open, if MTU was slow (>1s, indicating new pairing),
            // disconnect, wait, and reconnect fresh
            if (needsReconnect && (afterMTU - beforeMTU) > 1000) {
                console.log("[BLE] new pairing detected, reconnecting after delay...");
                _bleNotifyChar.removeEventListener("characteristicvaluechanged", _bleOnNotification);
                try { await _bleNotifyChar.stopNotifications(); } catch {}
                device.gatt.disconnect();
                await new Promise(r => setTimeout(r, 4000));
                return await bleOpen(device, false);
            }
        }

        await bleOpen(device, true);

        // Disconnect handler: clean up on unexpected BLE disconnect
        device.addEventListener("gattserverdisconnected", () => {
            console.log("[BLE] device disconnected");
            _bleDevice = null;
            _bleWriteChar = null;
            _bleNotifyChar = null;
            _bleNotifyQueue = [];
            _bleNotifyResolve = null;
            _activeTransport = null;
            _activeTransportMode = null;
        });
    } else if (mode === TRANSPORT_NATIVE_BLE) {
        // Check if native side already has a connected device (e.g. from a previous iframe session)
        let alreadyConnected = false;
        try { alreadyConnected = await nativeBLE("isConnected"); } catch {}
        if (alreadyConnected) {
            _activeTransport = { name: "connected" };
            _activeTransportMode = TRANSPORT_NATIVE_BLE;
            return;
        }
        // Disconnect any stale native connection before scanning
        try { await nativeBLE("disconnect"); } catch {}
        await nativeBLE("scan");
        await new Promise(r => setTimeout(r, 3000));
        await nativeBLE("stopScan");
        const devices = await nativeBLE("getDevices");
        if (!devices || devices.length === 0) {
            throw new Error("No Ledger device found. Make sure Bluetooth is enabled and the device is nearby.");
        }
        await nativeBLE("connect", { deviceName: devices[0].name });
        _activeTransport = devices[0];
        _activeTransportMode = TRANSPORT_NATIVE_BLE;
    } else {
        throw new Error("Unknown transport mode: " + mode);
    }
}

async function transportDisconnect() {
    try {
        if (_activeTransportMode === TRANSPORT_WEB_USB && _usbDevice) {
            if (_usbDevice.opened) await _usbDevice.close();
            _usbDevice = null;
            _usbEndpointIn = 0;
            _usbEndpointOut = 0;
        } else if (_activeTransportMode === TRANSPORT_WEB_HID && _hidDevice) {
            if (_hidDevice.opened) await _hidDevice.close();
            _hidDevice = null;
        } else if (_activeTransportMode === TRANSPORT_WEB_BLE && _bleDevice) {
            if (_bleNotifyChar) {
                _bleNotifyChar.removeEventListener("characteristicvaluechanged", _bleOnNotification);
                try { await _bleNotifyChar.stopNotifications(); } catch {}
            }
            if (_bleDevice.gatt?.connected) _bleDevice.gatt.disconnect();
            _bleDevice = null;
            _bleWriteChar = null;
            _bleNotifyChar = null;
            _bleNotifyQueue = [];
            _bleNotifyResolve = null;
        } else if (_activeTransportMode === TRANSPORT_NATIVE_BLE) {
            await nativeBLE("disconnect");
        }
    } catch {}
    _activeTransport = null;
    _activeTransportMode = null;
}

async function transportIsConnected() {
    if (_activeTransportMode === TRANSPORT_WEB_USB) {
        return _usbDevice != null && _usbDevice.opened;
    }
    if (_activeTransportMode === TRANSPORT_WEB_HID) {
        return _hidDevice != null && _hidDevice.opened;
    }
    if (_activeTransportMode === TRANSPORT_WEB_BLE) {
        return _bleDevice != null && _bleDevice.gatt?.connected;
    }
    if (_activeTransportMode === TRANSPORT_NATIVE_BLE) {
        return await nativeBLE("isConnected");
    }
    return false;
}

// ============================================================================
// BIP32 Path Encoding
// ============================================================================

function bip32PathToBytes(path) {
    const parts = path.split("/");
    const result = new Uint8Array(parts.length * 4);
    for (let i = 0; i < parts.length; i++) {
        const part = parts[i];
        let val = part.endsWith("'")
            ? (Math.abs(parseInt(part.slice(0, -1))) | 0x80000000) >>> 0
            : Math.abs(parseInt(part));
        result[i * 4]     = (val >> 24) & 0xff;
        result[i * 4 + 1] = (val >> 16) & 0xff;
        result[i * 4 + 2] = (val >> 8) & 0xff;
        result[i * 4 + 3] = val & 0xff;
    }
    return result;
}

// ============================================================================
// Base58 / Base Encoding
// ============================================================================

const BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

function base58Encode(bytes) {
    if (bytes.length === 0) return "";
    const digits = [0];
    for (let i = 0; i < bytes.length; i++) {
        let carry = bytes[i];
        for (let j = 0; j < digits.length; j++) {
            carry += digits[j] << 8;
            digits[j] = carry % 58;
            carry = (carry / 58) | 0;
        }
        while (carry > 0) {
            digits.push(carry % 58);
            carry = (carry / 58) | 0;
        }
    }
    let result = "";
    for (let i = 0; i < bytes.length && bytes[i] === 0; i++) result += "1";
    for (let i = digits.length - 1; i >= 0; i--) result += BASE58_ALPHABET[digits[i]];
    return result;
}

function base58Decode(str) {
    const bytes = [];
    for (let i = 0; i < str.length; i++) {
        const idx = BASE58_ALPHABET.indexOf(str[i]);
        if (idx < 0) throw new Error("Invalid base58 character: " + str[i]);
        let carry = idx;
        for (let j = 0; j < bytes.length; j++) {
            carry += bytes[j] * 58;
            bytes[j] = carry & 0xff;
            carry >>= 8;
        }
        while (carry > 0) {
            bytes.push(carry & 0xff);
            carry >>= 8;
        }
    }
    for (let i = 0; i < str.length && str[i] === "1"; i++) bytes.push(0);
    return new Uint8Array(bytes.reverse());
}

// ============================================================================
// NEAR Ledger APDU Client
// ============================================================================

const SIGN_TRANSACTION = 2;
const GET_PUBLIC_KEY = 4;
const GET_VERSION = 6;
const SIGN_MESSAGE = 7;
const SIGN_META_TRANSACTION = 8;

const BOLOS_CLA = 0xb0;
const BOLOS_INS_GET_APP_NAME = 0x01;
const BOLOS_INS_QUIT_APP = 0xa7;
const APP_OPEN_CLA = 0xe0;
const APP_OPEN_INS = 0xd8;

const networkId = "W".charCodeAt(0); // mainnet
const DEFAULT_DERIVATION_PATH = "44'/397'/0'/0'/1'";
const CHUNK_SIZE = 123; // 128 - 5 service bytes

/**
 * Build an APDU command buffer in the format expected by Ledger.
 * transport.send(cla, ins, p1, p2, data) → [cla, ins, p1, p2, len, ...data]
 */
function buildAPDU(cla, ins, p1, p2, data) {
    const apdu = new Uint8Array(5 + (data ? data.length : 0));
    apdu[0] = cla;
    apdu[1] = ins;
    apdu[2] = p1;
    apdu[3] = p2;
    apdu[4] = data ? data.length : 0;
    if (data) apdu.set(data, 5);
    return apdu;
}

/**
 * Exchange an APDU with the Ledger device via the active transport.
 */
async function exchangeAPDU(apdu) {
    return await transportExchange(apdu);
}

/**
 * High-level transport.send equivalent.
 * Checks the status word (last 2 bytes) and throws on errors.
 */
async function ledgerSend(cla, ins, p1, p2, data) {
    const apdu = buildAPDU(cla, ins, p1, p2, data);
    const response = await exchangeAPDU(apdu);
    if (response.length >= 2) {
        const sw = (response[response.length - 2] << 8) | response[response.length - 1];
        if (sw !== 0x9000) {
            throw new Error(`Ledger error 0x${sw.toString(16)}`);
        }
    }
    return response;
}

async function getVersion() {
    const response = await ledgerSend(0x80, GET_VERSION, 0, 0);
    return `${response[0]}.${response[1]}.${response[2]}`;
}

async function getPublicKey(path) {
    // Reset state with getVersion first
    await getVersion();
    path = path || DEFAULT_DERIVATION_PATH;
    const response = await ledgerSend(0x80, GET_PUBLIC_KEY, 0, networkId, bip32PathToBytes(path));
    // Strip last 2 bytes (status word)
    return response.subarray(0, response.length - 2);
}

async function sign(transactionData, path) {
    transactionData = new Uint8Array(transactionData);

    // Detect NEP-413 prefix
    const isNep413 = transactionData.length >= 4 &&
        transactionData[0] === 0x9d && transactionData[1] === 0x01 &&
        transactionData[2] === 0x00 && transactionData[3] === 0x80;
    if (isNep413) transactionData = transactionData.slice(4);

    // Detect NEP-366 prefix
    const isNep366 = transactionData.length >= 4 &&
        transactionData[0] === 0x6e && transactionData[1] === 0x01 &&
        transactionData[2] === 0x00 && transactionData[3] === 0x40;
    if (isNep366) transactionData = transactionData.slice(4);

    // Reset state
    const version = await getVersion();
    console.info("Ledger app version:", version);

    path = path || DEFAULT_DERIVATION_PATH;
    const pathBytes = bip32PathToBytes(path);
    const allData = new Uint8Array(pathBytes.length + transactionData.length);
    allData.set(pathBytes, 0);
    allData.set(transactionData, pathBytes.length);

    let code = SIGN_TRANSACTION;
    if (isNep413) code = SIGN_MESSAGE;
    else if (isNep366) code = SIGN_META_TRANSACTION;

    let lastResponse;
    for (let offset = 0; offset < allData.length; offset += CHUNK_SIZE) {
        const chunk = allData.slice(offset, offset + CHUNK_SIZE);
        const isLastChunk = offset + CHUNK_SIZE >= allData.length;
        const response = await ledgerSend(0x80, code, isLastChunk ? 0x80 : 0, networkId, chunk);
        if (isLastChunk) {
            lastResponse = response.subarray(0, response.length - 2);
        }
    }
    return lastResponse;
}

async function getRunningAppName() {
    const res = await ledgerSend(BOLOS_CLA, BOLOS_INS_GET_APP_NAME, 0, 0);
    const nameLength = res[1];
    const nameBytes = res.subarray(2, 2 + nameLength);
    return new TextDecoder().decode(nameBytes);
}

async function quitOpenApplication() {
    await ledgerSend(BOLOS_CLA, BOLOS_INS_QUIT_APP, 0, 0);
}

async function openNearApplication() {
    const runningApp = await getRunningAppName();
    if (runningApp === "NEAR") return;
    if (runningApp !== "BOLOS") {
        await quitOpenApplication();
        await new Promise(r => setTimeout(r, 1000));
    }
    const nearAppName = new TextEncoder().encode("NEAR");
    try {
        await ledgerSend(APP_OPEN_CLA, APP_OPEN_INS, 0x00, 0x00, nearAppName);
    } catch (error) {
        const msg = error.message || "";
        if (msg.includes("6807")) throw new Error("NEAR application is missing on the Ledger device");
        if (msg.includes("5501")) throw new Error("User declined to open the NEAR app");
        throw error;
    }
}

// ============================================================================
// RPC Helpers
// ============================================================================

async function rpcRequest(network, method, params) {
    const rpcUrls = {
        mainnet: "https://rpc.near.org",
        testnet: "https://rpc.testnet.near.org",
    };
    const rpcUrl = rpcUrls[network] || rpcUrls.mainnet;
    const response = await fetch(rpcUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: "dontcare", method, params }),
    });
    const json = await response.json();
    if (json.error) throw new Error(json.error.message || "RPC request failed");
    if (json.result?.error) {
        const errMsg = typeof json.result.error === "string" ? json.result.error : JSON.stringify(json.result.error);
        throw new Error(errMsg);
    }
    return json.result;
}

async function lookupAccountsByPublicKey(network, publicKey) {
    const baseUrl = network === "testnet"
        ? "https://api.testnet.fastnear.com"
        : "https://api.fastnear.com";
    try {
        const response = await fetch(`${baseUrl}/v0/public_key/${publicKey}`);
        if (!response.ok) return [];
        const json = await response.json();
        return json.account_ids || [];
    } catch {
        return [];
    }
}

// ============================================================================
// Error Messages
// ============================================================================

function getLedgerErrorMessage(error) {
    const msg = error.message || "";
    if (msg.includes("0xb005") || msg.includes("UNKNOWN_ERROR")) return "Please approve opening the NEAR app on your Ledger device.";
    if (msg.includes("0x5515") || msg.includes("Locked device")) return "Your Ledger device is locked. Please unlock it and try again.";
    if (msg.includes("6807") || msg.includes("NEAR application is missing")) return "NEAR application is not installed on your Ledger device. Please install it using Ledger Live.";
    if (msg.includes("5501") || msg.includes("declined")) return "You declined to open the NEAR app.";
    if (msg.includes("No device selected") || msg.includes("No Ledger device found") || msg.includes("No Ledger device selected")) return "No Ledger device was found. Please make sure your Ledger is connected and try again.";
    if (msg.includes("0x6985")) return "You declined the request on the Ledger device.";
    if (msg.includes("0x6e01") || msg.includes("0x6d02")) return "Please unlock your Ledger device and open the NEAR app.";
    if (msg.includes("MTU negotiation timed out") || msg.includes("BLE response timed out")) return "Unable to communicate with the Ledger device over Bluetooth. Please make sure the device is unlocked and nearby, then try again.";
    if (error.name === "NotFoundError" || msg.includes("User cancelled")) return "No Ledger device was selected.";
    if (msg.includes("is blocked") || error.name === "SecurityError") return msg;
    return msg || "An unknown error occurred.";
}

function isGuidanceMessage(message) {
    const text = (message || "").toLowerCase();
    return text.includes("locked") || text.includes("unlock") || text.includes("approve opening") || text.includes("approve the request");
}

// ============================================================================
// Formatting Helpers
// ============================================================================

function formatNearAmount(yocto) {
    const s = String(yocto).padStart(25, "0");
    const whole = s.slice(0, s.length - 24) || "0";
    const frac = s.slice(s.length - 24).replace(/0+$/, "");
    return frac ? `${whole}.${frac}` : whole;
}

// ============================================================================
// UI Helpers
// ============================================================================

function alertBox(message) {
    const guidance = isGuidanceMessage(message);
    const bg = guidance ? "#0a1628" : "#290606";
    const color = guidance ? "#93c5fd" : "#ef4444";
    return `<div style="padding:12px; border-radius:12px; background:${bg};">
        <p style="font-family:-apple-system,sans-serif; font-size:12px; color:${color}; line-height:1.5; margin:0; overflow-wrap:anywhere; word-break:break-word;">${message}</p>
    </div>`;
}

/**
 * Show approval UI with retry/cancel support.
 */
async function showLedgerApprovalUI(title, message, asyncOperation, hideOnSuccess = false) {
    await window.selector.ui.showIframe();
    const root = document.getElementById("root");
    root.style.display = "flex";

    function renderLoadingUI() {
        root.style.display = "flex";
        root.innerHTML = `
        <style>@keyframes ledger-spin { to { transform: rotate(360deg); } }</style>
        <div style="display:flex; flex-direction:column; width:100%; height:100%; background:#000; border-radius:24px; overflow:hidden; text-align:left;">
          <div style="flex:1; padding:24px; display:flex; flex-direction:column; gap:32px;">
            <div style="display:flex; flex-direction:column; gap:12px; padding-top:20px;">
              <span style="font-family:-apple-system,sans-serif; font-weight:600; font-size:24px; color:#fafafa;">${title}</span>
              <p style="font-family:-apple-system,sans-serif; font-size:16px; color:#a3a3a3; line-height:1.5; margin:0;">${message}</p>
            </div>
            <div style="display:flex; align-items:center; justify-content:center; padding:22.5px 0;">
              <div style="width:44px; height:44px; border:3px solid #313131; border-top-color:#fafafa; border-radius:50%; animation:ledger-spin 1s linear infinite;"></div>
            </div>
          </div>
        </div>`;
    }

    function renderErrorUI(error) {
        const errorMessage = getLedgerErrorMessage(error);
        root.style.display = "flex";
        root.innerHTML = `
        <div style="display:flex; flex-direction:column; width:100%; height:100%; background:#000; border-radius:24px; overflow:hidden; text-align:left;">
          <div style="flex:1; padding:24px; display:flex; flex-direction:column; gap:32px; overflow:auto;">
            <div style="display:flex; flex-direction:column; gap:12px; padding-top:20px;">
              <span style="font-family:-apple-system,sans-serif; font-weight:600; font-size:24px; color:#fafafa;">${title}</span>
              <p style="font-family:-apple-system,sans-serif; font-size:16px; color:#a3a3a3; line-height:1.5; margin:0;">${message}</p>
            </div>
            <div style="display:flex; flex-direction:column; gap:16px;">
              ${alertBox(errorMessage)}
              <div style="display:flex; flex-direction:column; gap:8px; padding-top:16px;">
                <button id="approvalCancelBtn" style="width:100%; padding:9.5px 24px; border-radius:8px; border:1px solid #404040; background:rgba(255,255,255,0.05); color:#fafafa; cursor:pointer; font-family:-apple-system,sans-serif; font-size:14px; font-weight:500;">Cancel</button>
                <button id="approvalRetryBtn" style="width:100%; padding:9.5px 24px; border-radius:8px; border:none; background:#f5f5f5; color:#0a0a0a; cursor:pointer; font-family:-apple-system,sans-serif; font-size:14px; font-weight:500;">Try Again</button>
              </div>
            </div>
          </div>
        </div>`;
    }

    function waitForRetryAction() {
        return new Promise((resolve, reject) => {
            const retryBtn = document.getElementById("approvalRetryBtn");
            const cancelBtn = document.getElementById("approvalCancelBtn");
            if (!retryBtn || !cancelBtn) { reject(new Error("UI unavailable")); return; }
            retryBtn.addEventListener("click", () => resolve("retry"), { once: true });
            cancelBtn.addEventListener("click", () => resolve("cancel"), { once: true });
        });
    }

    while (true) {
        renderLoadingUI();
        try {
            const result = await asyncOperation();
            if (hideOnSuccess) {
                root.innerHTML = "";
                root.style.display = "none";
                window.selector.ui.hideIframe();
            }
            return result;
        } catch (error) {
            renderErrorUI(error);
            const action = await waitForRetryAction();
            if (action === "retry") continue;
            root.innerHTML = "";
            root.style.display = "none";
            window.selector.ui.hideIframe();
            throw new Error("User cancelled");
        }
    }
}

// ============================================================================
// Connect Flow UI
// ============================================================================

const STORAGE_KEY_ACCOUNTS = "ledger:accounts";
const STORAGE_KEY_DERIVATION_PATH = "ledger:derivationPath";
const STORAGE_KEY_TRANSPORT_MODE = "ledger:transportMode";

async function promptForLedgerConnect() {
    const storedMode = await window.selector.storage.get(STORAGE_KEY_TRANSPORT_MODE);
    const usbAvailable = isWebUsbSupported() || isWebHidSupported();
    const webBleAvailable = isWebBleSupported();
    const nativeBleAvailable = await isNativeBLEAvailable();
    const bleAvailable = webBleAvailable || nativeBleAvailable;
    const bleTransportMode = webBleAvailable ? TRANSPORT_WEB_BLE : TRANSPORT_NATIVE_BLE;
    const usbLastUsed = storedMode === TRANSPORT_WEB_USB || storedMode === TRANSPORT_WEB_HID;
    const bleLastUsed = storedMode === TRANSPORT_WEB_BLE || storedMode === TRANSPORT_NATIVE_BLE;

    await window.selector.ui.showIframe();
    const root = document.getElementById("root");
    root.style.display = "flex";

    async function directConnect(mode) {
        function renderConnecting(statusMessage) {
            root.style.display = "flex";
            root.innerHTML = `
            <style>@keyframes ledger-connect-spin { to { transform: rotate(360deg); } }</style>
            <div style="display:flex; flex-direction:column; width:100%; height:100%; background:#000; border-radius:24px; overflow:hidden; text-align:left;">
              <div style="flex:1; padding:24px; display:flex; flex-direction:column; gap:32px;">
                <div style="display:flex; flex-direction:column; gap:12px; padding-top:20px;">
                  <span style="font-family:-apple-system,sans-serif; font-weight:600; font-size:24px; color:#fafafa;">Connect Ledger</span>
                  <p style="font-family:-apple-system,sans-serif; font-size:16px; color:#a3a3a3; line-height:1.5; margin:0;">${statusMessage}</p>
                </div>
                <div style="display:flex; align-items:center; justify-content:center; padding:22.5px 0;">
                  <div style="width:44px; height:44px; border:3px solid #313131; border-top-color:#fafafa; border-radius:50%; animation:ledger-connect-spin 1s linear infinite;"></div>
                </div>
              </div>
            </div>`;
        }
        function renderRetry(errorMessage) {
            root.style.display = "flex";
            root.innerHTML = `
            <div style="display:flex; flex-direction:column; width:100%; height:100%; background:#000; border-radius:24px; overflow:hidden; text-align:left;">
              <div style="flex:1; padding:24px; display:flex; flex-direction:column; gap:32px; overflow:auto;">
                <div style="display:flex; flex-direction:column; gap:12px; padding-top:20px;">
                  <div style="display:flex; align-items:center; justify-content:space-between;">
                    <span style="font-family:-apple-system,sans-serif; font-weight:600; font-size:24px; color:#fafafa;">Connect Ledger</span>
                    <button id="cancelBtn" style="background:transparent; border:none; cursor:pointer; padding:4px;">
                      <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M1 1L13 13M13 1L1 13" stroke="#fafafa" stroke-width="1.5" stroke-linecap="round"/></svg>
                    </button>
                  </div>
                  <p style="font-family:-apple-system,sans-serif; font-size:16px; color:#a3a3a3; line-height:1.5; margin:0;">
                    Make sure your Ledger device is ${mode === TRANSPORT_NATIVE_BLE || mode === TRANSPORT_WEB_BLE ? "nearby with Bluetooth enabled" : "connected via USB"} and the <strong style="color:#fafafa;">NEAR app</strong> is installed.
                  </p>
                </div>
                <div style="display:flex; flex-direction:column; gap:16px;">
                  ${alertBox(errorMessage)}
                  <button id="retryBtn" style="width:100%; padding:9.5px 24px; border-radius:8px; border:none; background:#f5f5f5; color:#0a0a0a; cursor:pointer; font-family:-apple-system,sans-serif; font-size:14px; font-weight:500;">Retry</button>
                  <button id="closeBtn" style="width:100%; padding:9.5px 24px; border-radius:8px; border:1px solid #404040; background:rgba(255,255,255,0.05); color:#fafafa; cursor:pointer; font-family:-apple-system,sans-serif; font-size:14px; font-weight:500;">Close</button>
                </div>
              </div>
            </div>`;
        }

        return new Promise(async (resolve, reject) => {
            async function attempt() {
                try {
                    if (!(await transportIsConnected()) || _activeTransportMode !== mode) {
                        if (await transportIsConnected()) await transportDisconnect();
                        renderConnecting("Connecting to your Ledger device…");
                        await transportConnect(mode);
                        await window.selector.storage.set(STORAGE_KEY_TRANSPORT_MODE, mode);
                    }
                    renderConnecting("Please approve opening the NEAR app on your Ledger device.");
                    await openNearApplication();
                    resolve();
                } catch (error) {
                    const errorMsg = getLedgerErrorMessage(error);
                    if (!isGuidanceMessage(errorMsg) && await transportIsConnected()) {
                        await transportDisconnect();
                    }
                    renderRetry(errorMsg);
                    document.getElementById("retryBtn").addEventListener("click", attempt);
                    document.getElementById("closeBtn").addEventListener("click", () => {
                        root.innerHTML = "";
                        root.style.display = "none";
                        window.selector.ui.hideIframe();
                        reject(new Error("User cancelled"));
                    });
                    document.getElementById("cancelBtn").addEventListener("click", () => {
                        root.innerHTML = "";
                        root.style.display = "none";
                        window.selector.ui.hideIframe();
                        reject(new Error("User cancelled"));
                    });
                }
            }
            await attempt();
        });
    }

    // If only one transport is available, skip selection and connect directly
    if (bleAvailable && !usbAvailable) {
        return await directConnect(bleTransportMode);
    }
    if (usbAvailable && !bleAvailable) {
        return await directConnect(TRANSPORT_WEB_USB);
    }

    const transportButtons = [
        {
            id: "usbBtn",
            icon: "🔌",
            label: "USB",
            description: "Wired Ledger connection",
            available: usbAvailable,
            lastUsed: usbLastUsed,
            unsupportedReason: usbAvailable ? null : "USB is not supported in this environment. Try Chrome or Edge for WebUSB/WebHID support.",
        },
        {
            id: "bleBtn",
            icon: "📡",
            label: "Bluetooth",
            description: "Wireless Ledger connection",
            available: bleAvailable,
            lastUsed: bleLastUsed,
            unsupportedReason: bleAvailable ? null : "Bluetooth is not available in this environment. Try Chrome or Edge for WebBLE support.",
        },
    ];

    function renderUI(errorMessage = null) {
        root.style.display = "flex";
        root.innerHTML = `
        <style>
          .ledger-transport-btn { position: relative; }
          .ledger-tooltip {
            display: none; position: absolute; bottom: calc(100% + 8px); left: 50%; transform: translateX(-50%);
            background: #1a1a1a; border: 1px solid #313131; border-radius: 8px;
            padding: 8px 10px; font-family: -apple-system, sans-serif; font-size: 12px; color: #a3a3a3;
            white-space: normal; width: 220px; z-index: 10; pointer-events: none; line-height: 1.5;
          }
          .ledger-transport-btn:hover .ledger-tooltip { display: block; }
          @keyframes ledger-connect-spin { to { transform: rotate(360deg); } }
        </style>
        <div style="display:flex; flex-direction:column; width:100%; height:100%; background:#000; border-radius:24px; overflow:hidden; text-align:left;">
          <div style="flex:1; padding:24px; display:flex; flex-direction:column; gap:32px; overflow:auto;">
            <div style="display:flex; flex-direction:column; gap:12px; padding-top:20px;">
              <div style="display:flex; align-items:center; justify-content:space-between;">
                <span style="font-family:-apple-system,sans-serif; font-weight:600; font-size:24px; color:#fafafa;">Connect Ledger</span>
                <button id="cancelBtn" style="background:transparent; border:none; cursor:pointer; padding:4px;">
                  <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M1 1L13 13M13 1L1 13" stroke="#fafafa" stroke-width="1.5" stroke-linecap="round"/></svg>
                </button>
              </div>
              <p style="font-family:-apple-system,sans-serif; font-size:16px; color:#a3a3a3; line-height:1.5; margin:0;">
                Before continuing, please ensure the <strong style="color:#fafafa;">NEAR app</strong> is installed on your Ledger device. You may install it via Ledger Live.
              </p>
            </div>
            <div style="display:flex; flex-direction:column; gap:16px;">
              ${transportButtons.filter(t => t.available || t.unsupportedReason).map(t => `
              <div class="ledger-transport-btn" style="position:relative;">
                <button id="${t.id}" ${!t.available ? "disabled" : ""} style="width:100%; padding:12px; border-radius:12px; border:1px solid #313131; background:#1a1a1a; display:flex; align-items:center; gap:12px; cursor:${t.available ? "pointer" : "not-allowed"}; opacity:${t.available ? "1" : "0.4"}; text-align:left;">
                  <div style="width:40px; height:40px; border-radius:8px; background:#2a2a2a; display:flex; align-items:center; justify-content:center; flex-shrink:0; font-size:16px;">${t.icon}</div>
                  <div style="flex:1;">
                    <div style="font-family:-apple-system,sans-serif; font-weight:600; font-size:16px; color:#f5f5f5; line-height:1.5;">${t.label}</div>
                    <div style="font-family:-apple-system,sans-serif; font-size:12px; color:#a3a3a3; line-height:1.5;">${t.description}</div>
                  </div>
                  ${t.lastUsed ? `<div style="padding:3px 8px; border-radius:8px; background:#262626; font-family:-apple-system,sans-serif; font-size:12px; font-weight:500; color:#f5f5f5; white-space:nowrap; flex-shrink:0;">Last used</div>` : ""}
                </button>
                ${!t.available && t.unsupportedReason ? `<div class="ledger-tooltip">${t.unsupportedReason}</div>` : ""}
              </div>
              `).join("")}
              ${errorMessage ? alertBox(errorMessage) : ""}
              <div style="padding-top:32px;">
                <button id="closeBtn" style="width:100%; padding:9.5px 24px; border-radius:8px; border:1px solid #404040; background:rgba(255,255,255,0.05); color:#fafafa; cursor:pointer; font-family:-apple-system,sans-serif; font-size:14px; font-weight:500;">Close</button>
              </div>
            </div>
          </div>
        </div>`;
    }

    function renderConnectingUI(statusMessage) {
        root.style.display = "flex";
        root.innerHTML = `
        <style>@keyframes ledger-connect-spin { to { transform: rotate(360deg); } }</style>
        <div style="display:flex; flex-direction:column; width:100%; height:100%; background:#000; border-radius:24px; overflow:hidden; text-align:left;">
          <div style="flex:1; padding:24px; display:flex; flex-direction:column; gap:32px;">
            <div style="display:flex; flex-direction:column; gap:12px; padding-top:20px;">
              <span style="font-family:-apple-system,sans-serif; font-weight:600; font-size:24px; color:#fafafa;">Connect Ledger</span>
              <p style="font-family:-apple-system,sans-serif; font-size:16px; color:#a3a3a3; line-height:1.5; margin:0;">${statusMessage}</p>
            </div>
            <div style="display:flex; align-items:center; justify-content:center; padding:22.5px 0;">
              <div style="width:44px; height:44px; border:3px solid #313131; border-top-color:#fafafa; border-radius:50%; animation:ledger-connect-spin 1s linear infinite;"></div>
            </div>
          </div>
        </div>`;
    }

    renderUI();

    return new Promise((resolve, reject) => {
        async function handleConnect(mode) {
            try {
                if (!(await transportIsConnected()) || _activeTransportMode !== mode) {
                    if (await transportIsConnected()) await transportDisconnect();
                    renderConnectingUI("Connecting to your Ledger device…");
                    await transportConnect(mode);
                    await window.selector.storage.set(STORAGE_KEY_TRANSPORT_MODE, mode);
                }

                renderConnectingUI("Please approve opening the NEAR app on your Ledger device.");
                await openNearApplication();
                resolve();
            } catch (error) {
                const errorMessage = getLedgerErrorMessage(error);
                if (!isGuidanceMessage(errorMessage) && await transportIsConnected()) {
                    await transportDisconnect();
                }
                renderUI(errorMessage);
                setupListeners();
            }
        }

        function setupListeners() {
            const cancelBtn = document.getElementById("cancelBtn");
            const closeBtn = document.getElementById("closeBtn");
            const usbBtn = document.getElementById("usbBtn");
            const bleBtn = document.getElementById("bleBtn");

            if (usbBtn) usbBtn.addEventListener("click", () => handleConnect(TRANSPORT_WEB_USB));
            if (bleBtn) bleBtn.addEventListener("click", () => handleConnect(bleTransportMode));
            if (closeBtn) closeBtn.addEventListener("click", () => {
                root.innerHTML = "";
                root.style.display = "none";
                window.selector.ui.hideIframe();
                reject(new Error("User cancelled"));
            });
            if (cancelBtn) cancelBtn.addEventListener("click", () => {
                root.innerHTML = "";
                root.style.display = "none";
                window.selector.ui.hideIframe();
                reject(new Error("User cancelled"));
            });
        }

        setupListeners();
    });
}

// ============================================================================
// Derivation Path UI
// ============================================================================

async function promptForDerivationPath(currentPath = DEFAULT_DERIVATION_PATH) {
    await window.selector.ui.showIframe();
    const root = document.getElementById("root");
    root.style.display = "flex";

    const pathOptions = [
        { label: "Account 1", path: "44'/397'/0'/0'/0'" },
        { label: "Account 2", path: "44'/397'/0'/0'/1'" },
        { label: "Account 3", path: "44'/397'/0'/0'/2'" },
    ];

    function renderUI() {
        root.innerHTML = `
        <div style="display:flex; flex-direction:column; width:100%; height:100%; background:#000; border-radius:24px; overflow:hidden; text-align:left;">
          <div style="flex:1; padding:24px; display:flex; flex-direction:column; justify-content:space-between; overflow:hidden;">
            <div style="display:flex; flex-direction:column; gap:12px; padding-top:20px;">
              <div style="display:flex; align-items:center; justify-content:space-between;">
                <span style="font-family:-apple-system,sans-serif; font-weight:600; font-size:24px; color:#fafafa;">Select Derivation Path</span>
                <button id="cancelBtn" style="background:transparent; border:none; cursor:pointer; padding:4px;">
                  <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M1 1L13 13M13 1L1 13" stroke="#fafafa" stroke-width="1.5" stroke-linecap="round"/></svg>
                </button>
              </div>
              <p style="font-family:-apple-system,sans-serif; font-size:16px; color:#a3a3a3; line-height:1.5; margin:0;">Choose account index to use from your Ledger.</p>
            </div>
            <div style="display:flex; flex-direction:column; gap:16px; flex:1; padding-top:32px;">
              ${pathOptions.map(opt => `
                <button class="path-btn" data-path="${opt.path}" style="width:100%; padding:12px; border-radius:12px; border:1px solid ${currentPath === opt.path ? "#a6a6a6" : "#313131"}; background:#1a1a1a; cursor:pointer; text-align:left;">
                  <div style="font-family:-apple-system,sans-serif; font-weight:600; font-size:16px; color:#f5f5f5; line-height:1.5;">${opt.label}</div>
                  <div style="font-family:-apple-system,sans-serif; font-size:12px; color:#a3a3a3; line-height:1.5;">${opt.path}</div>
                </button>
              `).join("")}
            </div>
            <div style="padding-top:16px;">
              <button id="confirmBtn" style="width:100%; padding:9.5px 24px; border-radius:8px; border:none; background:#f5f5f5; color:#0a0a0a; cursor:pointer; font-family:-apple-system,sans-serif; font-size:14px; font-weight:500;">Continue</button>
            </div>
          </div>
        </div>`;
    }

    renderUI();

    return new Promise((resolve, reject) => {
        function setupListeners() {
            const confirmBtn = document.getElementById("confirmBtn");
            const cancelBtn = document.getElementById("cancelBtn");
            const pathBtns = document.querySelectorAll(".path-btn");

            pathBtns.forEach(btn => {
                btn.addEventListener("click", () => {
                    currentPath = btn.dataset.path;
                    renderUI();
                    setupListeners();
                });
            });

            confirmBtn.addEventListener("click", () => {
                root.innerHTML = "";
                root.style.display = "none";
                resolve(currentPath);
            });

            cancelBtn.addEventListener("click", () => {
                root.innerHTML = "";
                root.style.display = "none";
                window.selector.ui.hideIframe();
                reject(new Error("User cancelled"));
            });
        }
        setupListeners();
    });
}

// ============================================================================
// Account ID Input UI
// ============================================================================

async function promptForAccountId(foundAccounts = [], implicitAccountId = "", onVerify = null, hideOnSuccess = true) {
    await window.selector.ui.showIframe();
    const root = document.getElementById("root");
    root.style.display = "flex";

    // Filter implicit account out of found accounts to avoid duplication
    const namedAccounts = foundAccounts.filter(a => a !== implicitAccountId);
    const hasResults = namedAccounts.length > 0;

    function renderUI(errorMessage = null, currentValue = "", showManualInput = !hasResults) {
        const accountButtons = namedAccounts.map((acct, i) => `
            <button class="accountBtn" data-account="${acct}" style="width:100%; padding:12px 14px; border-radius:12px; border:1px solid #313131; background:#1a1a1a; cursor:pointer; text-align:left; display:flex; align-items:center; gap:10px;">
              <div style="width:32px; height:32px; border-radius:50%; background:#262626; display:flex; align-items:center; justify-content:center; flex-shrink:0;">
                <span style="font-family:-apple-system,sans-serif; font-size:13px; color:#a3a3a3;">${acct.charAt(0).toUpperCase()}</span>
              </div>
              <span style="font-family:-apple-system,sans-serif; font-size:14px; color:#f5f5f5; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${acct}</span>
            </button>`).join("");

        const implicitButton = implicitAccountId ? `
            <button id="useImplicitBtn" class="accountBtn" data-account="${implicitAccountId}" style="width:100%; padding:12px 14px; border-radius:12px; border:1px solid #313131; background:#1a1a1a; cursor:pointer; text-align:left; display:flex; align-items:center; gap:10px;">
              <div style="width:32px; height:32px; border-radius:50%; background:#262626; display:flex; align-items:center; justify-content:center; flex-shrink:0;">
                <svg width="14" height="14" viewBox="0 0 16 16" fill="none"><path d="M13 14s1 0 1-1-1-4-6-4-6 3-6 4 1 1 1 1h10zM8 7a3 3 0 1 0 0-6 3 3 0 0 0 0 6z" fill="#a3a3a3"/></svg>
              </div>
              <div style="display:flex; flex-direction:column; gap:2px; overflow:hidden;">
                <span style="font-family:-apple-system,sans-serif; font-size:12px; color:#a3a3a3;">Implicit account</span>
                <span style="font-family:-apple-system,sans-serif; font-size:13px; color:#f5f5f5; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${implicitAccountId.slice(0, 16)}...${implicitAccountId.slice(-8)}</span>
              </div>
            </button>` : "";

        const manualInputSection = showManualInput ? `
            <div style="display:flex; flex-direction:column; gap:12px;">
              ${errorMessage ? alertBox(errorMessage) : ""}
              <input type="text" id="accountIdInput" placeholder="example.near" autocapitalize="off" autocorrect="off" value="${currentValue}"
                style="width:100%; padding:12px; border-radius:12px; border:1px solid ${errorMessage ? "#ef4444" : "#a6a6a6"}; background:#1a1a1a; color:#fafafa; font-family:-apple-system,sans-serif; font-size:14px; box-sizing:border-box; outline:none;" />
              <button id="confirmBtn" style="width:100%; padding:9.5px 24px; border-radius:8px; border:none; background:#f5f5f5; color:#0a0a0a; cursor:pointer; font-family:-apple-system,sans-serif; font-size:14px; font-weight:500;">Confirm</button>
            </div>` : `
            ${errorMessage ? alertBox(errorMessage) : ""}
            <button id="showManualBtn" style="width:100%; padding:10px; border-radius:12px; border:1px dashed #525252; background:transparent; cursor:pointer; font-family:-apple-system,sans-serif; font-size:13px; color:#a3a3a3;">
              Enter account ID manually
            </button>`;

        const description = hasResults
            ? "Select the account you'd like to use with this Ledger device."
            : "Ledger provides your public key. Enter the NEAR account ID that this key has access to.";

        root.innerHTML = `
        <div style="display:flex; flex-direction:column; width:100%; height:100%; background:#000; border-radius:24px; overflow:hidden; text-align:left;">
          <div style="flex:1; padding:24px; display:flex; flex-direction:column; gap:24px; overflow:auto;">
            <div style="display:flex; flex-direction:column; gap:12px; padding-top:20px;">
              <div style="display:flex; align-items:center; justify-content:space-between;">
                <span style="font-family:-apple-system,sans-serif; font-weight:600; font-size:24px; color:#fafafa;">Select Account</span>
                <button id="cancelBtn" style="background:transparent; border:none; cursor:pointer; padding:4px;">
                  <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M1 1L13 13M13 1L1 13" stroke="#fafafa" stroke-width="1.5" stroke-linecap="round"/></svg>
                </button>
              </div>
              <p style="font-family:-apple-system,sans-serif; font-size:14px; color:#a3a3a3; line-height:1.5; margin:0;">
                ${description}
              </p>
            </div>
            <div style="display:flex; flex-direction:column; gap:10px;">
              ${accountButtons}
              ${implicitButton}
            </div>
            ${manualInputSection}
          </div>
        </div>`;
    }

    renderUI();

    return new Promise((resolve, reject) => {
        function selectAccount(accountId) {
            return async () => {
                // Find the clicked button and show loading state
                const btns = document.querySelectorAll(".accountBtn");
                btns.forEach(btn => {
                    btn.disabled = true;
                    if (btn.dataset.account === accountId) {
                        btn.style.borderColor = "#525252";
                        btn.style.opacity = "0.7";
                    }
                });
                const confirmBtn = document.getElementById("confirmBtn");
                if (confirmBtn) confirmBtn.disabled = true;

                if (onVerify) {
                    try {
                        await onVerify(accountId);
                        if (hideOnSuccess) {
                            root.innerHTML = "";
                            root.style.display = "none";
                            window.selector.ui.hideIframe();
                        }
                        resolve(accountId);
                    } catch (error) {
                        renderUI(error.message, "", false);
                        setupListeners();
                    }
                } else {
                    if (hideOnSuccess) {
                        root.innerHTML = "";
                        root.style.display = "none";
                        window.selector.ui.hideIframe();
                    }
                    resolve(accountId);
                }
            };
        }

        function setupListeners() {
            const cancelBtn = document.getElementById("cancelBtn");
            cancelBtn.addEventListener("click", () => {
                root.innerHTML = "";
                root.style.display = "none";
                window.selector.ui.hideIframe();
                reject(new Error("User cancelled"));
            });

            // Account selection buttons
            document.querySelectorAll(".accountBtn").forEach(btn => {
                btn.addEventListener("click", selectAccount(btn.dataset.account));
            });

            // "Enter manually" toggle
            const showManualBtn = document.getElementById("showManualBtn");
            if (showManualBtn) {
                showManualBtn.addEventListener("click", () => {
                    renderUI(null, "", true);
                    setupListeners();
                });
            }

            // Manual input confirm
            const confirmBtn = document.getElementById("confirmBtn");
            const input = document.getElementById("accountIdInput");
            if (confirmBtn && input) {
                confirmBtn.addEventListener("click", async () => {
                    const accountId = input.value.trim();
                    if (!accountId) return;
                    if (onVerify) {
                        confirmBtn.disabled = true;
                        confirmBtn.textContent = "Verifying...";
                        try {
                            await onVerify(accountId);
                            if (hideOnSuccess) {
                                root.innerHTML = "";
                                root.style.display = "none";
                                window.selector.ui.hideIframe();
                            }
                            resolve(accountId);
                        } catch (error) {
                            renderUI(error.message, accountId, true);
                            setupListeners();
                        }
                    } else {
                        if (hideOnSuccess) {
                            root.innerHTML = "";
                            root.style.display = "none";
                            window.selector.ui.hideIframe();
                        }
                        resolve(accountId);
                    }
                });
                input.addEventListener("keypress", e => {
                    if (e.key === "Enter") confirmBtn.click();
                });
                setTimeout(() => input.focus(), 100);
            }
        }
        setupListeners();
    });
}

// ============================================================================
// Access Key Verification
// ============================================================================

async function verifyAccessKey(network, accountId, publicKey) {
    // Check account exists
    try {
        await rpcRequest(network, "query", {
            request_type: "view_account",
            finality: "final",
            account_id: accountId,
        });
    } catch (error) {
        const msg = error.message || "";
        if (msg.includes("does not exist") || msg.includes("UnknownAccount")) {
            throw new Error(`Account ${accountId} does not exist on the NEAR blockchain.`);
        }
        throw error;
    }

    // Check access key
    try {
        const accessKey = await rpcRequest(network, "query", {
            request_type: "view_access_key",
            finality: "final",
            account_id: accountId,
            public_key: publicKey,
        });
        if (accessKey.permission !== "FullAccess") {
            throw new Error("The public key does not have FullAccess permission for this account.");
        }
        return true;
    } catch (error) {
        const msg = error.message || "";
        if (msg.includes("access key") || msg.includes("does not exist")) {
            throw new Error(`Access key not found for account ${accountId}. Please make sure the Ledger public key is registered for this account.`);
        }
        throw error;
    }
}

// ============================================================================
// Borsh Serialization Helpers
// ============================================================================

// We manually construct Borsh-serialized payloads to avoid importing
// @near-js/transactions which has assertion code that fails in WKWebView.

function writeU32LE(buf, offset, val) {
    buf[offset]     = val & 0xff;
    buf[offset + 1] = (val >> 8) & 0xff;
    buf[offset + 2] = (val >> 16) & 0xff;
    buf[offset + 3] = (val >> 24) & 0xff;
    return offset + 4;
}

function writeU64LE(buf, offset, val) {
    const bigVal = BigInt(val);
    for (let i = 0; i < 8; i++) {
        buf[offset + i] = Number((bigVal >> BigInt(i * 8)) & 0xFFn);
    }
    return offset + 8;
}

function writeU128LE(buf, offset, val) {
    const bigVal = BigInt(val);
    for (let i = 0; i < 16; i++) {
        buf[offset + i] = Number((bigVal >> BigInt(i * 8)) & 0xFFn);
    }
    return offset + 16;
}

// ============================================================================
// Transaction Building (manual Borsh)
// ============================================================================

/**
 * Build action bytes for a single action in Borsh format.
 * Returns { bytes: Uint8Array }
 */
function buildActionBytes(action) {
    const parts = [];

    function pushAction(typeIndex, data) {
        const buf = new Uint8Array(1 + data.length);
        buf[0] = typeIndex;
        buf.set(data, 1);
        parts.push(buf);
    }

    if (action.type === "FunctionCall") {
        const p = action.params;
        const methodBytes = new TextEncoder().encode(p.methodName);
        let args;
        if (typeof p.args === "string") {
            // base64 encoded
            args = Uint8Array.from(atob(p.args), c => c.charCodeAt(0));
        } else if (p.args instanceof Uint8Array) {
            args = p.args;
        } else if (typeof p.args === "object") {
            args = new TextEncoder().encode(JSON.stringify(p.args));
        } else {
            args = new Uint8Array(0);
        }
        const data = new Uint8Array(4 + methodBytes.length + 4 + args.length + 8 + 16);
        let off = 0;
        off = writeU32LE(data, off, methodBytes.length);
        data.set(methodBytes, off); off += methodBytes.length;
        off = writeU32LE(data, off, args.length);
        data.set(args, off); off += args.length;
        off = writeU64LE(data, off, p.gas || "30000000000000");
        off = writeU128LE(data, off, p.deposit || "0");
        pushAction(2, data.subarray(0, off));
    } else if (action.type === "Transfer") {
        const data = new Uint8Array(16);
        writeU128LE(data, 0, action.params.deposit);
        pushAction(3, data);
    } else if (action.type === "CreateAccount") {
        pushAction(0, new Uint8Array(0));
    } else if (action.type === "DeleteAccount") {
        const benBytes = new TextEncoder().encode(action.params.beneficiaryId);
        const data = new Uint8Array(4 + benBytes.length);
        writeU32LE(data, 0, benBytes.length);
        data.set(benBytes, 4);
        pushAction(7, data);
    } else if (action.type === "AddKey") {
        const p = action.params;
        const pkStr = p.publicKey;
        const keyStr = pkStr.startsWith("ed25519:") ? pkStr.slice(8) : pkStr;
        const keyBytes = base58Decode(keyStr);

        if (p.accessKey && p.accessKey.permission && p.accessKey.permission !== "FullAccess") {
            // FunctionCall access key
            // Borsh: publicKey(33) + nonce(8) + enum(1=FunctionCall) + allowance(option<u128>=1+16) + receiverId(string) + methodNames(vec<string>)
            const perm = p.accessKey.permission;
            const receiverBytes = new TextEncoder().encode(perm.receiverId || "");
            const methodNames = perm.methodNames || [];
            const methodParts = methodNames.map(m => new TextEncoder().encode(m));
            const methodsSize = 4 + methodParts.reduce((s, m) => s + 4 + m.length, 0);
            const hasAllowance = perm.allowance != null && perm.allowance !== "0";
            const allowanceSize = 1 + (hasAllowance ? 16 : 0);
            const totalSize = 33 + 8 + 1 + allowanceSize + 4 + receiverBytes.length + methodsSize;
            const data = new Uint8Array(totalSize);
            let off = 0;
            data[off] = 0; off += 1; // keyType ed25519
            data.set(keyBytes.subarray(0, 32), off); off += 32;
            off = writeU64LE(data, off, 0); // nonce
            data[off] = 0; off += 1; // enum variant 0 = FunctionCall
            if (hasAllowance) {
                data[off] = 1; off += 1; // Some
                off = writeU128LE(data, off, perm.allowance);
            } else {
                data[off] = 0; off += 1; // None
            }
            off = writeU32LE(data, off, receiverBytes.length);
            data.set(receiverBytes, off); off += receiverBytes.length;
            off = writeU32LE(data, off, methodNames.length);
            for (const m of methodParts) {
                off = writeU32LE(data, off, m.length);
                data.set(m, off); off += m.length;
            }
            pushAction(5, data.subarray(0, off));
        } else {
            // FullAccess key
            const data = new Uint8Array(33 + 8 + 1);
            let off = 0;
            data[off] = 0; off += 1; // keyType ed25519
            data.set(keyBytes.subarray(0, 32), off); off += 32;
            off = writeU64LE(data, off, 0); // nonce
            data[off] = 1; off += 1; // FullAccess
            pushAction(5, data.subarray(0, off));
        }
    } else if (action.type === "DeleteKey") {
        const data = new Uint8Array(33);
        const pkStr = action.params.publicKey;
        const keyStr = pkStr.startsWith("ed25519:") ? pkStr.slice(8) : pkStr;
        const keyBytes = base58Decode(keyStr);
        data[0] = 0; // keyType
        data.set(keyBytes.subarray(0, 32), 1);
        pushAction(6, data);
    } else if (action.type === "Stake") {
        const data = new Uint8Array(16 + 33);
        let off = writeU128LE(data, 0, action.params.stake);
        const pkStr = action.params.publicKey;
        const keyStr = pkStr.startsWith("ed25519:") ? pkStr.slice(8) : pkStr;
        const keyBytes = base58Decode(keyStr);
        data[off] = 0; off += 1;
        data.set(keyBytes.subarray(0, 32), off);
        pushAction(4, data);
    } else if (action.type === "DeployContract") {
        const code = action.params.code;
        const data = new Uint8Array(4 + code.length);
        writeU32LE(data, 0, code.length);
        data.set(code, 4);
        pushAction(1, data);
    } else {
        throw new Error("Unsupported action type: " + action.type);
    }

    // Concatenate all parts
    const totalLength = parts.reduce((s, p) => s + p.length, 0);
    const result = new Uint8Array(totalLength);
    let pos = 0;
    for (const p of parts) { result.set(p, pos); pos += p.length; }
    return result;
}

/**
 * Build a complete Borsh-serialized transaction (unsigned).
 */
function buildTransaction(signerId, publicKey, receiverId, nonce, actions, blockHash) {
    const parts = [];

    // signerId (borsh string)
    const signerBytes = new TextEncoder().encode(signerId);
    const signerBuf = new Uint8Array(4 + signerBytes.length);
    writeU32LE(signerBuf, 0, signerBytes.length);
    signerBuf.set(signerBytes, 4);
    parts.push(signerBuf);

    // publicKey (enum variant 0 = ed25519, then 32 bytes)
    const pkBuf = new Uint8Array(33);
    const keyStr = publicKey.startsWith("ed25519:") ? publicKey.slice(8) : publicKey;
    pkBuf[0] = 0;
    pkBuf.set(base58Decode(keyStr).subarray(0, 32), 1);
    parts.push(pkBuf);

    // nonce (u64 LE)
    const nonceBuf = new Uint8Array(8);
    writeU64LE(nonceBuf, 0, nonce);
    parts.push(nonceBuf);

    // receiverId (borsh string)
    const recvBytes = new TextEncoder().encode(receiverId);
    const recvBuf = new Uint8Array(4 + recvBytes.length);
    writeU32LE(recvBuf, 0, recvBytes.length);
    recvBuf.set(recvBytes, 4);
    parts.push(recvBuf);

    // blockHash (32 bytes)
    parts.push(blockHash);

    // actions (vec<Action>: u32 count + action bytes)
    const actionParts = actions.map(a => buildActionBytes(a));
    const countBuf = new Uint8Array(4);
    writeU32LE(countBuf, 0, actionParts.length);
    parts.push(countBuf);
    for (const ap of actionParts) parts.push(ap);

    // Concatenate
    const totalLength = parts.reduce((s, p) => s + p.length, 0);
    const result = new Uint8Array(totalLength);
    let pos = 0;
    for (const p of parts) { result.set(p, pos); pos += p.length; }
    return result;
}

/**
 * Build a Borsh-serialized DelegateAction.
 */
function buildDelegateActionBytes(senderId, receiverId, actions, nonce, maxBlockHeight, publicKey) {
    const parts = [];

    // senderId
    const senderBytes = new TextEncoder().encode(senderId);
    const senderBuf = new Uint8Array(4 + senderBytes.length);
    writeU32LE(senderBuf, 0, senderBytes.length);
    senderBuf.set(senderBytes, 4);
    parts.push(senderBuf);

    // receiverId
    const recvBytes = new TextEncoder().encode(receiverId);
    const recvBuf = new Uint8Array(4 + recvBytes.length);
    writeU32LE(recvBuf, 0, recvBytes.length);
    recvBuf.set(recvBytes, 4);
    parts.push(recvBuf);

    // actions (NonDelegateAction vec)
    const actionParts = actions.map(a => buildActionBytes(a));
    const countBuf = new Uint8Array(4);
    writeU32LE(countBuf, 0, actionParts.length);
    parts.push(countBuf);
    for (const ap of actionParts) parts.push(ap);

    // nonce (u64)
    const nonceBuf = new Uint8Array(8);
    writeU64LE(nonceBuf, 0, nonce);
    parts.push(nonceBuf);

    // maxBlockHeight (u64)
    const mbbuf = new Uint8Array(8);
    writeU64LE(mbbuf, 0, maxBlockHeight);
    parts.push(mbbuf);

    // publicKey
    const pkBuf = new Uint8Array(33);
    const keyStr = publicKey.startsWith("ed25519:") ? publicKey.slice(8) : publicKey;
    pkBuf[0] = 0;
    pkBuf.set(base58Decode(keyStr).subarray(0, 32), 1);
    parts.push(pkBuf);

    const totalLength = parts.reduce((s, p) => s + p.length, 0);
    const result = new Uint8Array(totalLength);
    let pos = 0;
    for (const p of parts) { result.set(p, pos); pos += p.length; }
    return result;
}

/**
 * Build NEP-413 message payload (Borsh serialized).
 */
function buildNep413Payload(message, recipient, nonce) {
    const messageBytes = new TextEncoder().encode(message);
    const recipientBytes = new TextEncoder().encode(recipient);
    const payloadSize = 4 + messageBytes.length + 32 + 4 + recipientBytes.length + 1;
    const payload = new Uint8Array(payloadSize);
    const view = new DataView(payload.buffer);
    let offset = 0;
    view.setUint32(offset, messageBytes.length, true); offset += 4;
    payload.set(messageBytes, offset); offset += messageBytes.length;
    payload.set(nonce, offset); offset += 32;
    view.setUint32(offset, recipientBytes.length, true); offset += 4;
    payload.set(recipientBytes, offset); offset += recipientBytes.length;
    payload[offset] = 0; // callback_url = None
    return payload;
}

// ============================================================================
// Wallet Implementation
// ============================================================================

class LedgerWallet {
    async getDerivationPath() {
        const path = await window.selector.storage.get(STORAGE_KEY_DERIVATION_PATH);
        return path || DEFAULT_DERIVATION_PATH;
    }

    async _reconnectForSigning() {
        if (await transportIsConnected()) return;

        const storedMode = await window.selector.storage.get(STORAGE_KEY_TRANSPORT_MODE);
        if (!storedMode) throw new Error("Ledger is not connected. Please sign in with Ledger first.");

        // WebUSB can reconnect silently via getDevices() — no user gesture needed
        if ((storedMode === TRANSPORT_WEB_USB || storedMode === TRANSPORT_WEB_HID) && isWebUsbSupported()) {
            try {
                const devices = await navigator.usb.getDevices();
                const device = devices.find(d => d.vendorId === LEDGER_VENDOR_ID);
                if (device) {
                    await usbOpen(device);
                    _activeTransport = _usbDevice;
                    _activeTransportMode = TRANSPORT_WEB_USB;
                    await openNearApplication();
                    return;
                }
            } catch {
                // WebUSB silent reconnect failed — fall through to manual reconnect
            }
        }

        // Native BLE can reconnect silently — the native side persists the connection across iframe lifecycles
        if (storedMode === TRANSPORT_NATIVE_BLE) {
            try {
                await transportConnect(TRANSPORT_NATIVE_BLE);
                await openNearApplication();
                return;
            } catch {
                // Native BLE reconnect failed — fall through to manual reconnect
            }
        }

        // Web BLE and HID require requestDevice() which needs a user gesture.
        // Show a "Connect" button so the click provides the gesture context.
        await window.selector.ui.showIframe();
        const root = document.getElementById("root");
        root.style.display = "flex";

        await new Promise((resolve, reject) => {
            function renderConnectButton(errorMessage) {
                root.innerHTML = `
                <div style="display:flex; flex-direction:column; width:100%; height:100%; background:#000; border-radius:24px; overflow:hidden; text-align:left;">
                  <div style="flex:1; padding:24px; display:flex; flex-direction:column; gap:32px; overflow:auto;">
                    <div style="display:flex; flex-direction:column; gap:12px; padding-top:20px;">
                      <div style="display:flex; align-items:center; justify-content:space-between;">
                        <span style="font-family:-apple-system,sans-serif; font-weight:600; font-size:24px; color:#fafafa;">Reconnect Ledger</span>
                        <button id="reconnectCancelBtn" style="background:transparent; border:none; cursor:pointer; padding:4px;">
                          <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M1 1L13 13M13 1L1 13" stroke="#fafafa" stroke-width="1.5" stroke-linecap="round"/></svg>
                        </button>
                      </div>
                      <p style="font-family:-apple-system,sans-serif; font-size:16px; color:#a3a3a3; line-height:1.5; margin:0;">
                        Please make sure your Ledger device is ${(storedMode === TRANSPORT_WEB_BLE || storedMode === TRANSPORT_NATIVE_BLE) ? "nearby and Bluetooth is enabled" : "connected via USB"}.
                      </p>
                    </div>
                    ${errorMessage ? alertBox(errorMessage) : ""}
                    <div style="display:flex; flex-direction:column; gap:8px;">
                      <button id="reconnectBtn" style="width:100%; padding:9.5px 24px; border-radius:8px; border:none; background:#f5f5f5; color:#0a0a0a; cursor:pointer; font-family:-apple-system,sans-serif; font-size:14px; font-weight:500;">Connect</button>
                    </div>
                  </div>
                </div>`;

                document.getElementById("reconnectBtn").addEventListener("click", async () => {
                    root.innerHTML = `
                    <style>@keyframes ledger-spin { to { transform: rotate(360deg); } }</style>
                    <div style="display:flex; flex-direction:column; width:100%; height:100%; background:#000; border-radius:24px; overflow:hidden; text-align:left;">
                      <div style="flex:1; padding:24px; display:flex; flex-direction:column; gap:32px;">
                        <div style="display:flex; flex-direction:column; gap:12px; padding-top:20px;">
                          <span style="font-family:-apple-system,sans-serif; font-weight:600; font-size:24px; color:#fafafa;">Reconnect Ledger</span>
                          <p style="font-family:-apple-system,sans-serif; font-size:16px; color:#a3a3a3; line-height:1.5; margin:0;">Connecting to your Ledger device…</p>
                        </div>
                        <div style="display:flex; align-items:center; justify-content:center; padding:22.5px 0;">
                          <div style="width:44px; height:44px; border:3px solid #313131; border-top-color:#fafafa; border-radius:50%; animation:ledger-spin 1s linear infinite;"></div>
                        </div>
                      </div>
                    </div>`;
                    try {
                        if (!(await transportIsConnected())) {
                            await transportConnect(storedMode);
                        }
                        await openNearApplication();
                        root.innerHTML = "";
                        root.style.display = "none";
                        resolve();
                    } catch (error) {
                        const errorMsg = getLedgerErrorMessage(error);
                        renderConnectButton(errorMsg);
                    }
                });

                document.getElementById("reconnectCancelBtn").addEventListener("click", () => {
                    root.innerHTML = "";
                    root.style.display = "none";
                    window.selector.ui.hideIframe();
                    reject(new Error("User cancelled"));
                });
            }

            renderConnectButton(null);
        });
    }

    async _ensureReady() {
        const accounts = await this.getAccounts();
        if (!accounts || accounts.length === 0) throw new Error("No account connected");
        if (!(await transportIsConnected())) await this._reconnectForSigning();
        return accounts;
    }

    async _getAccessKeyAndBlock(network, signerId, publicKey) {
        const accessKey = await rpcRequest(network, "query", {
            request_type: "view_access_key",
            finality: "final",
            account_id: signerId,
            public_key: publicKey,
        });
        const block = await rpcRequest(network, "block", { finality: "final" });
        return { accessKey, block };
    }

    async _performSignInFlow(params) {
        await promptForLedgerConnect();

        const defaultPath = await this.getDerivationPath();
        const derivationPath = await promptForDerivationPath(defaultPath);

        const publicKeyBytes = await showLedgerApprovalUI(
            "Approve on Ledger",
            "Please approve the request on your Ledger device to share your public key.",
            () => getPublicKey(derivationPath),
        );
        const publicKeyString = base58Encode(publicKeyBytes);
        const publicKey = `ed25519:${publicKeyString}`;

        // Implicit account ID (hex)
        const implicitAccountId = Array.from(publicKeyBytes).map(b => b.toString(16).padStart(2, "0")).join("");

        const network = params?.network || "mainnet";

        // Look up accounts associated with this public key
        const foundAccounts = await lookupAccountsByPublicKey(network, publicKey);

        const verifyAccount = async (accountId) => {
            await verifyAccessKey(network, accountId, publicKey);
        };

        const accountId = await promptForAccountId(foundAccounts, implicitAccountId, verifyAccount, false);

        const accounts = [{ accountId, publicKey }];
        await window.selector.storage.set(STORAGE_KEY_ACCOUNTS, JSON.stringify(accounts));
        await window.selector.storage.set(STORAGE_KEY_DERIVATION_PATH, derivationPath);

        // If addFunctionCallKey is requested, build and sign the AddKey transaction
        if (params?.addFunctionCallKey) {
            await this._addFunctionCallKey(params.addFunctionCallKey, accountId, publicKey, derivationPath, network);
        }

        return { accounts, derivationPath };
    }

    async _addFunctionCallKey(keyParams, accountId, signerPublicKey, derivationPath, network) {
        const { contractId, publicKey: fckPublicKey, allowMethods, gasAllowance } = keyParams;

        // Build method names list
        const methodNames = allowMethods?.anyMethod ? [] : (allowMethods?.methodNames || []);

        // Compute allowance in yoctoNEAR
        let allowance = null;
        if (!gasAllowance || gasAllowance.kind === "limited") {
            allowance = gasAllowance?.amount || "250000000000000000000000"; // 0.25 NEAR default
        }
        // gasAllowance.kind === "unlimited" → allowance stays null (None in borsh)

        // Build descriptive UI message
        const methodDesc = methodNames.length > 0
            ? `methods: ${methodNames.join(", ")}`
            : "any method";
        const allowanceDesc = allowance
            ? `up to ${formatNearAmount(allowance)} NEAR in gas fees`
            : "unlimited gas fees";

        const { accessKey, block } = await this._getAccessKeyAndBlock(network, accountId, signerPublicKey);
        const blockHash = base58Decode(block.header.hash);
        const nonce = BigInt(accessKey.nonce) + 1n;

        const addKeyAction = {
            type: "AddKey",
            params: {
                publicKey: fckPublicKey,
                accessKey: {
                    permission: {
                        receiverId: contractId,
                        methodNames: methodNames,
                        allowance: allowance,
                    },
                },
            },
        };

        const txBytes = buildTransaction(accountId, signerPublicKey, accountId, nonce, [addKeyAction], blockHash);

        const signature = await showLedgerApprovalUI(
            "Grant App Access",
            `This transaction adds an access key that allows the application to call ${methodDesc} on <strong style="color:#fafafa;">${contractId}</strong> using ${allowanceDesc} on behalf of your account. Your funds remain safe — the key can only be used for gas fees, not transfers.`,
            () => sign(txBytes, derivationPath),
            true,
        );

        // Build signed transaction and broadcast
        const signedTx = new Uint8Array(txBytes.length + 1 + 64);
        signedTx.set(txBytes, 0);
        signedTx[txBytes.length] = 0; // ed25519
        signedTx.set(signature.subarray(0, 64), txBytes.length + 1);

        const base64Tx = btoa(String.fromCharCode(...signedTx));
        await rpcRequest(network, "broadcast_tx_commit", [base64Tx]);
    }

    async signIn(params) {
        try {
            const { accounts } = await this._performSignInFlow(params);
            window.selector.ui.hideIframe();
            return accounts;
        } catch (error) {
            await transportDisconnect();
            throw error;
        }
    }

    async signInAndSignMessage(params) {
        try {
            const { accounts, derivationPath } = await this._performSignInFlow(params);
            const { message, recipient, nonce } = params.messageParams;
            const payload = buildNep413Payload(message, recipient || "", nonce || new Uint8Array(32));

            // NEP-413 prefix for Ledger signing
            const NEP413_PREFIX = new Uint8Array([0x9d, 0x01, 0x00, 0x80]);
            const dataWithPrefix = new Uint8Array(NEP413_PREFIX.length + payload.length);
            dataWithPrefix.set(NEP413_PREFIX, 0);
            dataWithPrefix.set(payload, NEP413_PREFIX.length);

            const signature = await showLedgerApprovalUI(
                "Sign Message",
                "Please review and approve the message signing on your Ledger device.",
                () => sign(dataWithPrefix, derivationPath),
                true,
            );

            const signatureBase64 = btoa(String.fromCharCode(...signature));

            return accounts.map(account => ({
                ...account,
                signedMessage: {
                    accountId: account.accountId,
                    publicKey: account.publicKey,
                    signature: signatureBase64,
                },
            }));
        } catch (error) {
            await transportDisconnect();
            throw error;
        }
    }

    async signOut() {
        await transportDisconnect();
        await window.selector.storage.remove(STORAGE_KEY_ACCOUNTS);
        await window.selector.storage.remove(STORAGE_KEY_DERIVATION_PATH);
        await window.selector.storage.remove(STORAGE_KEY_TRANSPORT_MODE);
        return true;
    }

    async getAccounts() {
        const json = await window.selector.storage.get(STORAGE_KEY_ACCOUNTS);
        if (!json) return [];
        try { return JSON.parse(json); } catch { return []; }
    }

    async signAndSendTransaction(params) {
        const accounts = await this._ensureReady();
        const network = params.network || "mainnet";
        const signerId = accounts[0].accountId;
        const { receiverId, actions } = params;

        let blockHash = params.blockHash;
        let nonce = params.nonce;
        if (blockHash == null || nonce == null) {
            const { accessKey, block } = await this._getAccessKeyAndBlock(network, signerId, accounts[0].publicKey);
            blockHash ??= base58Decode(block.header.hash);
            nonce ??= BigInt(accessKey.nonce) + 1n;
        }

        const txBytes = buildTransaction(signerId, accounts[0].publicKey, receiverId, nonce, actions, blockHash);
        const derivationPath = await this.getDerivationPath();

        const signature = await showLedgerApprovalUI(
            "Approve Transaction",
            "Please review and approve the transaction on your Ledger device.",
            () => sign(txBytes, derivationPath),
            true,
        );

        // Build signed transaction: tx bytes + signature (enum variant 0 + 64 bytes)
        const signedTx = new Uint8Array(txBytes.length + 1 + 64);
        signedTx.set(txBytes, 0);
        signedTx[txBytes.length] = 0; // ed25519
        signedTx.set(signature.subarray(0, 64), txBytes.length + 1);

        const base64Tx = btoa(String.fromCharCode(...signedTx));
        const result = await rpcRequest(network, "broadcast_tx_commit", [base64Tx]);
        return result;
    }

    async signAndSendTransactions(params) {
        const accounts = await this._ensureReady();
        const network = params.network || "mainnet";
        const signerId = accounts[0].accountId;

        const { accessKey, block } = await this._getAccessKeyAndBlock(network, signerId, accounts[0].publicKey);
        const blockHash = base58Decode(block.header.hash);
        let nonce = BigInt(accessKey.nonce);

        const results = [];
        for (const tx of params.transactions) {
            nonce += 1n;
            const result = await this.signAndSendTransaction({
                network,
                receiverId: tx.receiverId,
                actions: tx.actions,
                nonce,
                blockHash,
            });
            results.push(result);
        }
        return results;
    }

    async signDelegateActions(params) {
        const accounts = await this._ensureReady();
        const network = params.network || "mainnet";
        const { accountId: signerId, publicKey } = accounts[0];
        const derivationPath = await this.getDerivationPath();

        const { accessKey, block } = await this._getAccessKeyAndBlock(network, signerId, publicKey);
        let nonce = BigInt(accessKey.nonce);
        const maxBlockHeight = BigInt(block.header.height) + 120n;

        const signedDelegateActions = [];
        for (const { receiverId, actions } of params.delegateActions) {
            nonce += 1n;

            const daBytes = buildDelegateActionBytes(signerId, receiverId, actions, nonce, maxBlockHeight, publicKey);

            // NEP-366 prefix for Ledger signing
            const NEP366_PREFIX = new Uint8Array([0x6e, 0x01, 0x00, 0x40]);
            const dataWithPrefix = new Uint8Array(NEP366_PREFIX.length + daBytes.length);
            dataWithPrefix.set(NEP366_PREFIX, 0);
            dataWithPrefix.set(daBytes, NEP366_PREFIX.length);

            const signature = await showLedgerApprovalUI(
                "Approve Transaction",
                "Please review and approve the transaction on your Ledger device.",
                () => sign(dataWithPrefix, derivationPath),
                true,
            );

            // Build SignedDelegate: DelegateAction bytes + Signature (enum 0 + 64 bytes)
            const signedDelegateBytes = new Uint8Array(daBytes.length + 1 + 64);
            signedDelegateBytes.set(daBytes, 0);
            signedDelegateBytes[daBytes.length] = 0; // ed25519
            signedDelegateBytes.set(signature.subarray(0, 64), daBytes.length + 1);

            signedDelegateActions.push(btoa(String.fromCharCode(...signedDelegateBytes)));
        }
        return { signedDelegateActions };
    }

    async signMessage(params) {
        const accounts = await this._ensureReady();
        const message = params.message;
        const recipient = params.recipient || "";
        const nonce = params.nonce || new Uint8Array(32);

        const payload = buildNep413Payload(message, recipient, nonce);
        const NEP413_PREFIX = new Uint8Array([0x9d, 0x01, 0x00, 0x80]);
        const dataWithPrefix = new Uint8Array(NEP413_PREFIX.length + payload.length);
        dataWithPrefix.set(NEP413_PREFIX, 0);
        dataWithPrefix.set(payload, NEP413_PREFIX.length);

        const derivationPath = await this.getDerivationPath();
        const signature = await showLedgerApprovalUI(
            "Sign Message",
            "Please review and approve the message signing on your Ledger device.",
            () => sign(dataWithPrefix, derivationPath),
            true,
        );

        const signatureBase64 = btoa(String.fromCharCode(...signature));
        return {
            accountId: accounts[0].accountId,
            publicKey: accounts[0].publicKey,
            signature: signatureBase64,
        };
    }
}

// ============================================================================
// Initialize and register with near-connect
// ============================================================================

const wallet = new LedgerWallet();
window.selector.ready(wallet);
