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
const MAX_BLOCK_HEIGHT_INCREMENT = 900n; // About 15 minutes

// How long we wait for the device to answer an APDU. Signing APDUs only answer
// once the user has reviewed and approved (or rejected) on the Ledger, so this
// is the user's review time — keep it generous enough to read a long
// transaction thoroughly.
const DEVICE_REVIEW_TIMEOUT_MS = 7 * 60 * 1000;
// Connection housekeeping (scan, connect, probe) that never waits on the user.
const NATIVE_BLE_OPERATION_TIMEOUT_MS = 60 * 1000;

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

function nativeBLE(action, params = {}, timeoutMs = NATIVE_BLE_OPERATION_TIMEOUT_MS) {
    return new Promise((resolve, reject) => {
        const id = String(++_callbackId);
        _pendingCallbacks.set(id, { resolve, reject });
        try {
            // Post to all ancestor frames so the native bridge relay receives
            // the message regardless of iframe nesting depth.
            const msg = { type: LEDGER_BLE_REQUEST, id, action, params };
            let target = window.parent;
            while (target && target !== window) {
                try { target.postMessage(msg, "*"); } catch { }
                if (target === target.parent) break;
                target = target.parent;
            }
        } catch (e) {
            _pendingCallbacks.delete(id);
            reject(new Error("Native BLE bridge unavailable: " + e.message));
        }

        setTimeout(() => {
            if (_pendingCallbacks.has(id)) {
                _pendingCallbacks.delete(id);
                reject(new Error("Ledger BLE operation timed out"));
            }
        }, timeoutMs);
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
    try { await device.reset(); } catch { }

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
            }, DEVICE_REVIEW_TIMEOUT_MS);
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
// The device the user picked in the Bluetooth chooser. Kept even when GATT
// drops (Ledger sleeps, goes out of range) so we can reconnect with
// gatt.connect() — no chooser, no user gesture — instead of asking the user
// to pick it again. Only signOut forgets it.
let _bleKnownDevice = null;
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
            new Promise((_, reject) => setTimeout(() => reject(new Error("BLE response timed out")), DEVICE_REVIEW_TIMEOUT_MS)),
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
        for (const uuid of LEDGER_BLE_SERVICES) {
            try { service = await device.gatt.getPrimaryService(uuid); break; } catch { }
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
        try { await _bleNotifyChar.stopNotifications(); } catch { }
        device.gatt.disconnect();
        await new Promise(r => setTimeout(r, 4000));
        return await bleOpen(device, false);
    }
}

function _bleOnDisconnected() {
    console.log("[BLE] device disconnected");
    // Drop the session, keep _bleKnownDevice for a silent reconnect.
    _bleDevice = null;
    _bleWriteChar = null;
    _bleNotifyChar = null;
    _bleNotifyQueue = [];
    _bleNotifyResolve = null;
    if (_activeTransportMode === TRANSPORT_WEB_BLE) {
        _activeTransport = null;
        _activeTransportMode = null;
    }
}

/**
 * Web Bluetooth errors meaning the GATT link is gone or stale: the Ledger
 * locked, slept or went out of range. The browser may still report
 * `gatt.connected`, so the session has to be dropped explicitly.
 */
function isBleLinkError(error) {
    const msg = error?.message || "";
    return error?.name === "NetworkError"
        || error?.name === "InvalidStateError"
        || /GATT|no longer exists|BLE device not connected/i.test(msg);
}

/** Error shown when the Bluetooth link to the Ledger is lost. */
const BLE_LINK_LOST_MESSAGE = "Lost the Bluetooth connection to your Ledger.";

/** Forget the current GATT session (not the device) so the next call reconnects. */
function bleDropSession() {
    const device = _bleDevice || _bleKnownDevice;
    if (_bleNotifyChar) _bleNotifyChar.removeEventListener("characteristicvaluechanged", _bleOnNotification);
    _bleOnDisconnected();
    try { if (device?.gatt?.connected) device.gatt.disconnect(); } catch { }
}

/** Open (or re-open) GATT to the remembered device; throws if that isn't possible. */
async function bleReconnectKnownDevice() {
    if (!_bleKnownDevice) throw new Error("No remembered Ledger Bluetooth device");
    await bleOpen(_bleKnownDevice, false);
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
        try {
            return await bleExchange(apdu);
        } catch (error) {
            if (!isBleLinkError(error)) throw error;
            console.log("[BLE] link lost during exchange:", error.message);
            bleDropSession();
            throw new Error(BLE_LINK_LOST_MESSAGE);
        }
    }
    if (_activeTransportMode === TRANSPORT_NATIVE_BLE) {
        const response = await nativeBLE("exchange", { command: Array.from(apdu) }, DEVICE_REVIEW_TIMEOUT_MS);
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
                } catch { }
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
        // Reconnect to the device picked earlier without showing the chooser.
        if (_bleKnownDevice) {
            try {
                await bleReconnectKnownDevice();
                return;
            } catch (e) {
                console.log("[BLE] silent reconnect failed, asking for the device:", e.message);
            }
        }
        let device;
        try {
            device = await navigator.bluetooth.requestDevice({
                filters: LEDGER_BLE_SERVICES.map(uuid => ({ services: [uuid] })),
            });
        } catch (e) {
            if (e.name === "SecurityError") throw new Error("Web Bluetooth access is blocked. Please check browser permissions.");
            throw e;
        }
        if (!device) throw new Error("No Ledger device selected.");

        await bleOpen(device, true);
        if (_bleKnownDevice !== device) {
            _bleKnownDevice?.removeEventListener("gattserverdisconnected", _bleOnDisconnected);
            device.addEventListener("gattserverdisconnected", _bleOnDisconnected);
            _bleKnownDevice = device;
        }
    } else if (mode === TRANSPORT_NATIVE_BLE) {
        // Check if native side already has a connected device (e.g. from a previous iframe session)
        let alreadyConnected = false;
        try { alreadyConnected = await nativeBLE("isConnected"); } catch { }
        if (alreadyConnected) {
            _activeTransport = { name: "connected" };
            _activeTransportMode = TRANSPORT_NATIVE_BLE;
            return;
        }
        // Disconnect any stale native connection before scanning
        try { await nativeBLE("disconnect"); } catch { }
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

/** Close the session. `forget` also drops the remembered Bluetooth device (signOut). */
async function transportDisconnect({ forget = false } = {}) {
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
                try { await _bleNotifyChar.stopNotifications(); } catch { }
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
    } catch { }
    _activeTransport = null;
    _activeTransportMode = null;
    if (forget && _bleKnownDevice) {
        _bleKnownDevice.removeEventListener("gattserverdisconnected", _bleOnDisconnected);
        if (_bleKnownDevice.gatt?.connected) _bleKnownDevice.gatt.disconnect();
        _bleKnownDevice = null;
    }
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
        result[i * 4] = (val >> 24) & 0xff;
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

async function sign(transactionData, path, onProgress) {
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

    const totalChunks = Math.max(1, Math.ceil(allData.length / CHUNK_SIZE));
    let sentChunks = 0;
    if (onProgress) onProgress(0, totalChunks);

    let lastResponse;
    for (let offset = 0; offset < allData.length; offset += CHUNK_SIZE) {
        const chunk = allData.slice(offset, offset + CHUNK_SIZE);
        const isLastChunk = offset + CHUNK_SIZE >= allData.length;
        const response = await ledgerSend(0x80, code, isLastChunk ? 0x80 : 0, networkId, chunk);
        sentChunks++;
        if (onProgress) onProgress(sentChunks, totalChunks);
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
    if (json.error) {
        const err = new Error(json.error.message || "RPC request failed");
        err.cause = json.error.cause;
        // The useful detail ("access key … does not exist while viewing") is in
        // `data`; `message` is usually just "Server error".
        err.data = typeof json.error.data === "string" ? json.error.data : JSON.stringify(json.error.data ?? "");
        throw err;
    }
    if (json.result?.error) {
        const errMsg = typeof json.result.error === "string" ? json.result.error : JSON.stringify(json.result.error);
        throw new Error(errMsg);
    }
    return json.result;
}

async function lookupAccountsByPublicKey(network, publicKey) {
    const baseUrl = network === "testnet"
        ? "https://api.testnet.fastnear.com"
        : "https://api.mainnet.fastnear.com";
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
    if (msg === BLE_LINK_LOST_MESSAGE || isBleLinkError(error)) return "Your Ledger isn't responding over Bluetooth. Unlock it, open the NEAR app, keep it nearby, and try again.";
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

// Chunked base64 encode for Uint8Array. `btoa(String.fromCharCode(...bytes))`
// overflows the call stack for large payloads (e.g. 0.5MB+ signed txs) because
// the spread pushes every byte as a separate argument.
function bytesToBase64(bytes) {
    const CHUNK = 0x8000;
    let binary = "";
    for (let i = 0; i < bytes.length; i += CHUNK) {
        binary += String.fromCharCode.apply(null, bytes.subarray(i, i + CHUNK));
    }
    return btoa(binary);
}

// ============================================================================
// UI Helpers
// ============================================================================

// One stylesheet for every screen: hover (pointer devices only), press, focus,
// disabled and loading states. Screens are re-rendered via root.innerHTML, so
// it lives in <head> where re-renders can't wipe it.
const LEDGER_STYLES = `
/* Inherit by default: the host's #root * rule would otherwise paint every
   element (e.g. a button's label span) white in its own font. */
.lc-screen, .lc-screen * { color:inherit; font-family:inherit; }
:root {
  --lc-bg:#0b0b0c; --lc-surface:#161618; --lc-surface-hover:#1e1e21; --lc-surface-press:#252529;
  --lc-border:#2a2a2e; --lc-border-strong:#46464d; --lc-text:#fafafa; --lc-muted:#a1a1aa; --lc-subtle:#71717a;
  --lc-focus:#60a5fa; --lc-ok:#22c55e; --lc-danger:#f87171;
  --lc-font:-apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,Roboto,sans-serif;
  --lc-mono:ui-monospace,SFMono-Regular,Menlo,monospace;
}
/* Screens size to their content (top-aligned): near-connect's modal shows
   ~615px of this 720px iframe, so actions must never be pinned to the bottom. */
.lc-screen {
  display:flex; flex-direction:column; width:100%; min-height:100%; box-sizing:border-box;
  background:var(--lc-bg); text-align:left; font-family:var(--lc-font); color:var(--lc-text);
  animation:lc-enter 200ms ease-out both;
}
.lc-body { flex:1; padding:28px 24px 24px; display:flex; flex-direction:column; gap:24px; overflow:auto; }
.lc-header { display:flex; flex-direction:column; gap:8px; }
.lc-title-row { display:flex; align-items:center; justify-content:space-between; gap:12px; }
.lc-title { font-size:22px; font-weight:600; line-height:1.25; letter-spacing:-0.01em; color:var(--lc-text); }
.lc-text { margin:0; font-size:15px; line-height:1.5; color:var(--lc-muted); }
.lc-break { overflow-wrap:anywhere; word-break:break-word; }
.lc-em { color:var(--lc-text); font-weight:600; }
.lc-stack { display:flex; flex-direction:column; gap:12px; }
.lc-list { display:flex; flex-direction:column; gap:8px; }
.lc-actions { display:flex; flex-direction:column; gap:8px; padding-top:8px; }
.lc-row { display:flex; align-items:center; gap:12px; }
.lc-grow { flex:1; min-width:0; }
.lc-ellipsis { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; display:block; }
.lc-mono { font-family:var(--lc-mono); }
.lc-note { margin:8px 4px 0; font-size:12px; line-height:1.5; color:var(--lc-muted); }
.lc-field-error { margin-top:8px; font-size:12px; line-height:1.5; color:var(--lc-danger); }

.lc-btn {
  width:100%; min-height:44px; padding:10px 20px; border-radius:12px;
  display:flex; align-items:center; justify-content:center; gap:8px;
  font-family:var(--lc-font); font-size:15px; font-weight:600; cursor:pointer;
  transition:background-color 120ms ease, border-color 120ms ease, opacity 120ms ease, transform 80ms ease;
  -webkit-tap-highlight-color:transparent; touch-action:manipulation;
}
.lc-btn-primary { border:none; background:#fafafa; color:#0b0b0c; }
.lc-btn-secondary { border:1px solid var(--lc-border-strong); background:transparent; color:var(--lc-text); }
.lc-card {
  width:100%; padding:14px; border-radius:14px; border:1px solid var(--lc-border); background:var(--lc-surface);
  cursor:pointer; text-align:left; box-sizing:border-box; font-family:var(--lc-font); color:var(--lc-text);
  transition:background-color 120ms ease, border-color 120ms ease, opacity 120ms ease, transform 80ms ease;
  -webkit-tap-highlight-color:transparent; touch-action:manipulation;
}
.lc-card.is-selected { border-color:var(--lc-text); background:var(--lc-surface-hover); }
.lc-card.is-unavailable { cursor:not-allowed; opacity:0.4; }
.lc-card-title { font-size:15px; font-weight:600; line-height:1.4; color:var(--lc-text); }
.lc-card-desc { font-size:12px; line-height:1.5; color:var(--lc-muted); }
.lc-card-icon {
  width:40px; height:40px; flex-shrink:0; border-radius:10px; background:#232327; color:var(--lc-text);
  display:flex; align-items:center; justify-content:center;
}
.lc-avatar {
  width:32px; height:32px; flex-shrink:0; border-radius:50%; background:#26262a; color:var(--lc-muted);
  display:flex; align-items:center; justify-content:center; font-size:13px; font-weight:600;
}
.lc-badge {
  flex-shrink:0; padding:3px 8px; border-radius:999px; background:#26262a; white-space:nowrap;
  font-size:11px; font-weight:600; letter-spacing:0.02em; color:var(--lc-text);
}
.lc-chevron { flex-shrink:0; color:var(--lc-subtle); transition:transform 120ms ease, color 120ms ease; }
.lc-row-status { display:flex; align-items:center; gap:6px; flex-shrink:0; font-size:12px; color:var(--lc-muted); }
.lc-radio {
  width:18px; height:18px; flex-shrink:0; box-sizing:border-box; border-radius:50%;
  border:1.5px solid var(--lc-border-strong); transition:border-color 120ms ease, box-shadow 120ms ease;
}
.is-selected .lc-radio { border-color:var(--lc-text); box-shadow:inset 0 0 0 4px var(--lc-bg), inset 0 0 0 9px var(--lc-text); }
.lc-icon-btn {
  color:var(--lc-muted); display:inline-flex; align-items:center; justify-content:center; width:36px; height:36px; flex-shrink:0;
  margin:-8px -10px -8px 0; padding:0; border:none; border-radius:10px; background:transparent; cursor:pointer;
  transition:background-color 120ms ease; -webkit-tap-highlight-color:transparent;
}
/* Advanced fallback (manual account ID): a quiet disclosure under a divider,
   so it never competes with the account list as the main call to action. */
.lc-advanced { display:flex; flex-direction:column; gap:8px; padding-top:12px; border-top:1px solid var(--lc-border); }
.lc-advanced-toggle {
  width:100%; display:flex; align-items:center; gap:12px; margin:0 -8px; padding:10px 8px; box-sizing:content-box;
  border:0; border-radius:10px; background:transparent; color:var(--lc-muted); text-align:left; cursor:pointer;
  font-family:var(--lc-font); transition:background-color 120ms ease; -webkit-tap-highlight-color:transparent;
}
.lc-advanced-title { display:block; font-size:12px; line-height:1.4; color:var(--lc-subtle); }
.lc-advanced-sub { display:block; font-size:14px; font-weight:500; line-height:1.4; color:var(--lc-muted); }
.lc-advanced-toggle .lc-chevron { transition:transform 160ms ease, color 120ms ease; }
.lc-advanced-toggle[aria-expanded="true"] .lc-chevron { transform:rotate(90deg); }
.lc-advanced-panel {
  display:flex; flex-direction:column; gap:10px; padding:14px; border-radius:14px;
  background:var(--lc-surface); border:1px solid var(--lc-border);
}
.lc-field-label { font-size:12px; font-weight:600; letter-spacing:0.02em; color:var(--lc-muted); }
.lc-input {
  width:100%; padding:12px 14px; border-radius:12px; border:1px solid var(--lc-border-strong); background:var(--lc-surface); color:var(--lc-text);
  font-family:var(--lc-font); font-size:15px; box-sizing:border-box; outline:none;
  transition:border-color 120ms ease, box-shadow 120ms ease;
}
.lc-input::placeholder { color:var(--lc-subtle); }
.lc-input-suffix { display:flex; align-items:center; gap:8px; }
.lc-input-suffix .lc-input { flex:1; min-width:0; }
.lc-suffix { font-size:15px; font-weight:500; color:var(--lc-muted); }
.lc-input:focus { border-color:#d4d4d8; box-shadow:0 0 0 3px rgba(212,212,216,0.16); }
.lc-input.is-invalid { border-color:var(--lc-danger); }
.lc-input.is-invalid:focus { box-shadow:0 0 0 3px rgba(248,113,113,0.2); }
.lc-input:disabled { opacity:0.6; }

.lc-alert { display:flex; gap:10px; align-items:flex-start; padding:12px 14px; border-radius:12px; border:1px solid; }
.lc-alert p { margin:0; font-size:13px; line-height:1.5; overflow-wrap:anywhere; word-break:break-word; }
.lc-alert-icon { display:flex; padding-top:1px; flex-shrink:0; }
.lc-alert.is-error { background:rgba(248,113,113,0.08); border-color:rgba(248,113,113,0.28); color:#fca5a5; }
.lc-alert.is-info { background:rgba(96,165,250,0.08); border-color:rgba(96,165,250,0.28); color:#bfdbfe; }

.lc-visual { display:flex; flex-direction:column; align-items:center; gap:16px; padding:12px 0; }
.lc-progress { display:flex; flex-direction:column; gap:10px; padding:12px 0; }
.lc-progress-track { width:100%; height:6px; background:#232327; border-radius:999px; overflow:hidden; }
.lc-progress-bar { height:100%; background:var(--lc-text); border-radius:999px; transition:width 200ms ease-out; }

/* Hover only where a real pointer can hover — no sticky hover on touch. */
@media (hover: hover) {
  .lc-btn-primary:not(:disabled):hover { background:#e4e4e7; }
  .lc-btn-secondary:not(:disabled):hover { background:rgba(255,255,255,0.06); border-color:#5c5c66; }
  .lc-card:not(:disabled):not(.is-unavailable):hover { background:var(--lc-surface-hover); border-color:var(--lc-border-strong); }
  .lc-card.is-selected:not(:disabled):hover { border-color:var(--lc-text); }
  .lc-card:not(:disabled):not(.is-unavailable):hover .lc-chevron { color:var(--lc-text); transform:translateX(2px); }
  .lc-card:not(.is-selected):not(:disabled):hover .lc-radio { border-color:#71717a; }
  .lc-icon-btn:hover { background:rgba(255,255,255,0.08); color:var(--lc-text); }
  .lc-advanced-toggle:not(:disabled):hover { background:rgba(255,255,255,0.04); }
  .lc-advanced-toggle:not(:disabled):hover .lc-advanced-sub, .lc-advanced-toggle:not(:disabled):hover .lc-chevron { color:var(--lc-text); }
}
/* Press: immediate feedback on mouse and touch alike. */
.lc-btn:not(:disabled):active, .lc-card:not(:disabled):not(.is-unavailable):active { transform:scale(0.98); }
.lc-btn-primary:not(:disabled):active { background:#d4d4d8; }
.lc-btn-secondary:not(:disabled):active { background:rgba(255,255,255,0.1); }
.lc-card:not(:disabled):not(.is-unavailable):active { background:var(--lc-surface-press); }
.lc-icon-btn:active { background:rgba(255,255,255,0.14); }
.lc-advanced-toggle:not(:disabled):active { background:rgba(255,255,255,0.07); }
/* Keyboard focus. */
.lc-btn:focus-visible, .lc-card:focus-visible, .lc-icon-btn:focus-visible, .lc-advanced-toggle:focus-visible {
  outline:2px solid #60a5fa; outline-offset:2px;
}
/* Disabled vs. busy: a busy control stays fully visible and shows a spinner;
   its disabled siblings fade so the user sees which one is working. */
.lc-btn:disabled, .lc-card:disabled, .lc-advanced-toggle:disabled { cursor:default; }
.lc-btn:disabled:not(.is-loading), .lc-card:disabled:not(.is-loading):not(.is-unavailable), .lc-advanced-toggle:disabled { opacity:0.45; }
.lc-btn.is-loading, .lc-card.is-loading { cursor:progress; }
.lc-card.is-loading { border-color:var(--lc-border-strong); background:var(--lc-surface-hover); }

/* "The app is working" spinner. */
.lc-spinner {
  width:44px; height:44px; box-sizing:border-box; border:3px solid #26262a; border-top-color:var(--lc-text);
  border-radius:50%; animation:lc-spin 1s linear infinite;
}
.lc-spinner-sm {
  display:inline-block; width:14px; height:14px; flex-shrink:0; box-sizing:border-box;
  border:2px solid rgba(250,250,250,0.25); border-top-color:#fafafa; border-radius:50%;
  animation:lc-spin 0.8s linear infinite;
}
.lc-btn-primary .lc-spinner-sm { border-color:rgba(11,11,12,0.2); border-top-color:#0b0b0c; }

/* "Your turn on the Ledger" visual — deliberately different from the spinner. */
.lc-device { position:relative; width:88px; height:88px; display:flex; align-items:center; justify-content:center; }
.lc-device::before {
  content:""; position:absolute; inset:0; border-radius:50%; border:2px solid #fafafa; opacity:0;
  animation:lc-ring 1.8s ease-out infinite;
}
.lc-device-icon { position:relative; width:88px; height:88px; border-radius:50%; background:var(--lc-surface); border:1px solid var(--lc-border); display:flex; align-items:center; justify-content:center; box-sizing:border-box; }
.lc-hint {
  display:flex; align-items:center; justify-content:center; gap:8px; margin:0;
  font-size:13px; line-height:1.4; color:var(--lc-muted); text-align:center;
}
.lc-dot { width:8px; height:8px; flex-shrink:0; border-radius:50%; background:var(--lc-ok); animation:lc-blink 1.4s ease-in-out infinite; }

@keyframes lc-spin { to { transform:rotate(360deg); } }
@keyframes lc-enter { from { transform:translateY(6px); } to { transform:none; } }
@keyframes lc-ring { 0% { transform:scale(0.9); opacity:0.45; } 100% { transform:scale(1.35); opacity:0; } }
@keyframes lc-blink { 0%, 100% { opacity:1; } 50% { opacity:0.3; } }

@media (prefers-reduced-motion: reduce) {
  .lc-btn, .lc-card, .lc-icon-btn, .lc-advanced-toggle, .lc-input, .lc-chevron, .lc-radio { transition:none; }
  .lc-advanced-toggle .lc-chevron { transition:none; }
  .lc-screen { animation:none; }
  .lc-card:hover .lc-chevron { transform:none; }
  .lc-btn:not(:disabled):active, .lc-card:not(:disabled):not(.is-unavailable):active { transform:none; }
  .lc-spinner, .lc-spinner-sm { animation-duration:2.4s; }
  .lc-device::before, .lc-dot { animation:none; }
  .lc-device::before { opacity:0.3; transform:scale(1.15); }
}
`;

const CHEVRON_SVG = `<svg class="lc-chevron" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m9 6 6 6-6 6"/></svg>`;
const USB_ICON_SVG = `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M9 2v4M15 2v4"/><path d="M6 6h12v5a6 6 0 0 1-12 0V6z"/><path d="M12 17v5"/></svg>`;
const BLUETOOTH_ICON_SVG = `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="m7 7 10 10-5 5V2l5 5L7 17"/></svg>`;

// The sandbox host styles `#root * { color: #fff; font-family: … }` (specificity
// 1,0,0), which outranks any class selector — e.g. the primary button's label
// turned white on white. Prefix every style rule with `#root ` so ours win;
// `:root`, @-rules and keyframe steps are left alone.
function scopeToRoot(css) {
    return css
        .replace(/\/\*[\s\S]*?\*\//g, "")
        .replace(/(^|[{}])([^{}@]+?)\{/g, (match, boundary, selectors) => {
            const trimmed = selectors.trim();
            if (!trimmed || trimmed.startsWith(":root") || /^(from|to|\d+%)/.test(trimmed)) return match;
            const scoped = trimmed.split(",").map(sel => `#root ${sel.trim()}`).join(", ");
            return `${boundary}\n${scoped} {`;
        });
}

function ensureLedgerStyles() {
    const root = document.getElementById("root");
    // Our screens are top-aligned and paint their own background; the host's
    // #root centers content on a gradient.
    if (root) {
        root.style.alignItems = "stretch";
        root.style.justifyContent = "flex-start";
        root.style.background = "#0b0b0c";
        // The host sizes #root at 100vw × 100vh; 100vw includes the vertical
        // scrollbar, so a tall screen also got a horizontal scrollbar. Follow
        // the document width and grow with the content instead.
        root.style.width = "100%";
        root.style.height = "auto";
        root.style.minHeight = "100vh";
        document.body.style.overflowX = "hidden";
        // The host paints html/body grey; match our background past the first screen.
        document.documentElement.style.background = "#0b0b0c";
        document.body.style.background = "#0b0b0c";
    }
    if (document.getElementById("lc-styles")) return;
    const style = document.createElement("style");
    style.id = "lc-styles";
    style.textContent = scopeToRoot(LEDGER_STYLES);
    document.head.appendChild(style);
}

function spinnerHtml() {
    return `<div class="lc-visual" role="status" aria-label="Loading"><div class="lc-spinner"></div></div>`;
}

// Waiting for the user to act on the device (approve, review, open the app).
function deviceWaitHtml(hint = "Waiting for confirmation on your Ledger…") {
    return `<div class="lc-visual" role="status">
      <div class="lc-device" aria-hidden="true">
        <div class="lc-device-icon">
          <svg width="52" height="52" viewBox="0 0 52 52" fill="none">
            <rect x="6" y="18" width="40" height="16" rx="5" stroke="#fafafa" stroke-width="2"/>
            <rect x="11" y="22" width="20" height="8" rx="1.5" fill="#262626" stroke="#737373"/>
            <circle cx="39" cy="26" r="3" stroke="#fafafa" stroke-width="2"/>
          </svg>
        </div>
      </div>
      <p class="lc-hint"><span class="lc-dot" aria-hidden="true"></span><span>${hint}</span></p>
    </div>`;
}

// Full-screen status: `spinner` = the app is working, `device` = user's turn on the Ledger.
async function showStatusScreen(title, message, visual = "spinner") {
    ensureLedgerStyles();
    await window.selector.ui.showIframe();
    const root = document.getElementById("root");
    root.style.display = "flex";
    root.innerHTML = `
    <div class="lc-screen">
      <div class="lc-body">
        <div class="lc-header">
          <span class="lc-title">${title}</span>
          <p class="lc-text">${message}</p>
        </div>
        ${visual === "device" ? deviceWaitHtml() : spinnerHtml()}
      </div>
    </div>`;
}

function closeLedgerUI() {
    const root = document.getElementById("root");
    if (root) {
        root.innerHTML = "";
        root.style.display = "none";
    }
    window.selector.ui.hideIframe();
}

// Busy state for the control the user just pressed: disabled, spinner, label.
function setButtonLoading(btn, label) {
    btn.disabled = true;
    btn.classList.add("is-loading");
    btn.setAttribute("aria-busy", "true");
    btn.innerHTML = `<span class="lc-spinner-sm" aria-hidden="true"></span><span>${label}</span>`;
}

function alertBox(message) {
    const guidance = isGuidanceMessage(message);
    const icon = guidance
        ? `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 11v5M12 8h.01"/></svg>`
        : `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3 2 20h20L12 3z"/><path d="M12 10v4M12 17h.01"/></svg>`;
    return `<div class="lc-alert ${guidance ? "is-info" : "is-error"}" role="${guidance ? "status" : "alert"}">
        <span class="lc-alert-icon" aria-hidden="true">${icon}</span>
        <p>${message}</p>
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
        <div class="lc-screen">
          <div class="lc-body">
            <div class="lc-header">
              <span class="lc-title">${title}</span>
              <p class="lc-text">${message}</p>
            </div>
            <div id="ledgerSpinner">${deviceWaitHtml()}</div>
            <div id="ledgerProgress" class="lc-progress" style="display:none;">
              <div class="lc-progress-track">
                <div id="ledgerProgressBar" class="lc-progress-bar" style="width:0%;"></div>
              </div>
              <p id="ledgerProgressText" class="lc-hint"></p>
            </div>
          </div>
        </div>`;
    }

    // Threshold: hide the spinner and show a progress bar only for transfers large
    // enough that users would otherwise think the device hung (1MB tx ≈ 8500 chunks
    // and can take 10–20 min over BLE).
    const PROGRESS_BAR_MIN_CHUNKS = 16;

    function reportProgress(current, total) {
        const bar = document.getElementById("ledgerProgressBar");
        const text = document.getElementById("ledgerProgressText");
        const spinner = document.getElementById("ledgerSpinner");
        const progress = document.getElementById("ledgerProgress");
        if (!bar || !text || !progress) return;
        if (total < PROGRESS_BAR_MIN_CHUNKS) return;
        // Every chunk is on the device: the wait is now the user's review, not
        // the transfer — swap the bar back to the "your turn" device visual.
        if (current >= total) {
            progress.style.display = "none";
            if (spinner) {
                spinner.innerHTML = deviceWaitHtml("All data sent. Review and approve on your Ledger…");
                spinner.style.display = "block";
            }
            return;
        }
        if (spinner) spinner.style.display = "none";
        progress.style.display = "flex";
        const pct = total > 0 ? Math.min(100, Math.floor((current / total) * 100)) : 0;
        bar.style.width = `${pct}%`;
        text.textContent = `Sending to Ledger: ${current} / ${total} chunks (${pct}%)`;
    }

    function renderErrorUI(error) {
        const errorMessage = getLedgerErrorMessage(error);
        root.style.display = "flex";
        root.innerHTML = `
        <div class="lc-screen">
          <div class="lc-body">
            <div class="lc-header">
              <span class="lc-title">${title}</span>
              <p class="lc-text">${message}</p>
            </div>
            <div class="lc-stack">
              ${alertBox(errorMessage)}
              <div class="lc-actions">
                <button id="approvalRetryBtn" class="lc-btn lc-btn-primary">Try Again</button>
                <button id="approvalCancelBtn" class="lc-btn lc-btn-secondary">Cancel</button>
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
        ensureLedgerStyles();
        try {
            // A retry after a lost connection needs a fresh link first.
            if (!(await transportIsConnected())) await reconnectLedger();
        } catch (error) {
            if (error.message !== "User cancelled") {
                renderErrorUI(error);
                const action = await waitForRetryAction();
                if (action === "retry") continue;
            }
            root.innerHTML = "";
            root.style.display = "none";
            window.selector.ui.hideIframe();
            throw new Error("User cancelled");
        }
        renderLoadingUI();
        try {
            const result = await asyncOperation(reportProgress);
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

    ensureLedgerStyles();
    await window.selector.ui.showIframe();
    const root = document.getElementById("root");
    root.style.display = "flex";

    async function directConnect(mode) {
        function renderConnecting(statusMessage, visual = "spinner") {
            root.style.display = "flex";
            root.innerHTML = `
            <div class="lc-screen">
              <div class="lc-body">
                <div class="lc-header">
                  <span class="lc-title">Connect Ledger</span>
                  <p class="lc-text">${statusMessage}</p>
                </div>
                ${visual === "device" ? deviceWaitHtml() : spinnerHtml()}
              </div>
            </div>`;
        }
        function renderRetry(errorMessage) {
            root.style.display = "flex";
            root.innerHTML = `
            <div class="lc-screen">
              <div class="lc-body">
                <div class="lc-header">
                  <div class="lc-title-row">
                    <span class="lc-title">Connect Ledger</span>
                    <button id="cancelBtn" class="lc-icon-btn" aria-label="Close">
                      <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M1 1L13 13M13 1L1 13" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/></svg>
                    </button>
                  </div>
                  <p class="lc-text">
                    Make sure your Ledger device is ${mode === TRANSPORT_NATIVE_BLE || mode === TRANSPORT_WEB_BLE ? "nearby with Bluetooth enabled" : "connected via USB"} and the <strong class="lc-em">NEAR app</strong> is installed.
                  </p>
                </div>
                <div class="lc-stack">
                  ${alertBox(errorMessage)}
                  <button id="retryBtn" class="lc-btn lc-btn-primary">Retry</button>
                  <button id="closeBtn" class="lc-btn lc-btn-secondary">Close</button>
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
                    renderConnecting("Please approve opening the NEAR app on your Ledger device.", "device");
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
            icon: USB_ICON_SVG,
            label: "USB",
            description: "Wired Ledger connection",
            available: usbAvailable,
            lastUsed: usbLastUsed,
            unsupportedReason: usbAvailable ? null : "USB is not supported in this environment. Try Chrome or Edge for WebUSB/WebHID support.",
        },
        {
            id: "bleBtn",
            icon: BLUETOOTH_ICON_SVG,
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
        <div class="lc-screen">
          <div class="lc-body">
            <div class="lc-header">
              <div class="lc-title-row">
                <span class="lc-title">Connect Ledger</span>
                <button id="cancelBtn" class="lc-icon-btn" aria-label="Close">
                  <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M1 1L13 13M13 1L1 13" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/></svg>
                </button>
              </div>
              <p class="lc-text">
                Before continuing, please ensure the <strong class="lc-em">NEAR app</strong> is installed on your Ledger device. You may install it via Ledger Live.
              </p>
            </div>
            <div class="lc-stack">
              ${transportButtons.filter(t => t.available || t.unsupportedReason).map(t => `
              <div>
                <button id="${t.id}" ${!t.available ? `disabled aria-describedby="${t.id}Reason"` : ""} class="lc-card lc-row${t.available ? "" : " is-unavailable"}">
                  <div class="lc-card-icon" aria-hidden="true">${t.icon}</div>
                  <div class="lc-grow">
                    <div class="lc-card-title">${t.label}</div>
                    <div class="lc-card-desc">${t.description}</div>
                  </div>
                  ${t.lastUsed ? `<div class="lc-badge">Last used</div>` : ""}
                  ${t.available ? CHEVRON_SVG : ""}
                </button>
                ${!t.available && t.unsupportedReason ? `<p id="${t.id}Reason" class="lc-note">${t.unsupportedReason}</p>` : ""}
              </div>
              `).join("")}
              ${errorMessage ? alertBox(errorMessage) : ""}
              <div class="lc-actions">
                <button id="closeBtn" class="lc-btn lc-btn-secondary">Close</button>
              </div>
            </div>
          </div>
        </div>`;
    }

    function renderConnectingUI(statusMessage, visual = "spinner") {
        root.style.display = "flex";
        root.innerHTML = `
        <div class="lc-screen">
          <div class="lc-body">
            <div class="lc-header">
              <span class="lc-title">Connect Ledger</span>
              <p class="lc-text">${statusMessage}</p>
            </div>
            ${visual === "device" ? deviceWaitHtml() : spinnerHtml()}
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

                renderConnectingUI("Please approve opening the NEAR app on your Ledger device.", "device");
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
    ensureLedgerStyles();
    await window.selector.ui.showIframe();
    const root = document.getElementById("root");
    root.style.display = "flex";

    const pathOptions = [
        { label: "Account 1", path: "44'/397'/0'/0'/0'" },
        { label: "Account 2", path: "44'/397'/0'/0'/1'" },
        { label: "Account 3", path: "44'/397'/0'/0'/2'" },
    ];

    const customPathPrefix = "44'/397'/0'/0'/";
    const customPathSuffix = "'";
    function buildCustomPath(idx) {
        return `${customPathPrefix}${idx}${customPathSuffix}`;
    }
    function isPresetPath(path) {
        return pathOptions.some(opt => opt.path === path);
    }

    // If incoming path is not one of the presets, treat it as custom and preselect.
    let isCustomSelected = !isPresetPath(currentPath);
    let customIndexValue = "";
    if (isCustomSelected) {
        const m = currentPath.match(/^44'\/397'\/0'\/0'\/(\d+)'$/);
        customIndexValue = m ? m[1] : "";
    }

    function renderUI() {
        root.innerHTML = `
        <div class="lc-screen">
          <div class="lc-body">
            <div class="lc-header">
              <div class="lc-title-row">
                <span class="lc-title">Select Derivation Path</span>
                <button id="cancelBtn" class="lc-icon-btn" aria-label="Close">
                  <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M1 1L13 13M13 1L1 13" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/></svg>
                </button>
              </div>
              <p class="lc-text">Choose account index to use from your Ledger.</p>
            </div>
            <div class="lc-list">
              ${pathOptions.map(opt => `
                <button class="path-btn lc-card lc-row${!isCustomSelected && currentPath === opt.path ? " is-selected" : ""}" data-path="${opt.path}" role="radio" aria-checked="${!isCustomSelected && currentPath === opt.path}">
                  <div class="lc-grow">
                    <div class="lc-card-title">${opt.label}</div>
                    <div class="lc-card-desc lc-mono">${opt.path}</div>
                  </div>
                  <span class="lc-radio" aria-hidden="true"></span>
                </button>
              `).join("")}
              <div id="customWrap" class="custom-wrap lc-card${isCustomSelected ? " is-selected" : ""}">
                <div class="lc-row">
                  <div class="lc-card-title lc-grow">Custom Index</div>
                  <span class="lc-radio" aria-hidden="true"></span>
                </div>
                <div class="lc-card-desc lc-mono" style="display:flex; align-items:center; gap:6px; margin-top:6px;">
                  <span>${customPathPrefix}</span>
                  <input id="customIdx" type="number" min="0" step="1" inputmode="numeric" placeholder="0" value="${customIndexValue}" class="lc-input" style="width:96px; padding:6px 10px; border-radius:8px; background:#0b0b0c; font-size:13px;" aria-label="Custom account index" />
                  <span>${customPathSuffix}</span>
                </div>
                <div id="customError" class="lc-field-error" style="display:none;"></div>
              </div>
            </div>
            <div class="lc-actions">
              <button id="confirmBtn" class="lc-btn lc-btn-primary">Continue</button>
            </div>
          </div>
        </div>`;
    }

    renderUI();

    return new Promise((resolve, reject) => {
        function validateCustom() {
            const errEl = document.getElementById("customError");
            const trimmed = customIndexValue.trim();
            if (trimmed === "") {
                errEl.style.display = "block";
                errEl.textContent = "Enter an account index.";
                return null;
            }
            if (!/^\d+$/.test(trimmed)) {
                errEl.style.display = "block";
                errEl.textContent = "Index must be a non-negative integer.";
                return null;
            }
            // BIP32 hardened child index must fit in 31 bits.
            const idx = Number(trimmed);
            if (!Number.isSafeInteger(idx) || idx > 0x7fffffff) {
                errEl.style.display = "block";
                errEl.textContent = "Index out of range (max 2147483647).";
                return null;
            }
            errEl.style.display = "none";
            errEl.textContent = "";
            return buildCustomPath(idx);
        }

        function setupListeners() {
            const confirmBtn = document.getElementById("confirmBtn");
            const cancelBtn = document.getElementById("cancelBtn");
            const pathBtns = document.querySelectorAll(".path-btn");
            const customWrap = document.getElementById("customWrap");
            const customIdx = document.getElementById("customIdx");

            pathBtns.forEach(btn => {
                btn.addEventListener("click", () => {
                    currentPath = btn.dataset.path;
                    isCustomSelected = false;
                    renderUI();
                    setupListeners();
                });
            });

            // Selecting the custom card (but not its input) marks it active.
            customWrap.addEventListener("click", (e) => {
                if (e.target === customIdx) return;
                if (!isCustomSelected) {
                    isCustomSelected = true;
                    renderUI();
                    setupListeners();
                    document.getElementById("customIdx").focus();
                }
            });

            customIdx.addEventListener("focus", () => {
                if (!isCustomSelected) {
                    isCustomSelected = true;
                    renderUI();
                    setupListeners();
                    document.getElementById("customIdx").focus();
                }
            });

            customIdx.addEventListener("input", (e) => {
                customIndexValue = e.target.value;
                isCustomSelected = true;
                validateCustom();
            });

            confirmBtn.addEventListener("click", () => {
                if (isCustomSelected) {
                    const path = validateCustom();
                    if (!path) return;
                    currentPath = path;
                }
                setButtonLoading(confirmBtn, "Continue");
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
// Account ID Validation
// ============================================================================

// https://nomicon.io/DataStructures/Account#account-id-rules
const ACCOUNT_ID_MIN_LENGTH = 2;
const ACCOUNT_ID_MAX_LENGTH = 64;
const ACCOUNT_ID_PATTERN = /^(([a-z\d]+[-_])*[a-z\d]+\.)*([a-z\d]+[-_])*[a-z\d]+$/;
// New accounts are created as `<name>.near` (mainnet only): one level under
// `near`, since nobody else's sub-accounts can be created on the user's behalf.
const NEW_ACCOUNT_SUFFIX = ".near";
const NEW_ACCOUNT_NETWORK = "mainnet";

function escapeHtml(value) {
    return String(value).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
}

/** Account IDs are lowercase; tolerate stray spaces and capitals in what was typed. */
function normalizeAccountIdInput(value) {
    return (value || "").trim().toLowerCase();
}

/**
 * Why `accountId` can't be a NEAR account ID, as a plain-language message
 * saying what to change (HTML-safe), or null when it's valid.
 */
function accountIdProblem(accountId) {
    if (!accountId) return "Enter your NEAR account ID, for example <strong>alice.near</strong>.";
    if (/\s/.test(accountId)) return "Account IDs can't contain spaces. Remove them and try again.";
    const invalidChars = [...new Set(accountId.replace(/[a-z0-9._-]/g, ""))];
    if (invalidChars.length > 0) {
        const list = invalidChars.map(c => `<strong>${escapeHtml(c)}</strong>`).join(" ");
        return `Account IDs can only use lowercase letters (a–z), digits (0–9), and the separators <strong>.</strong> <strong>-</strong> <strong>_</strong>. Remove ${list} and try again.`;
    }
    if (accountId.length < ACCOUNT_ID_MIN_LENGTH) return "That's too short: account IDs have at least 2 characters.";
    if (accountId.length > ACCOUNT_ID_MAX_LENGTH) {
        return `That's too long: account IDs have at most 64 characters, and this one has ${accountId.length}.`;
    }
    if (!ACCOUNT_ID_PATTERN.test(accountId)) {
        return "A dot, dash or underscore can't be at the start or end of an account ID, or right next to another one (like <strong>..</strong> or <strong>-.</strong>). Fix that and try again.";
    }
    return null;
}

/** 64-hex NEAR implicit or 0x-prefixed Ethereum-style implicit account ID. */
function isImplicitAccountId(accountId) {
    return /^[0-9a-f]{64}$/.test(accountId) || /^0x[0-9a-f]{40}$/.test(accountId);
}

/**
 * The `<name>` for a new `<name>.near` account suggested by what was typed:
 * `alice` for "alice" or "alice.near"; empty when there's no sensible
 * suggestion (sub-accounts such as "qq.alice.near", other suffixes, implicit IDs).
 */
function suggestedNewAccountName(accountId) {
    if (isImplicitAccountId(accountId)) return "";
    if (!accountId.includes(".")) return accountId;
    const labels = accountId.split(".");
    return labels.length === 2 && `.${labels[1]}` === NEW_ACCOUNT_SUFFIX ? labels[0] : "";
}

/**
 * Why `name` can't be used for a new `<name>.near` account (HTML-safe
 * message), or null when it can. A trailing ".near" is ignored.
 */
function newAccountNameProblem(name) {
    if (!name) return "Enter a name for your new account, for example <strong>alice</strong> for alice.near.";
    if (name.includes(".")) {
        return "Use a single name without dots, for example <strong>alice</strong> for alice.near. Accounts like <strong>team.alice.near</strong> can only be created by the owner of alice.near.";
    }
    const problem = accountIdProblem(name + NEW_ACCOUNT_SUFFIX);
    if (problem && name.length + NEW_ACCOUNT_SUFFIX.length > ACCOUNT_ID_MAX_LENGTH) {
        return `That name is too long: it can have at most ${ACCOUNT_ID_MAX_LENGTH - NEW_ACCOUNT_SUFFIX.length} characters before .near.`;
    }
    return problem;
}

/** Strip a typed ".near" so "alice.near" and "alice" both mean alice.near. */
function newAccountNameFromInput(value) {
    const name = normalizeAccountIdInput(value);
    return name.endsWith(NEW_ACCOUNT_SUFFIX) ? name.slice(0, -NEW_ACCOUNT_SUFFIX.length) : name;
}

/** An error whose message is already written for the user (HTML-safe). */
function userError(message, code) {
    const err = new Error(message);
    err.userFacing = true;
    if (code) err.code = code;
    return err;
}

/** What to show for an error from checking an account: its own words, or a retry hint. */
function accountCheckMessage(error, accountId) {
    if (error?.userFacing) return error.message;
    console.log("[Ledger] account check failed:", error);
    return `Couldn't check <strong>${escapeHtml(accountId)}</strong> right now. Check your internet connection and try again.`;
}

// ============================================================================
// Account ID Input UI
// ============================================================================

async function promptForAccountId(foundAccounts = [], implicitAccountId = "", onVerify = null, onCreateAccount = null, hideOnSuccess = true, network = "mainnet") {
    ensureLedgerStyles();
    await window.selector.ui.showIframe();
    const root = document.getElementById("root");
    root.style.display = "flex";

    // Filter implicit account out of found accounts to avoid duplication
    const namedAccounts = foundAccounts.filter(a => a !== implicitAccountId);
    const hasResults = namedAccounts.length > 0;
    // Manual entry is the main path only when there is nothing to pick;
    // otherwise it's an advanced fallback, collapsed behind a disclosure.
    const manualIsPrimary = !hasResults && !implicitAccountId;
    // What was typed survives collapsing and re-opening the disclosure.
    let manualDraft = "";

    function renderUI(errorMessage = null, currentValue = "", showManualInput = manualIsPrimary) {
        const accountButtons = namedAccounts.map((acct, i) => `
            <button class="accountBtn lc-card lc-row" data-account="${acct}">
              <div class="lc-avatar">
                <span>${acct.charAt(0).toUpperCase()}</span>
              </div>
              <span class="lc-grow lc-ellipsis lc-card-title">${acct}</span>
              <span class="lc-row-status">${CHEVRON_SVG}</span>
            </button>`).join("");

        const implicitButton = implicitAccountId ? `
            <button id="useImplicitBtn" class="accountBtn lc-card lc-row" data-account="${implicitAccountId}">
              <div class="lc-avatar">
                <svg width="14" height="14" viewBox="0 0 16 16" fill="none"><path d="M13 14s1 0 1-1-1-4-6-4-6 3-6 4 1 1 1 1h10zM8 7a3 3 0 1 0 0-6 3 3 0 0 0 0 6z" fill="#a3a3a3"/></svg>
              </div>
              <div class="lc-grow" style="display:flex; flex-direction:column; gap:2px;">
                <span class="lc-card-desc">Implicit account</span>
                <span class="lc-card-title lc-ellipsis lc-mono">${implicitAccountId.slice(0, 16)}…${implicitAccountId.slice(-8)}</span>
              </div>
              <span class="lc-row-status">${CHEVRON_SVG}</span>
            </button>` : "";

        const manualField = `
              <label for="accountIdInput" class="lc-field-label">Account ID</label>
              <input type="text" id="accountIdInput" placeholder="example.near" autocapitalize="off" autocorrect="off" spellcheck="false" value="${escapeHtml(currentValue)}"
                class="lc-input${errorMessage ? " is-invalid" : ""}"${errorMessage ? ' aria-invalid="true"' : ""} />
              ${errorMessage ? alertBox(errorMessage) : ""}`;

        let manualInputSection;
        if (manualIsPrimary) {
            manualInputSection = `
            <div class="lc-stack">
              ${manualField}
              <button id="confirmBtn" class="lc-btn lc-btn-primary">Continue</button>
            </div>`;
        } else {
            manualInputSection = `
            ${errorMessage && !showManualInput ? alertBox(errorMessage) : ""}
            <div class="lc-advanced">
              <button id="showManualBtn" class="lc-advanced-toggle" aria-expanded="${showManualInput}" aria-controls="manualPanel">
                <span class="lc-grow">
                  <span class="lc-advanced-title">Don't see your account?</span>
                  <span class="lc-advanced-sub">Enter an account ID manually</span>
                </span>
                ${CHEVRON_SVG}
              </button>
              ${showManualInput ? `
              <div id="manualPanel" class="lc-advanced-panel">
                ${manualField}
                <p class="lc-note" style="margin:0;">Any existing NEAR account this Ledger key has full access to.</p>
                <button id="confirmBtn" class="lc-btn lc-btn-secondary">Use This Account</button>
              </div>` : ""}
            </div>`;
        }

        const description = hasResults
            ? "Select the account you'd like to use with this Ledger device."
            : manualIsPrimary
                ? "Ledger provides your public key. Enter the NEAR account ID that this key has access to."
                : "No named accounts were found for this Ledger key. You can use its implicit account.";

        root.innerHTML = `
        <div class="lc-screen">
          <div class="lc-body">
            <div class="lc-header">
              <div class="lc-title-row">
                <span class="lc-title">Select Account</span>
                <button id="cancelBtn" class="lc-icon-btn" aria-label="Close">
                  <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M1 1L13 13M13 1L1 13" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/></svg>
                </button>
              </div>
              <p class="lc-text">
                ${description}
              </p>
            </div>
            <div class="lc-list">
              ${accountButtons}
              ${implicitButton}
            </div>
            ${manualInputSection}
          </div>
        </div>`;
    }

    renderUI();

    return new Promise((resolve, reject) => {
        function finish(accountId) {
            if (hideOnSuccess) {
                root.innerHTML = "";
                root.style.display = "none";
                window.selector.ui.hideIframe();
            }
            resolve(accountId);
        }

        /**
         * Verify `accountId`; if it doesn't exist, offer to create a new
         * `name.near` (mainnet only). Re-renders the picker with a message
         * saying what to do when that doesn't work out.
         */
        async function tryAccount(accountId, manualValue) {
            const showManual = manualValue !== null;
            if (!onVerify) return finish(accountId);
            try {
                await onVerify(accountId);
                return finish(accountId);
            } catch (error) {
                if (error?.code !== "ACCOUNT_CREATION_REQUIRED") {
                    renderUI(accountCheckMessage(error, accountId), manualValue ?? "", showManual);
                    return setupListeners();
                }
                if (!onCreateAccount || network !== NEW_ACCOUNT_NETWORK) {
                    renderUI(`There's no account called <strong>${escapeHtml(accountId)}</strong> on ${escapeHtml(network)}. Check the spelling, or create the account first, then try again.`, manualValue ?? "", showManual);
                    return setupListeners();
                }
                const created = await promptForCreateAccount(accountId, network, onCreateAccount);
                if (created) return finish(created);
                renderUI(null, manualValue ?? "", showManual);
                setupListeners();
            }
        }

        function selectAccount(accountId) {
            return async () => {
                // Busy state on the picked row (spinner + "Checking…"); the other
                // controls fade out as disabled so it's clear which one is working.
                const btns = document.querySelectorAll(".accountBtn");
                btns.forEach(btn => {
                    btn.disabled = true;
                    if (btn.dataset.account === accountId) {
                        btn.classList.add("is-loading");
                        btn.setAttribute("aria-busy", "true");
                        const status = btn.querySelector(".lc-row-status");
                        if (status) status.innerHTML = `<span class="lc-spinner-sm" aria-hidden="true"></span>Checking…`;
                    }
                });
                const confirmBtn = document.getElementById("confirmBtn");
                if (confirmBtn) confirmBtn.disabled = true;
                const showManualBtn = document.getElementById("showManualBtn");
                if (showManualBtn) showManualBtn.disabled = true;
                await tryAccount(accountId, null);
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
                    const expanded = showManualBtn.getAttribute("aria-expanded") === "true";
                    const input = document.getElementById("accountIdInput");
                    if (input) manualDraft = input.value;
                    renderUI(null, manualDraft, !expanded);
                    setupListeners();
                });
            }

            // Manual input confirm: catch typos before asking the network.
            const confirmBtn = document.getElementById("confirmBtn");
            const input = document.getElementById("accountIdInput");
            if (confirmBtn && input) {
                confirmBtn.addEventListener("click", async () => {
                    const typed = input.value;
                    const accountId = normalizeAccountIdInput(typed);
                    const problem = accountIdProblem(accountId);
                    if (problem) {
                        manualDraft = typed;
                        renderUI(problem, typed, true);
                        return setupListeners();
                    }
                    manualDraft = accountId;
                    setButtonLoading(confirmBtn, "Verifying…");
                    input.value = accountId;
                    input.disabled = true;
                    if (showManualBtn) showManualBtn.disabled = true;
                    document.querySelectorAll(".accountBtn").forEach(btn => { btn.disabled = true; });
                    await tryAccount(accountId, accountId);
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

function rpcErrorText(error) {
    return `${error?.cause?.name ?? ""} ${error?.data ?? ""} ${error?.message ?? ""}`;
}

function isUnknownAccountError(error) {
    return /UNKNOWN_ACCOUNT|UnknownAccount|AccountDoesNotExist|account .* does not exist/i.test(rpcErrorText(error));
}

function isUnknownAccessKeyError(error) {
    return /UNKNOWN_ACCESS_KEY|access key .* does not exist/i.test(rpcErrorText(error));
}

async function accountExists(network, accountId) {
    try {
        await rpcRequest(network, "query", { request_type: "view_account", finality: "final", account_id: accountId });
        return true;
    } catch (error) {
        if (isUnknownAccountError(error)) return false;
        throw error;
    }
}

/**
 * An implicit account id is the hex of its ed25519 public key, so it belongs
 * to that key even before anything has funded it on-chain.
 */
function isImplicitAccountOf(accountId, publicKey) {
    if (!/^[0-9a-f]{64}$/.test(accountId) || !publicKey.startsWith("ed25519:")) return false;
    const bytes = base58Decode(publicKey.slice("ed25519:".length));
    return bytes.length === 32 && Array.from(bytes, b => b.toString(16).padStart(2, "0")).join("") === accountId;
}

function accountCreationRequired(accountId) {
    return userError(
        `There's no account called <strong>${escapeHtml(accountId)}</strong>. Check the spelling and try again.`,
        "ACCOUNT_CREATION_REQUIRED",
    );
}

/**
 * Check that `publicKey` is a full-access key of `accountId`.
 *
 * One view_access_key call in the common case. Only when the key isn't found
 * do we ask view_account why: neard 2.13 (mainnet as of 2026-10) answers
 * "access key does not exist" for a missing account too; 2.14 reports
 * UNKNOWN_ACCOUNT directly, after which this fallback can go.
 *
 * A missing account is fine when it's this key's own implicit account — it
 * comes into existence when first funded. Any other missing account gets the
 * "create account" offer instead of a dead end.
 */
async function verifyAccessKey(network, accountId, publicKey) {
    let accessKey;
    try {
        accessKey = await rpcRequest(network, "query", {
            request_type: "view_access_key",
            finality: "final",
            account_id: accountId,
            public_key: publicKey,
        });
    } catch (error) {
        if (!isUnknownAccountError(error)) {
            if (!isUnknownAccessKeyError(error)) throw error;
            if (await accountExists(network, accountId)) {
                throw userError(`This Ledger isn't linked to <strong>${escapeHtml(accountId)}</strong>. Check that it's the right account, or first add this Ledger's key to it (for example, in the wallet you use for that account), then try again.`);
            }
        }
        if (isImplicitAccountOf(accountId, publicKey)) return true;
        throw accountCreationRequired(accountId);
    }
    if (accessKey.permission !== "FullAccess") {
        throw userError(`This Ledger only has limited access to <strong>${escapeHtml(accountId)}</strong>, so it can't sign for it. Add the Ledger as a full-access key to that account, or choose another account.`);
    }
    return true;
}

// ============================================================================
// Account Creation
// ============================================================================

async function createUserAccountViaApi(accountId, publicKey) {
    const shown = `<strong>${escapeHtml(accountId)}</strong>`;
    let response;
    try {
        response = await fetch("https://api.trezu.app/api/user/create", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ accountId, publicKey }),
        });
    } catch (error) {
        console.log("[Ledger] account creation request failed:", error);
        throw userError("Couldn't reach the account service. Check your internet connection and try again.");
    }
    if (response.ok) return await response.json().catch(() => null);

    // The service answers errors in plain text; keep it for debugging only.
    console.log("[Ledger] account creation failed:", response.status, await response.text().catch(() => ""));
    if (response.status === 409) throw userError(`${shown} is already taken. Try a different name.`);
    if (response.status === 400 || response.status === 422) throw userError(`${shown} can't be created. Try a different name.`);
    if (response.status === 429) throw userError("Too many attempts. Wait a minute, then try again.");
    throw userError(`Couldn't create ${shown} right now. Wait a minute, then try again.`);
}

/**
 * Offer to create a new `<name>.near` account for this Ledger after
 * `requestedId` turned out not to exist. Resolves with the created account ID,
 * or null when the user goes back.
 */
async function promptForCreateAccount(requestedId, network, onCreateAccount) {
    ensureLedgerStyles();
    const root = document.getElementById("root");
    root.style.display = "flex";

    const requested = `<strong class="lc-em">${escapeHtml(requestedId)}</strong>`;
    const suggestion = suggestedNewAccountName(requestedId);
    let intro;
    if (suggestion && requestedId === suggestion + NEW_ACCOUNT_SUFFIX) {
        intro = `There's no account called ${requested} yet. You can create it now, controlled by this Ledger.`;
    } else if (suggestion) {
        intro = `There's no account called ${requested}. New accounts end in <strong>.near</strong>: you can create <strong class="lc-em">${escapeHtml(suggestion + NEW_ACCOUNT_SUFFIX)}</strong>, controlled by this Ledger.`;
    } else if (isImplicitAccountId(requestedId)) {
        intro = `${requested} is a key-based address that doesn't belong to this Ledger, so it can't be used here. You can create a new account with a name instead.`;
    } else if (requestedId.endsWith(NEW_ACCOUNT_SUFFIX)) {
        const parent = requestedId.slice(requestedId.indexOf(".") + 1);
        intro = `There's no account called ${requested}, and it can't be created here: only the owner of <strong>${escapeHtml(parent)}</strong> can create accounts under it. You can create a new <strong>name.near</strong> account instead.`;
    } else {
        intro = `There's no account called ${requested}. New accounts can only end in <strong>.near</strong>: you can create a new <strong>name.near</strong> account instead.`;
    }

    function renderUI(errorMessage = null, value = suggestion) {
        root.innerHTML = `
        <div class="lc-screen">
          <div class="lc-body">
            <div class="lc-header">
              <span class="lc-title">Create a New Account</span>
              <p class="lc-text lc-break">${intro}</p>
            </div>
            <div class="lc-stack">
              <label for="newAccountInput" class="lc-field-label">New account name</label>
              <div class="lc-input-suffix">
                <input type="text" id="newAccountInput" placeholder="alice" autocapitalize="off" autocorrect="off" spellcheck="false"
                  value="${escapeHtml(value)}" class="lc-input${errorMessage ? " is-invalid" : ""}"${errorMessage ? ' aria-invalid="true"' : ""} aria-describedby="newAccountSuffix" />
                <span id="newAccountSuffix" class="lc-suffix">${NEW_ACCOUNT_SUFFIX}</span>
              </div>
              ${errorMessage ? alertBox(errorMessage) : ""}
              <div class="lc-actions">
                <button id="createAccountConfirmBtn" class="lc-btn lc-btn-primary">Create Account</button>
                <button id="createAccountCancelBtn" class="lc-btn lc-btn-secondary">Back</button>
              </div>
            </div>
          </div>
        </div>`;
    }

    renderUI();

    return new Promise((resolve) => {
        function setupListeners() {
            const cancelBtn = document.getElementById("createAccountCancelBtn");
            const createBtn = document.getElementById("createAccountConfirmBtn");
            const input = document.getElementById("newAccountInput");

            cancelBtn.addEventListener("click", () => resolve(null));
            input.addEventListener("keypress", e => { if (e.key === "Enter") createBtn.click(); });
            setTimeout(() => input.focus(), 100);

            createBtn.addEventListener("click", async () => {
                const typed = input.value;
                const name = newAccountNameFromInput(typed);
                const problem = newAccountNameProblem(name);
                if (problem) {
                    renderUI(problem, typed);
                    setupListeners();
                    return;
                }
                const accountId = name + NEW_ACCOUNT_SUFFIX;
                const shown = `<strong>${escapeHtml(accountId)}</strong>`;

                setButtonLoading(createBtn, "Checking name…");
                cancelBtn.disabled = true;
                input.disabled = true;
                try {
                    if (await accountExists(network, accountId)) {
                        renderUI(`${shown} is already taken. If it's yours, go Back and enter it as your account. Otherwise, try a different name.`, typed);
                        setupListeners();
                        return;
                    }
                } catch (error) {
                    console.log("[Ledger] availability check failed:", error);
                    renderUI(`Couldn't check whether ${shown} is available. Check your internet connection and try again.`, typed);
                    setupListeners();
                    return;
                }

                setButtonLoading(createBtn, "Creating account…");
                try {
                    await onCreateAccount(accountId);
                    resolve(accountId);
                } catch (error) {
                    renderUI(error?.userFacing ? error.message : `Couldn't create ${shown}. Try again.`, typed);
                    setupListeners();
                }
            });
        }

        setupListeners();
    });
}

// ============================================================================
// Borsh Serialization Helpers
// ============================================================================

// We manually construct Borsh-serialized payloads to avoid importing
// @near-js/transactions which has assertion code that fails in WKWebView.

function writeU32LE(buf, offset, val) {
    buf[offset] = val & 0xff;
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

/**
 * Re-open the transport used at sign-in and the NEAR app: silently when
 * possible (USB, native BLE, the remembered Bluetooth device), otherwise via
 * a Connect button that provides the user gesture the chooser needs.
 */
async function reconnectLedger() {
    if (await transportIsConnected()) return;

    const storedMode = await window.selector.storage.get(STORAGE_KEY_TRANSPORT_MODE);
    if (!storedMode) throw new Error("Ledger is not connected. Please sign in with Ledger first.");

    ensureLedgerStyles();
    const openingNearApp = () => showStatusScreen(
        "Reconnect Ledger",
        "If asked, approve opening the NEAR app on your Ledger device.",
        "device",
    );

    // WebUSB can reconnect silently via getDevices() — no user gesture needed
    if ((storedMode === TRANSPORT_WEB_USB || storedMode === TRANSPORT_WEB_HID) && isWebUsbSupported()) {
        try {
            const devices = await navigator.usb.getDevices();
            const device = devices.find(d => d.vendorId === LEDGER_VENDOR_ID);
            if (device) {
                await showStatusScreen("Reconnect Ledger", "Connecting to your Ledger device…");
                await usbOpen(device);
                _activeTransport = _usbDevice;
                _activeTransportMode = TRANSPORT_WEB_USB;
                await openingNearApp();
                await openNearApplication();
                await showStatusScreen("Preparing Request", "Getting your request ready for signing…");
                return;
            }
        } catch {
            // WebUSB silent reconnect failed — fall through to manual reconnect
        }
    }

    // Native BLE can reconnect silently — the native side persists the connection across iframe lifecycles
    if (storedMode === TRANSPORT_NATIVE_BLE) {
        try {
            await showStatusScreen("Reconnect Ledger", "Connecting to your Ledger device…");
            await transportConnect(TRANSPORT_NATIVE_BLE);
            await openingNearApp();
            await openNearApplication();
            await showStatusScreen("Preparing Request", "Getting your request ready for signing…");
            return;
        } catch {
            // Native BLE reconnect failed — fall through to manual reconnect
        }
    }

    // Web Bluetooth: the device picked earlier can reconnect without the chooser.
    if (storedMode === TRANSPORT_WEB_BLE && _bleKnownDevice) {
        try {
            await showStatusScreen("Reconnect Ledger", "Reconnecting to your Ledger over Bluetooth…");
            await bleReconnectKnownDevice();
            await openingNearApp();
            await openNearApplication();
            await showStatusScreen("Preparing Request", "Getting your request ready for signing…");
            return;
        } catch {
            // Out of range / asleep / unpaired — fall through to the Connect button.
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
            <div class="lc-screen">
              <div class="lc-body">
                <div class="lc-header">
                  <div class="lc-title-row">
                    <span class="lc-title">Reconnect Ledger</span>
                    <button id="reconnectCancelBtn" class="lc-icon-btn" aria-label="Close">
                      <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M1 1L13 13M13 1L1 13" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/></svg>
                    </button>
                  </div>
                  <p class="lc-text">
                    Please make sure your Ledger device is ${(storedMode === TRANSPORT_WEB_BLE || storedMode === TRANSPORT_NATIVE_BLE) ? "nearby and Bluetooth is enabled" : "connected via USB"}.
                  </p>
                </div>
                ${errorMessage ? alertBox(errorMessage) : ""}
                <div class="lc-actions">
                  <button id="reconnectBtn" class="lc-btn lc-btn-primary">Connect</button>
                </div>
              </div>
            </div>`;

            document.getElementById("reconnectBtn").addEventListener("click", async () => {
                root.innerHTML = `
                <div class="lc-screen">
                  <div class="lc-body">
                    <div class="lc-header">
                      <span class="lc-title">Reconnect Ledger</span>
                      <p class="lc-text">Connecting to your Ledger device…</p>
                    </div>
                    ${spinnerHtml()}
                  </div>
                </div>`;
                try {
                    if (!(await transportIsConnected())) {
                        await transportConnect(storedMode);
                    }
                    await openingNearApp();
                    await openNearApplication();
                    await showStatusScreen("Preparing Request", "Getting your request ready for signing…");
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

class LedgerWallet {
    async getDerivationPath() {
        const path = await window.selector.storage.get(STORAGE_KEY_DERIVATION_PATH);
        return path || DEFAULT_DERIVATION_PATH;
    }

    async _reconnectForSigning() {
        return await reconnectLedger();
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
        await showStatusScreen("Finding Your Accounts", "Looking up NEAR accounts linked to this Ledger…");
        const foundAccounts = await lookupAccountsByPublicKey(network, publicKey);

        const verifyAccount = async (accountId) => {
            await verifyAccessKey(network, accountId, publicKey);
        };

        const createUserAccount = async (accountId) => {
            await createUserAccountViaApi(accountId, publicKey);
        };

        const accountId = await promptForAccountId(foundAccounts, implicitAccountId, verifyAccount, createUserAccount, false, network);

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

        await showStatusScreen("Grant App Access", "Preparing the access key transaction…");
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
            `This transaction adds an access key that allows the application to call ${methodDesc} on <strong class="lc-em">${contractId}</strong> using ${allowanceDesc} on behalf of your account. Your funds remain safe — the key can only be used for gas fees, not transfers.`,
            (onProgress) => sign(txBytes, derivationPath, onProgress),
        );

        // Build signed transaction and broadcast
        const signedTx = new Uint8Array(txBytes.length + 1 + 64);
        signedTx.set(txBytes, 0);
        signedTx[txBytes.length] = 0; // ed25519
        signedTx.set(signature.subarray(0, 64), txBytes.length + 1);

        const base64Tx = bytesToBase64(signedTx);
        await showStatusScreen("Grant App Access", "Submitting the transaction to NEAR…");
        await rpcRequest(network, "broadcast_tx_commit", [base64Tx]);
    }

    async signIn(params) {
        try {
            const { accounts } = await this._performSignInFlow(params);
            closeLedgerUI();
            return accounts;
        } catch (error) {
            closeLedgerUI();
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
                (onProgress) => sign(dataWithPrefix, derivationPath, onProgress),
                true,
            );

            const signatureBase64 = bytesToBase64(signature);

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
        await transportDisconnect({ forget: true });
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
            (onProgress) => sign(txBytes, derivationPath, onProgress),
        );

        // Build signed transaction: tx bytes + signature (enum variant 0 + 64 bytes)
        const signedTx = new Uint8Array(txBytes.length + 1 + 64);
        signedTx.set(txBytes, 0);
        signedTx[txBytes.length] = 0; // ed25519
        signedTx.set(signature.subarray(0, 64), txBytes.length + 1);

        const base64Tx = bytesToBase64(signedTx);
        await showStatusScreen("Sending Transaction", "Signed. Submitting your transaction to NEAR…");
        try {
            return await rpcRequest(network, "broadcast_tx_commit", [base64Tx]);
        } finally {
            closeLedgerUI();
        }
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
        const maxBlockHeight = BigInt(block.header.height) + MAX_BLOCK_HEIGHT_INCREMENT;

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
                (onProgress) => sign(dataWithPrefix, derivationPath, onProgress),
                true,
            );

            // Build SignedDelegate: DelegateAction bytes + Signature (enum 0 + 64 bytes)
            const signedDelegateBytes = new Uint8Array(daBytes.length + 1 + 64);
            signedDelegateBytes.set(daBytes, 0);
            signedDelegateBytes[daBytes.length] = 0; // ed25519
            signedDelegateBytes.set(signature.subarray(0, 64), daBytes.length + 1);

            signedDelegateActions.push(bytesToBase64(signedDelegateBytes));
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
            (onProgress) => sign(dataWithPrefix, derivationPath, onProgress),
            true,
        );

        const signatureBase64 = bytesToBase64(signature);
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
