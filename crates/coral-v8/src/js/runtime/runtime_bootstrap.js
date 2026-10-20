'use strict';
(() => {
  const forbiddenGlobals = globalThis.__coralForbiddenAmbientGlobals;
  Reflect.deleteProperty(globalThis, '__coralForbiddenAmbientGlobals');

  for (const name of forbiddenGlobals) {
    Reflect.deleteProperty(globalThis, name);
    if (typeof globalThis[name] !== 'undefined') {
      throw new Error(`failed to remove forbidden global: ${name}`);
    }
  }
})();

// Base64 is pure text conversion, so Programs and the Saved Functions linked
// into them get HTML's `atob` and `btoa`. Both convert between base64 and a
// binary string, which holds one byte in each character.
(() => {
  const NativeError = Error;
  const NativeTypeError = TypeError;
  const fromCharCode = String.fromCharCode;

  const ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
  const ALPHABET_CODES = [];
  const SEXTETS = Array.from({ length: 128 }, () => -1);
  for (let index = 0; index < ALPHABET.length; index += 1) {
    ALPHABET_CODES.push(ALPHABET.charCodeAt(index));
    SEXTETS[ALPHABET.charCodeAt(index)] = index;
  }
  const ASCII_WHITESPACE = /[\t\n\f\r ]/gu;
  // Each spread into `fromCharCode` stays far below V8's argument limit.
  const CHUNK_LENGTH = 8192;

  class InvalidCharacterError extends NativeError {}
  Object.defineProperty(InvalidCharacterError.prototype, 'name', {
    value: 'InvalidCharacterError',
  });
  Object.freeze(InvalidCharacterError.prototype);
  Object.freeze(InvalidCharacterError);

  // Builds a large result in chunks, never as one huge array or as millions
  // of one-character strings.
  const codeUnitWriter = () => {
    let text = '';
    const pending = [];
    return {
      push(codeUnit) {
        pending.push(codeUnit);
        if (pending.length === CHUNK_LENGTH) {
          text += fromCharCode(...pending);
          pending.length = 0;
        }
      },
      finish() {
        return text + fromCharCode(...pending);
      },
    };
  };

  // WHATWG Infra's forgiving-base64 decode.
  function atob(data) {
    if (arguments.length === 0) throw new NativeTypeError('atob requires 1 argument');
    let encoded = `${data}`.replace(ASCII_WHITESPACE, '');
    if (encoded.length % 4 === 0) {
      if (encoded.endsWith('==')) encoded = encoded.slice(0, -2);
      else if (encoded.endsWith('=')) encoded = encoded.slice(0, -1);
    }
    if (encoded.length % 4 === 1) {
      throw new InvalidCharacterError('atob input is not valid base64');
    }
    const output = codeUnitWriter();
    let buffer = 0;
    let bits = 0;
    for (let index = 0; index < encoded.length; index += 1) {
      const code = encoded.charCodeAt(index);
      const sextet = code < 128 ? SEXTETS[code] : -1;
      if (sextet === -1) {
        throw new InvalidCharacterError('atob input is not valid base64');
      }
      buffer = (buffer << 6) | sextet;
      bits += 6;
      if (bits >= 8) {
        bits -= 8;
        output.push(buffer >> bits);
        buffer &= (1 << bits) - 1;
      }
    }
    return output.finish();
  }

  function btoa(data) {
    if (arguments.length === 0) throw new NativeTypeError('btoa requires 1 argument');
    const binary = `${data}`;
    const byteAt = (index) => {
      const code = binary.charCodeAt(index);
      if (code > 0xff) {
        throw new InvalidCharacterError('btoa input has a character above U+00FF');
      }
      return code;
    };
    const output = codeUnitWriter();
    for (let index = 0; index < binary.length; index += 3) {
      const remaining = binary.length - index;
      const group =
        (byteAt(index) << 16) |
        (remaining > 1 ? byteAt(index + 1) << 8 : 0) |
        (remaining > 2 ? byteAt(index + 2) : 0);
      output.push(ALPHABET_CODES[group >> 18]);
      output.push(ALPHABET_CODES[(group >> 12) & 63]);
      output.push(remaining > 1 ? ALPHABET_CODES[(group >> 6) & 63] : 0x3d);
      output.push(remaining > 2 ? ALPHABET_CODES[group & 63] : 0x3d);
    }
    return output.finish();
  }

  for (const [name, value] of [
    ['atob', atob],
    ['btoa', btoa],
  ]) {
    Object.defineProperty(globalThis, name, {
      value,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
})();
