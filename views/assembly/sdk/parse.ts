import { JSON } from "json-as";
import { parseNumber } from "./number";

@inline
function isHexDigit(code: u32): bool {
  return (code >= 48 && code <= 57) || (code >= 65 && code <= 70) || (code >= 97 && code <= 102);
}

@inline
function hexValue(code: u32): u32 {
  if (code <= 57) return code - 48;
  if (code <= 70) return code - 55;
  return code - 87;
}

/**
 * Single-pass strict RFC 8259 JSON parser to eager Value containers.
 * Validates syntax, nesting limits, UTF-16 scalar strings, and finite IEEE-754
 * numbers while decoding object keys canonically in one pass.
 */
export class Parser {
  cursor: i32 = 0;
  maxDepth: i32 = 0;
  error: string = "";
  private base: usize;
  private len: i32;

  constructor(private text: string) {
    this.base = changetype<usize>(text);
    this.len = text.length;
  }

  whitespace(): void {
    const len = this.len;
    const base = this.base;
    let pos = this.cursor;
    while (pos < len) {
      const code = load<u16>(base + ((<usize>pos) << 1));
      if (code != 32 && code != 9 && code != 10 && code != 13) break;
      pos++;
    }
    this.cursor = pos;
  }

  string(): string {
    const len = this.len;
    const base = this.base;
    if (this.cursor >= len || load<u16>(base + ((<usize>this.cursor) << 1)) != 34) {
      this.error = "Request is not valid JSON";
      return "";
    }
    const contentStart = ++this.cursor;
    let pos = contentStart;
    while (pos < len) {
      const code = load<u16>(base + ((<usize>pos) << 1));
      if (code == 34) {
        this.cursor = pos + 1;
        // Input was UTF-8 validated, so an unescaped slice cannot contain lone surrogates.
        return this.text.slice(contentStart, pos);
      }
      if (code == 92) return this.escapedString(contentStart, pos);
      if (code < 32) {
        this.error = "Request is not valid JSON";
        return "";
      }
      pos++;
    }
    this.error = "Request is not valid JSON";
    return "";
  }

  private escapedString(contentStart: i32, firstEscape: i32): string {
    const len = this.len;
    const base = this.base;
    let pos = firstEscape;
    let outLen = firstEscape - contentStart;
    let pendingHigh = false;
    let badSurrogate = false;
    let contentEnd = -1;

    while (pos < len) {
      const code = load<u16>(base + ((<usize>pos) << 1));
      if (code == 34) {
        contentEnd = pos;
        pos++;
        break;
      }
      if (code < 32) {
        this.error = "Request is not valid JSON";
        return "";
      }
      if (code == 92) {
        pos++;
        if (pos >= len) {
          this.error = "Request is not valid JSON";
          return "";
        }
        const esc = load<u16>(base + ((<usize>pos) << 1));
        if (esc == 117) {
          if (pos + 4 >= len) {
            this.error = "Request is not valid JSON";
            return "";
          }
          const h0 = <u32>load<u16>(base + ((<usize>(pos + 1)) << 1));
          const h1 = <u32>load<u16>(base + ((<usize>(pos + 2)) << 1));
          const h2 = <u32>load<u16>(base + ((<usize>(pos + 3)) << 1));
          const h3 = <u32>load<u16>(base + ((<usize>(pos + 4)) << 1));
          if (!isHexDigit(h0) || !isHexDigit(h1) || !isHexDigit(h2) || !isHexDigit(h3)) {
            this.error = "Request is not valid JSON";
            return "";
          }
          const unit = (hexValue(h0) << 12) | (hexValue(h1) << 8) | (hexValue(h2) << 4) | hexValue(h3);
          if (unit >= 0xd800 && unit <= 0xdbff) {
            if (pendingHigh) badSurrogate = true;
            pendingHigh = true;
          } else if (unit >= 0xdc00 && unit <= 0xdfff) {
            if (pendingHigh) pendingHigh = false;
            else badSurrogate = true;
          } else if (pendingHigh) {
            badSurrogate = true;
            pendingHigh = false;
          }
          pos += 5;
          outLen++;
          continue;
        }
        if (
          esc != 34 && esc != 92 && esc != 47 &&
          esc != 98 && esc != 102 && esc != 110 && esc != 114 && esc != 116
        ) {
          this.error = "Request is not valid JSON";
          return "";
        }
        if (pendingHigh) {
          badSurrogate = true;
          pendingHigh = false;
        }
        pos++;
        outLen++;
        continue;
      }
      if (pendingHigh) {
        badSurrogate = true;
        pendingHigh = false;
      }
      pos++;
      outLen++;
    }

    if (contentEnd < 0) {
      this.error = "Request is not valid JSON";
      return "";
    }
    this.cursor = pos;
    if (pendingHigh || badSurrogate) {
      this.error = "JSON strings must contain valid Unicode scalar values";
      return "";
    }
    if (outLen == 0) return "";
    const out = changetype<string>(__new((<usize>outLen) << 1, idof<string>()));
    const outBase = changetype<usize>(out);
    const prefixLen = firstEscape - contentStart;
    if (prefixLen > 0) {
      memory.copy(outBase, base + ((<usize>contentStart) << 1), (<usize>prefixLen) << 1);
    }
    let writeIdx = prefixLen;
    let readIdx = firstEscape;
    while (readIdx < contentEnd) {
      const code = load<u16>(base + ((<usize>readIdx) << 1));
      if (code == 92) {
        readIdx++;
        const esc = load<u16>(base + ((<usize>readIdx) << 1));
        let decoded: u16;
        if (esc == 117) {
          const h0 = <u32>load<u16>(base + ((<usize>(readIdx + 1)) << 1));
          const h1 = <u32>load<u16>(base + ((<usize>(readIdx + 2)) << 1));
          const h2 = <u32>load<u16>(base + ((<usize>(readIdx + 3)) << 1));
          const h3 = <u32>load<u16>(base + ((<usize>(readIdx + 4)) << 1));
          decoded = <u16>((hexValue(h0) << 12) | (hexValue(h1) << 8) | (hexValue(h2) << 4) | hexValue(h3));
          readIdx += 5;
        } else {
          if (esc == 110) decoded = 10;
          else if (esc == 114) decoded = 13;
          else if (esc == 116) decoded = 9;
          else if (esc == 98) decoded = 8;
          else if (esc == 102) decoded = 12;
          else decoded = esc;
          readIdx++;
        }
        store<u16>(outBase + ((<usize>writeIdx) << 1), decoded);
        writeIdx++;
      } else {
        store<u16>(outBase + ((<usize>writeIdx) << 1), code);
        writeIdx++;
        readIdx++;
      }
    }
    return out;
  }

  private number(): JSON.Value {
    const len = this.len;
    const base = this.base;
    const start = this.cursor;
    let pos = start;
    if (pos < len && load<u16>(base + ((<usize>pos) << 1)) == 45) pos++;
    if (pos >= len) {
      this.error = "Request is not valid JSON";
      return JSON.Value.empty();
    }
    let code = load<u16>(base + ((<usize>pos) << 1));
    if (code == 48) {
      pos++;
      if (pos < len) {
        const next = load<u16>(base + ((<usize>pos) << 1));
        if (next >= 48 && next <= 57) {
          this.error = "Request is not valid JSON";
          return JSON.Value.empty();
        }
      }
    } else if (code >= 49 && code <= 57) {
      pos++;
      while (pos < len) {
        code = load<u16>(base + ((<usize>pos) << 1));
        if (code < 48 || code > 57) break;
        pos++;
      }
    } else {
      this.error = "Request is not valid JSON";
      return JSON.Value.empty();
    }
    if (pos < len && load<u16>(base + ((<usize>pos) << 1)) == 46) {
      pos++;
      if (pos >= len) {
        this.error = "Request is not valid JSON";
        return JSON.Value.empty();
      }
      code = load<u16>(base + ((<usize>pos) << 1));
      if (code < 48 || code > 57) {
        this.error = "Request is not valid JSON";
        return JSON.Value.empty();
      }
      pos++;
      while (pos < len) {
        code = load<u16>(base + ((<usize>pos) << 1));
        if (code < 48 || code > 57) break;
        pos++;
      }
    }
    if (pos < len) {
      code = load<u16>(base + ((<usize>pos) << 1));
      if (code == 101 || code == 69) {
        pos++;
        if (pos < len) {
          code = load<u16>(base + ((<usize>pos) << 1));
          if (code == 43 || code == 45) pos++;
        }
        if (pos >= len) {
          this.error = "Request is not valid JSON";
          return JSON.Value.empty();
        }
        code = load<u16>(base + ((<usize>pos) << 1));
        if (code < 48 || code > 57) {
          this.error = "Request is not valid JSON";
          return JSON.Value.empty();
        }
        pos++;
        while (pos < len) {
          code = load<u16>(base + ((<usize>pos) << 1));
          if (code < 48 || code > 57) break;
          pos++;
        }
      }
    }
    this.cursor = pos;
    const value = parseNumber(this.text, start, pos);
    if (!isFinite(value)) {
      this.error = "Request must contain finite JSON numbers";
      return JSON.Value.empty();
    }
    return JSON.Value.from<f64>(value);
  }

  value(depth: i32 = 0): JSON.Value {
    // Up to four transport levels (request, groups array, group object, values array)
    // can surround a 64-level datum in reduce_groups.
    if (depth > 68) {
      this.error = "Request exceeds 68 JSON transport nesting levels";
      return JSON.Value.empty();
    }
    if (depth > this.maxDepth) this.maxDepth = depth;
    this.whitespace();
    if (this.cursor >= this.len) {
      this.error = "Request is not valid JSON";
      return JSON.Value.empty();
    }
    const base = this.base;
    const first = load<u16>(base + ((<usize>this.cursor) << 1));
    if (first == 34) {
      const str = this.string();
      if (this.error.length) return JSON.Value.empty();
      return JSON.Value.from(str);
    }
    if (first == 123) {
      const object = new JSON.Obj();
      this.cursor++;
      this.whitespace();
      if (this.cursor >= this.len) {
        this.error = "Request is not valid JSON";
        return JSON.Value.empty();
      }
      if (load<u16>(base + ((<usize>this.cursor) << 1)) == 125) {
        this.cursor++;
        return JSON.Value.from(object);
      }
      while (true) {
        const key = this.string();
        if (this.error.length) return JSON.Value.empty();
        if (key.length > 65535) {
          this.error = "Object member names exceed 65535 UTF-16 code units";
          return JSON.Value.empty();
        }
        this.whitespace();
        if (this.cursor >= this.len || load<u16>(base + ((<usize>this.cursor) << 1)) != 58) {
          this.error = "Request is not valid JSON";
          return JSON.Value.empty();
        }
        this.cursor++;
        const val = this.value(depth + 1);
        if (this.error.length) return JSON.Value.empty();
        object.set<JSON.Value>(key, val);
        this.whitespace();
        if (this.cursor >= this.len) {
          this.error = "Request is not valid JSON";
          return JSON.Value.empty();
        }
        const sep = load<u16>(base + ((<usize>this.cursor) << 1));
        if (sep == 44) {
          this.cursor++;
          this.whitespace();
        } else if (sep == 125) {
          this.cursor++;
          return JSON.Value.from(object);
        } else {
          this.error = "Request is not valid JSON";
          return JSON.Value.empty();
        }
      }
    }
    if (first == 91) {
      const array = new JSON.Arr();
      this.cursor++;
      this.whitespace();
      if (this.cursor >= this.len) {
        this.error = "Request is not valid JSON";
        return JSON.Value.empty();
      }
      if (load<u16>(base + ((<usize>this.cursor) << 1)) == 93) {
        this.cursor++;
        return JSON.Value.from(array);
      }
      while (true) {
        const elem = this.value(depth + 1);
        if (this.error.length) return JSON.Value.empty();
        array.push(elem);
        this.whitespace();
        if (this.cursor >= this.len) {
          this.error = "Request is not valid JSON";
          return JSON.Value.empty();
        }
        const sep = load<u16>(base + ((<usize>this.cursor) << 1));
        if (sep == 44) {
          this.cursor++;
        } else if (sep == 93) {
          this.cursor++;
          return JSON.Value.from(array);
        } else {
          this.error = "Request is not valid JSON";
          return JSON.Value.empty();
        }
      }
    }
    if (first == 110) {
      if (
        this.cursor + 4 <= this.len &&
        load<u16>(base + ((<usize>(this.cursor + 1)) << 1)) == 117 &&
        load<u16>(base + ((<usize>(this.cursor + 2)) << 1)) == 108 &&
        load<u16>(base + ((<usize>(this.cursor + 3)) << 1)) == 108
      ) {
        this.cursor += 4;
        return JSON.Value.empty();
      }
      this.error = "Request is not valid JSON";
      return JSON.Value.empty();
    }
    if (first == 116) {
      if (
        this.cursor + 4 <= this.len &&
        load<u16>(base + ((<usize>(this.cursor + 1)) << 1)) == 114 &&
        load<u16>(base + ((<usize>(this.cursor + 2)) << 1)) == 117 &&
        load<u16>(base + ((<usize>(this.cursor + 3)) << 1)) == 101
      ) {
        this.cursor += 4;
        return JSON.Value.from<bool>(true);
      }
      this.error = "Request is not valid JSON";
      return JSON.Value.empty();
    }
    if (first == 102) {
      if (
        this.cursor + 5 <= this.len &&
        load<u16>(base + ((<usize>(this.cursor + 1)) << 1)) == 97 &&
        load<u16>(base + ((<usize>(this.cursor + 2)) << 1)) == 108 &&
        load<u16>(base + ((<usize>(this.cursor + 3)) << 1)) == 115 &&
        load<u16>(base + ((<usize>(this.cursor + 4)) << 1)) == 101
      ) {
        this.cursor += 5;
        return JSON.Value.from<bool>(false);
      }
      this.error = "Request is not valid JSON";
      return JSON.Value.empty();
    }
    if (first == 45 || (first >= 48 && first <= 57)) {
      return this.number();
    }
    this.error = "Request is not valid JSON";
    return JSON.Value.empty();
  }
}
