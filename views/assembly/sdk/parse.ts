import { JSON } from "json-as";
import { parseNumber } from "./number";

/**
 * Adapt validated JSON to eager Value containers. json-as 1.6.2's dynamic
 * object decoder retains raw escaped member names; reading string tokens with
 * its scalar codec gives canonical keys and makes state round trips stable.
 * Syntax is checked by validateJSON before this adapter is called.
 */
export class Parser {
  cursor: i32 = 0;
  error: string = "";
  constructor(private text: string) {}

  whitespace(): void {
    while (this.cursor < this.text.length) {
      const code = this.text.charCodeAt(this.cursor);
      if (code != 32 && code != 9 && code != 10 && code != 13) break;
      this.cursor++;
    }
  }

  string(): string {
    const start = this.cursor++;
    while (this.cursor < this.text.length) {
      const code = this.text.charCodeAt(this.cursor++);
      if (code == 92) this.cursor++;
      else if (code == 34) break;
    }
    return JSON.parse<string>(this.text.slice(start, this.cursor));
  }

  value(depth: i32 = 0): JSON.Value {
    if (depth > 64) {
      this.error = "Request exceeds 64 nesting levels";
      return JSON.Value.empty();
    }
    this.whitespace();
    const first = this.text.charCodeAt(this.cursor);
    if (first == 34) return JSON.Value.from(this.string());
    if (first == 123) {
      const object = new JSON.Obj();
      this.cursor++;
      this.whitespace();
      while (this.text.charCodeAt(this.cursor) != 125) {
        const key = this.string();
        if (key.length > 65535) {
          this.error = "Object member names exceed 65535 UTF-16 code units";
          return JSON.Value.empty();
        }
        this.whitespace();
        this.cursor++; // colon
        object.set<JSON.Value>(key, this.value(depth + 1));
        if (this.error.length) return JSON.Value.empty();
        this.whitespace();
        if (this.text.charCodeAt(this.cursor) != 44) break;
        this.cursor++;
        this.whitespace();
      }
      this.cursor++;
      return JSON.Value.from(object);
    }
    if (first == 91) {
      const array = new JSON.Arr();
      this.cursor++;
      this.whitespace();
      while (this.text.charCodeAt(this.cursor) != 93) {
        array.push(this.value(depth + 1));
        if (this.error.length) return JSON.Value.empty();
        this.whitespace();
        if (this.text.charCodeAt(this.cursor) != 44) break;
        this.cursor++;
      }
      this.cursor++;
      return JSON.Value.from(array);
    }
    if (first == 110) { this.cursor += 4; return JSON.Value.empty(); }
    if (first == 116) { this.cursor += 4; return JSON.Value.from<bool>(true); }
    if (first == 102) { this.cursor += 5; return JSON.Value.from<bool>(false); }
    const start = this.cursor;
    while (this.cursor < this.text.length) {
      const code = this.text.charCodeAt(this.cursor);
      if ((code >= 48 && code <= 57) || code == 45 || code == 43 || code == 46 || code == 69 || code == 101) this.cursor++;
      else break;
    }
    return JSON.Value.from<f64>(parseNumber(this.text.slice(start, this.cursor)));
  }
}
