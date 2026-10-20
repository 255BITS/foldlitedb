/**
 * Exact decimal -> IEEE-754 binary64 conversion for syntax-validated JSON.
 * AssemblyScript 0.28.20's f64.parse loses decimal powers for some exponents
 * and double-rounds other values. Integer rational division below rounds once,
 * to nearest with ties to even, including subnormals and overflow boundaries.
 * This implementation is original SDK code; it does not patch dependencies.
 */
class Natural {
  words: u32[] = [0];

  multiply(factor: u32, digit: u32 = 0): void {
    let carry: u64 = digit;
    for (let i = 0; i < this.words.length; i++) {
      const product = <u64>this.words[i] * factor + carry;
      this.words[i] = <u32>product;
      carry = product >> 32;
    }
    if (carry != 0) this.words.push(<u32>carry);
  }

  bits(): i32 {
    const length = this.words.length;
    return (length - 1) * 32 + 32 - clz(this.words[length - 1]);
  }

  shiftLeft(count: i32): void {
    if (count <= 0 || this.bits() == 0) return;
    const whole = count >> 5;
    const remainder = count & 31;
    const shifted = new Array<u32>(this.words.length + whole + (remainder == 0 ? 0 : 1));
    let carry: u64 = 0;
    for (let i = 0; i < this.words.length; i++) {
      const value = (<u64>this.words[i] << remainder) | carry;
      shifted[i + whole] = <u32>value;
      carry = value >> 32;
    }
    if (remainder != 0) shifted[shifted.length - 1] = <u32>carry;
    this.words = shifted;
    this.trim();
  }

  shiftRight(): void {
    let carry: u32 = 0;
    for (let i = this.words.length - 1; i >= 0; i--) {
      const value = this.words[i];
      this.words[i] = (value >> 1) | carry;
      carry = value << 31;
    }
    this.trim();
  }

  trim(): void {
    while (this.words.length > 1 && this.words[this.words.length - 1] == 0) this.words.pop();
  }

  copy(): Natural {
    const value = new Natural();
    value.words = this.words.slice();
    return value;
  }

  compare(other: Natural): i32 {
    if (this.words.length != other.words.length) return this.words.length < other.words.length ? -1 : 1;
    for (let i = this.words.length - 1; i >= 0; i--) {
      if (this.words[i] != other.words[i]) return this.words[i] < other.words[i] ? -1 : 1;
    }
    return 0;
  }

  subtract(other: Natural): void {
    let borrow: u64 = 0;
    for (let i = 0; i < this.words.length; i++) {
      const left: u64 = this.words[i];
      const right = <u64>(i < other.words.length ? other.words[i] : 0) + borrow;
      this.words[i] = <u32>(left - right);
      borrow = <u64>(left < right);
    }
    this.trim();
  }
}

const POWERS10: usize = memory.data<f64>([
  1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11,
  1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
]);

export function parseNumber(text: string): f64 {
  const negative = text.charCodeAt(0) == 45;
  let cursor = negative ? 1 : 0;
  const numerator = new Natural();
  let fractionalDigits = 0, significantDigits = 0;
  let pointed = false, started = false;
  for (; cursor < text.length; cursor++) {
    const code = text.charCodeAt(cursor);
    if (code == 46) { pointed = true; continue; }
    if (code == 101 || code == 69) break;
    const digit = code - 48;
    if (pointed) fractionalDigits++;
    if (!started && digit == 0) continue;
    started = true;
    significantDigits++;
    numerator.multiply(10, digit);
  }
  let exponent = 0;
  if (cursor < text.length) {
    cursor++;
    const exponentNegative = text.charCodeAt(cursor) == 45;
    if (exponentNegative || text.charCodeAt(cursor) == 43) cursor++;
    for (; cursor < text.length; cursor++) exponent = min(1000000, exponent * 10 + text.charCodeAt(cursor) - 48);
    if (exponentNegative) exponent = -exponent;
  }
  exponent -= fractionalDigits;
  if (!started) return negative ? -0.0 : 0.0;
  if (numerator.words.length <= 2) {
    const integer = <u64>numerator.words[0] | (numerator.words.length == 2 ? <u64>numerator.words[1] << 32 : 0);
    // Integer casts round once. Powers 10^0..10^22 and integers <=2^53 are
    // exact doubles, so one multiply/divide also rounds exactly once.
    if (exponent == 0) return negative ? -<f64>integer : <f64>integer;
    if (integer <= 9007199254740992 && abs(exponent) <= 22) {
      const power = load<f64>(POWERS10 + (<usize>abs(exponent) << 3));
      const value = exponent > 0 ? <f64>integer * power : <f64>integer / power;
      return negative ? -value : value;
    }
  }
  const decimalOrder = significantDigits + exponent - 1;
  if (decimalOrder > 309) return negative ? -Infinity : Infinity;
  if (decimalOrder < -325) return negative ? -0.0 : 0.0;

  const denominator = new Natural();
  denominator.words[0] = 1;
  const power = exponent >= 0 ? numerator : denominator;
  for (let i = 0, count = abs(exponent); i < count; i++) power.multiply(5);

  // Find floor(log2(numerator / denominator * 2^exponent)).
  let binaryExponent = numerator.bits() - denominator.bits() + exponent;
  const left = numerator.copy(), right = denominator.copy();
  const correctionShift = exponent - binaryExponent;
  if (correctionShift >= 0) left.shiftLeft(correctionShift);
  else right.shiftLeft(-correctionShift);
  if (left.compare(right) < 0) binaryExponent--;
  if (binaryExponent > 1023) return negative ? -Infinity : Infinity;
  if (binaryExponent < -1075) return negative ? -0.0 : 0.0;

  // Normal values have 53 significant bits. Subnormals round to multiples of
  // 2^-1074. The quotient below therefore fits in exactly representable u64.
  const quantum = max(binaryExponent - 52, -1074);
  const shift = exponent - quantum;
  if (shift >= 0) numerator.shiftLeft(shift);
  else denominator.shiftLeft(-shift);
  const divisor = denominator.copy();
  const quotientBits = numerator.bits() - divisor.bits();
  let quotient: u64 = 0;
  if (quotientBits >= 0) {
    divisor.shiftLeft(quotientBits);
    for (let bit = quotientBits; bit >= 0; bit--) {
      if (numerator.compare(divisor) >= 0) {
        numerator.subtract(divisor);
        quotient |= <u64>1 << bit;
      }
      divisor.shiftRight();
    }
  }
  numerator.shiftLeft(1);
  const halfway = numerator.compare(denominator);
  if (halfway > 0 || (halfway == 0 && (quotient & 1) != 0)) quotient++;
  const value = NativeMath.scalbn(<f64>quotient, quantum);
  return negative ? -value : value;
}
