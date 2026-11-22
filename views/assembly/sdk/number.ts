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

const MAX_SIGNIFICANT_DIGITS: i32 = 768;
const MAX_U64_DIGITS: i32 = 19; // 10^19 - 1 < 2^64 - 1

export function parseNumber(text: string, start: i32 = 0, end: i32 = text.length): f64 {
  const base = changetype<usize>(text);
  const negative = load<u16>(base + ((<usize>start) << 1)) == 45;
  let cursor = negative ? start + 1 : start;
  let mantissa: u64 = 0;
  let numerator: Natural | null = null;
  let fractionalDigits = 0, significantDigits = 0, droppedIntegerDigits = 0;
  let pointed = false, started = false, stickyNonZero = false;
  for (; cursor < end; cursor++) {
    const code = load<u16>(base + ((<usize>cursor) << 1));
    if (code == 46) { pointed = true; continue; }
    if (code == 101 || code == 69) break;
    const digit = <u32>(code - 48);
    if (!started && digit == 0) {
      if (pointed) fractionalDigits++;
      continue;
    }
    started = true;
    if (significantDigits < MAX_U64_DIGITS) {
      if (pointed) fractionalDigits++;
      significantDigits++;
      mantissa = mantissa * 10 + digit;
    } else if (significantDigits < MAX_SIGNIFICANT_DIGITS) {
      if (pointed) fractionalDigits++;
      if (numerator == null) {
        numerator = new Natural();
        numerator.words[0] = <u32>mantissa;
        const high = <u32>(mantissa >> 32);
        if (high != 0) numerator.words.push(high);
      }
      significantDigits++;
      numerator.multiply(10, digit);
    } else {
      if (!pointed) droppedIntegerDigits++;
      if (digit != 0) stickyNonZero = true;
    }
  }
  let exponent = 0;
  if (cursor < end) {
    cursor++;
    const signCode = load<u16>(base + ((<usize>cursor) << 1));
    const exponentNegative = signCode == 45;
    if (exponentNegative || signCode == 43) cursor++;
    for (; cursor < end; cursor++) {
      exponent = min(10000000, exponent * 10 + <i32>load<u16>(base + ((<usize>cursor) << 1)) - 48);
    }
    if (exponentNegative) exponent = -exponent;
  }
  exponent += droppedIntegerDigits - fractionalDigits;
  if (!started) return negative ? -0.0 : 0.0;
  if (numerator == null) {
    // Integer casts round once. Powers 10^0..10^22 and integers <=2^53 are
    // exact doubles, so one multiply/divide also rounds exactly once.
    if (exponent == 0) return negative ? -<f64>mantissa : <f64>mantissa;
    if (mantissa <= 9007199254740992) {
      if (abs(exponent) <= 22) {
        const power = load<f64>(POWERS10 + (<usize>abs(exponent) << 3));
        const value = exponent > 0 ? <f64>mantissa * power : <f64>mantissa / power;
        return negative ? -value : value;
      }
      if (exponent > 22 && exponent <= 37) {
        let m = mantissa;
        let e = exponent;
        while (e > 22 && m <= 900719925474099) {
          m *= 10;
          e--;
        }
        if (e <= 22) {
          const value = <f64>m * load<f64>(POWERS10 + (<usize>e << 3));
          return negative ? -value : value;
        }
      }
    }
  }
  if (stickyNonZero) {
    numerator!.multiply(10, 1);
    significantDigits++;
    exponent--;
  }
  const decimalOrder = significantDigits + exponent - 1;
  if (decimalOrder > 309) return negative ? -Infinity : Infinity;
  if (decimalOrder < -325) return negative ? -0.0 : 0.0;

  if (numerator == null) {
    numerator = new Natural();
    numerator.words[0] = <u32>mantissa;
    const high = <u32>(mantissa >> 32);
    if (high != 0) numerator.words.push(high);
  }

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
