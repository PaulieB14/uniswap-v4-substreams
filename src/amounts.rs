//! Decimal scaling and the sign convention for token amounts.
//!
//! # The gap this closes
//!
//! A V4 `Swap` log carries `amount0`/`amount1` as raw `int128`s in the token's
//! own base units, and they are **swapper-centric**: negative means the swapper
//! paid that token in, positive means the swapper received it out. Two things
//! are therefore wrong with reading those integers as "the trade":
//!
//! 1. **The sign is backwards from every other V4 tool.** The deployed subgraph
//!    stores the POOL's point of view — `convertTokenToDecimal(amount0.neg(),
//!    …)` — so a row copied verbatim from the chain compares sign-flipped
//!    against `uniswap-v4-base-3`, against the Uniswap UI, and against the v3
//!    subgraphs everyone's existing SQL was written for.
//! 2. **The magnitude is in base units.** `347553` is not 347,553 USDC, it is
//!    0.347553 USDC.
//!
//! Both are fixed here, and the raw integers are **kept alongside** rather than
//! replaced. That is not hedging: the raw value is exactly what the chain
//! emitted and is the only lossless form, while the scaled value is a division
//! that can only ever be as trustworthy as the `decimals()` call behind it.
//! Throwing away the exact number to keep the derived one would be the wrong
//! way round.
//!
//! # Why string arithmetic and not `BigDecimal`
//!
//! Scaling by a power of ten is a decimal-point move, so doing it as text is
//! exact by construction, allocation-light, and — the part that matters for a
//! Substreams module — cannot emit scientific notation. The output goes
//! straight into a Postgres `NUMERIC` column as a SQL literal, and `1E-18`
//! is not a literal Postgres will take on every version/locale path, whereas
//! `0.000000000000000001` always is. `BigDecimal::to_string()` gives no such
//! guarantee across crate versions.
//!
//! # When it is allowed to run at all
//!
//! Only when the token's `decimals()` was genuinely **measured** over RPC.
//! `tokens.rs` falls back to 18 for a token whose `decimals()` cannot be read,
//! and scaling a 6-decimal token by 18 is wrong by a factor of a trillion — a
//! plausible, quiet, catastrophic number. Callers gate on `decimals_measured`
//! and emit nothing when it is false; see `enrich.rs`.

/// Scale a raw integer amount by `10^-decimals`, optionally flipping its sign.
///
/// `negate` is the swapper-centric → pool-centric flip. It is a parameter and
/// not baked in because the two callers genuinely differ: `Swap.amount0` is a
/// signed swapper delta and must be negated, while `Donate.amount0` is an
/// unsigned uint256 that only ever flows into the pool and is already
/// pool-centric.
///
/// Returns `None` — never a substitute value — when `raw` is not a decimal
/// integer. The only reachable case against this package's own producers is the
/// proto3 default empty string; a caller that gets `None` must omit the field,
/// because any stand-in would be indistinguishable from a real amount.
///
/// The result is normalised: no leading zeros beyond the units digit, no
/// trailing fractional zeros, no `-0`. That makes it directly comparable
/// against the subgraph's `BigDecimal` rendering, which normalises the same way.
pub fn scale(raw: &str, decimals: u32, negate: bool) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    let (mut neg, digits) = match raw.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, raw.strip_prefix('+').unwrap_or(raw)),
    };
    // Reject anything that is not pure digits rather than salvaging a prefix.
    // A partially-parsed amount is a wrong amount.
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if negate {
        neg = !neg;
    }

    // Strip leading zeros first so the pad length below is computed against the
    // significant digits; "000123" and "123" must scale identically.
    let significant = match digits.trim_start_matches('0') {
        "" => "0",
        rest => rest,
    };

    let d = decimals as usize;
    let out = if d == 0 {
        significant.to_string()
    } else {
        // Left-pad so there is at least one integer digit: 347553 at 18
        // decimals has to become 0.000000000000347553, not .000000000000347553.
        let padded = if significant.len() <= d {
            let mut s = String::with_capacity(d + 1);
            for _ in 0..(d + 1 - significant.len()) {
                s.push('0');
            }
            s.push_str(significant);
            s
        } else {
            significant.to_string()
        };
        let split = padded.len() - d;
        let (int_part, frac_part) = padded.split_at(split);
        let frac = frac_part.trim_end_matches('0');
        if frac.is_empty() {
            int_part.to_string()
        } else {
            format!("{}.{}", int_part, frac)
        }
    };

    // Zero has no sign. "-0" is a valid NUMERIC literal but it reads as a
    // direction that was never traded, and it breaks string equality against
    // the subgraph's "0".
    if out.bytes().all(|b| b == b'0' || b == b'.') {
        return Some("0".to_string());
    }

    Some(if neg { format!("-{}", out) } else { out })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact ground-truth row this whole module exists to reproduce.
    ///
    /// Deployed subgraph Qmbsc6XQWbiv4DfLVfaNciScqYLyDWUYjWzrFBbzzmRsMB, swap
    /// `0xacffee88f97b2f4284f6992ad74733667c0131e4fb7459a00cf7718d1e5bd5f5-104`
    /// (Base block 25401667, the WETH/USDC anchor pool's first swap) reports
    /// amount0 "0.000106502155157829" and amount1 "-0.347553". The chain emitted
    /// the opposite signs, in base units.
    #[test]
    fn matches_the_deployed_subgraph_on_a_real_swap() {
        assert_eq!(
            scale("-106502155157829", 18, true).unwrap(),
            "0.000106502155157829"
        );
        assert_eq!(scale("347553", 6, true).unwrap(), "-0.347553");
    }

    /// The negation is the whole point of `negate`, so pin both directions on
    /// the same input.
    #[test]
    fn negate_flips_and_only_flips_the_sign() {
        assert_eq!(scale("-106502155157829", 18, false).unwrap(), "-0.000106502155157829");
        assert_eq!(scale("106502155157829", 18, true).unwrap(), "-0.000106502155157829");
        assert_eq!(scale("106502155157829", 18, false).unwrap(), "0.000106502155157829");
    }

    /// Donations are unsigned and already pool-centric — the caller passes
    /// `negate = false`, and a positive number must stay positive.
    #[test]
    fn donate_convention_does_not_flip() {
        assert_eq!(scale("1500000", 6, false).unwrap(), "1.5");
    }

    #[test]
    fn zero_decimals_is_the_identity() {
        assert_eq!(scale("12345", 0, false).unwrap(), "12345");
        assert_eq!(scale("12345", 0, true).unwrap(), "-12345");
    }

    /// Zero carries no sign in either direction — otherwise a zero-amount leg
    /// renders "-0" and stops comparing equal to the subgraph's "0".
    #[test]
    fn zero_is_unsigned_however_it_is_spelled() {
        assert_eq!(scale("0", 18, true).unwrap(), "0");
        assert_eq!(scale("-0", 18, true).unwrap(), "0");
        assert_eq!(scale("0000", 6, false).unwrap(), "0");
    }

    /// Padding is computed on the significant digits, so a zero-padded input
    /// must not shift the decimal point.
    #[test]
    fn leading_zeros_do_not_move_the_point() {
        assert_eq!(scale("000347553", 6, false).unwrap(), "0.347553");
        assert_eq!(scale("347553", 6, false).unwrap(), "0.347553");
    }

    #[test]
    fn trailing_fractional_zeros_are_trimmed_but_integers_are_not() {
        assert_eq!(scale("1000000", 6, false).unwrap(), "1");
        assert_eq!(scale("1000000000", 6, false).unwrap(), "1000");
        assert_eq!(scale("1200000", 6, false).unwrap(), "1.2");
    }

    /// A full uint256 must survive: this is the reason the implementation is
    /// string-based rather than going through any fixed-width integer.
    #[test]
    fn a_full_uint256_survives_intact() {
        let max_u256 =
            "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        let got = scale(max_u256, 18, false).unwrap();
        assert_eq!(
            got,
            "115792089237316195423570985008687907853269984665640564039.457584007913129639935"
        );
        // and the digits are byte-identical to the input, point aside
        assert_eq!(got.replace('.', ""), max_u256);
    }

    /// No scientific notation, ever — the output is a Postgres NUMERIC literal.
    #[test]
    fn output_is_always_plain_decimal_notation() {
        for d in [1u32, 6, 18, 24, 36, 77] {
            let s = scale("1", d, false).unwrap();
            assert!(
                !s.contains('e') && !s.contains('E'),
                "decimals {} produced {}",
                d,
                s
            );
            assert!(s.starts_with("0."), "decimals {} produced {}", d, s);
            assert_eq!(s.len(), 2 + d as usize);
        }
    }

    /// Garbage in gets `None`, never a stand-in. An empty string is the
    /// reachable case (proto3 default); the rest guard a mapper regression.
    #[test]
    fn non_integers_are_rejected_rather_than_salvaged() {
        assert!(scale("", 18, false).is_none());
        assert!(scale("-", 18, false).is_none());
        assert!(scale("1.5", 18, false).is_none());
        assert!(scale("0x1f", 18, false).is_none());
        assert!(scale("12a", 18, false).is_none());
        assert!(scale("1 000", 18, false).is_none());
    }

    /// Round-trip against the raw integer: scaling is a decimal-point move and
    /// must lose nothing, which is the property that lets the schema keep both
    /// columns and treat the raw one as authoritative.
    #[test]
    fn scaling_is_lossless_digit_for_digit() {
        for (raw, dec) in [("-106502155157829", 18u32), ("347553", 6), ("1", 36)] {
            let scaled = scale(raw, dec, false).unwrap();
            let digits: String = scaled.chars().filter(|c| c.is_ascii_digit()).collect();
            let expect: String = raw.chars().filter(|c| c.is_ascii_digit()).collect();
            assert!(
                digits.trim_start_matches('0').ends_with(expect.trim_start_matches('0')),
                "{} at {} decimals lost digits: {}",
                raw,
                dec,
                scaled
            );
        }
    }
}
