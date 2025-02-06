/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt::{self, Display};
use std::ops;
use std::time::Duration;

/// Units of measurement
pub mod unit {
    use std::{fmt, str::FromStr};

    /// SI byte units
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ByteUnit {
        /// 1 byte
        Byte,
        /// 1000 bits (125 bytes)
        Kilobit,
        /// 2<sup>10</sup> bytes.
        Kibibyte,
        /// 125 * 10<sup>3</sup> bytes.
        Megabit,
        /// 2<sup>20</sup> bytes.
        Mebibyte,
        /// 125 * 10<sup>6</sup> bytes.
        Gigabit,
        /// 2<sup>30</sup> bytes.
        Gibibyte,
    }

    impl ByteUnit {
        /// Convert some number of bytes into this unit as an `f64`
        pub fn convert(&self, bytes: u64) -> f64 {
            bytes as f64 / self.as_bytes_u64() as f64
        }

        /// Figure out the best unit to display the given number of bytes in
        /// and return a [`ByteCountDisplayContext`] with the appropriate units set
        pub fn display(total_bytes: u64) -> ByteCountDisplayContext {
            let units = &[ByteUnit::Gibibyte, ByteUnit::Mebibyte, ByteUnit::Kibibyte];
            let mut unit = ByteUnit::Byte;
            for u in units {
                if total_bytes >= u.as_bytes_u64() {
                    unit = *u;
                    break;
                }
            }

            ByteCountDisplayContext::new(total_bytes, unit)
        }

        /// The number of bits represented by this unit
        pub const fn as_bits_u64(&self) -> u64 {
            self.as_bits_usize() as u64
        }

        /// The number of bits represented by this unit
        pub const fn as_bits_usize(&self) -> usize {
            match self {
                ByteUnit::Byte => 8,
                ByteUnit::Kilobit => 1_000,
                ByteUnit::Kibibyte => 1 << 13,
                ByteUnit::Megabit => 1_000_000,
                ByteUnit::Mebibyte => 1 << 23,
                ByteUnit::Gigabit => 1_000_000_000,
                ByteUnit::Gibibyte => 1 << 33,
            }
        }

        /// The number of bytes represented by this unit
        pub const fn as_bytes_u64(&self) -> u64 {
            self.as_bytes_usize() as u64
        }

        /// The number of bytes represented by this unit
        pub const fn as_bytes_usize(&self) -> usize {
            self.as_bits_usize() >> 3
        }

        pub(crate) const fn as_str(&self) -> &'static str {
            match self {
                ByteUnit::Byte => "B",
                ByteUnit::Kilobit => "Kb",
                ByteUnit::Kibibyte => "KiB",
                ByteUnit::Megabit => "Mb",
                ByteUnit::Mebibyte => "MiB",
                ByteUnit::Gigabit => "Gb",
                ByteUnit::Gibibyte => "GiB",
            }
        }
    }

    impl AsRef<str> for ByteUnit {
        fn as_ref(&self) -> &str {
            self.as_str()
        }
    }

    impl FromStr for ByteUnit {
        type Err = crate::error::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let unit = match s {
                "B" => ByteUnit::Byte,
                "Kb" => ByteUnit::Kilobit,
                "KiB" => ByteUnit::Kibibyte,
                "Mb" => ByteUnit::Megabit,
                "MiB" => ByteUnit::Mebibyte,
                "Gb" => ByteUnit::Gigabit,
                "GiB" => ByteUnit::Gibibyte,
                _ => {
                    return Err(crate::error::invalid_input(format!(
                        "unknown byte unit '{}'",
                        s
                    )))
                }
            };

            Ok(unit)
        }
    }

    /// Display context to format a value representing number of bytres in a particular unit
    #[derive(Debug)]
    pub struct ByteCountDisplayContext {
        /// The throughput measurment to display
        pub total_bytes: u64,
        /// The precise unit to display the throughput as
        pub unit: ByteUnit,
    }

    impl ByteCountDisplayContext {
        /// Create a new display context for the number of bytes in a specific unit
        pub fn new(total_bytes: u64, unit: ByteUnit) -> Self {
            Self { total_bytes, unit }
        }
    }

    impl fmt::Display for ByteCountDisplayContext {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            if self.total_bytes % self.unit.as_bytes_u64() == 0 {
                let converted = self.total_bytes / self.unit.as_bytes_u64();
                return write!(f, "{converted} {}", self.unit.as_str());
            }
            let precision = f.precision().unwrap_or(3);
            write!(
                f,
                "{1:.*} {2:}",
                precision,
                self.unit.convert(self.total_bytes),
                self.unit.as_str()
            )
        }
    }
}

/// Measured bytes transferred over some duration
#[derive(Debug, Clone, Copy)]
pub struct Throughput {
    bytes_transferred: u64,
    elapsed: Duration,
}

impl Throughput {
    /// Create a new throughput measurement with the given bytes read and time elapsed
    pub const fn new(bytes_transferred: u64, elapsed: Duration) -> Throughput {
        Throughput {
            bytes_transferred,
            elapsed,
        }
    }

    /// Create a new throughput measurement assuming a one second duration
    ///
    /// This is convenience for:
    ///
    /// ```
    /// use std::time::Duration;
    /// use aws_s3_transfer_manager::metrics::{unit, Throughput};
    /// let bytes_transferred = 5 * unit::ByteUnit::Mebibyte.as_bytes_u64();
    /// assert_eq!(
    ///     Throughput::new(bytes_transferred, Duration::from_secs(1)),
    ///     Throughput::new_bytes_per_sec(bytes_transferred)
    /// );
    /// ```
    pub const fn new_bytes_per_sec(bytes_transferred: u64) -> Throughput {
        Self::new(bytes_transferred, Duration::from_secs(1))
    }

    /// Convert this throughput into a specific unit per second
    pub fn as_unit_per_sec(&self, unit: unit::ByteUnit) -> f64 {
        (self.bytes_transferred as f64 / unit.as_bytes_u64() as f64) / self.elapsed.as_secs_f64()
    }

    /// Convert this throughput into bytes / sec
    pub fn as_bytes_per_sec(&self) -> f64 {
        self.as_unit_per_sec(unit::ByteUnit::Byte)
    }

    /// Total bytes transferred
    pub const fn bytes_transferred(&self) -> u64 {
        self.bytes_transferred
    }

    /// Returns a type that can be used to format/display this throughput in a particular unit
    pub fn display_as(&self, unit: unit::ByteUnit) -> ThroughputDisplayContext<'_> {
        ThroughputDisplayContext {
            throughput: self,
            unit,
        }
    }
}

impl PartialEq for Throughput {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes_per_sec() == other.as_bytes_per_sec()
    }
}

impl PartialOrd for Throughput {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_bytes_per_sec()
            .partial_cmp(&other.as_bytes_per_sec())
    }
}

/// Add two throughputs
impl ops::Add for Throughput {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let bps = self.as_bytes_per_sec() + rhs.as_bytes_per_sec();

        Throughput::new_bytes_per_sec(bps.round() as u64)
    }
}

/// Subtract throughput
impl ops::Sub for Throughput {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        let bps = self.as_bytes_per_sec() - rhs.as_bytes_per_sec();
        Throughput::new_bytes_per_sec(bps.round() as u64)
    }
}

/// Multiply throughput by a scalar
impl ops::Mul<u64> for Throughput {
    type Output = Self;

    fn mul(self, rhs: u64) -> Self::Output {
        Throughput::new(self.bytes_transferred * rhs, self.elapsed)
    }
}

/// Divide throughput by a scalar
impl ops::Div<u64> for Throughput {
    type Output = Self;

    fn div(self, rhs: u64) -> Self::Output {
        Throughput::new(self.bytes_transferred / rhs, self.elapsed)
    }
}

impl fmt::Display for Throughput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let context = ThroughputDisplayContext {
            throughput: self,
            unit: unit::ByteUnit::Megabit,
        };
        Display::fmt(&context, f)
    }
}

/// Display context to format throughput in a particular unit
#[derive(Debug)]
pub struct ThroughputDisplayContext<'a> {
    /// The throughput measurment to display
    pub throughput: &'a Throughput,
    /// The precise unit to display the throughput as
    pub unit: unit::ByteUnit,
}

impl fmt::Display for ThroughputDisplayContext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(precision) = f.precision() {
            write!(
                f,
                "{1:.*} {2:}/s",
                precision,
                self.throughput.as_unit_per_sec(self.unit),
                self.unit.as_str()
            )
        } else {
            write!(
                f,
                "{} {}/s",
                self.throughput.as_unit_per_sec(self.unit),
                self.unit.as_str()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use crate::metrics::unit::ByteCountDisplayContext;

    use super::{unit::ByteUnit, Throughput};

    #[test]
    fn test_throughput_display() {
        // default
        assert_eq!(
            "1 Mb/s",
            format!("{}", Throughput::new(125_000, Duration::from_secs(1)))
        );

        let t = Throughput::new(1_000_000, Duration::from_secs(1));

        // explicit
        assert_eq!("1000000 B/s", format!("{}", t.display_as(ByteUnit::Byte)));
        assert_eq!("8000 Kb/s", format!("{}", t.display_as(ByteUnit::Kilobit)));
        assert_eq!(
            "976.5625 KiB/s",
            format!("{}", t.display_as(ByteUnit::Kibibyte))
        );
        assert_eq!("8 Mb/s", format!("{}", t.display_as(ByteUnit::Megabit)));
        assert_eq!(
            "0.954 MiB/s",
            format!("{:.3}", t.display_as(ByteUnit::Mebibyte))
        );
        assert_eq!("0.008 Gb/s", format!("{}", t.display_as(ByteUnit::Gigabit)));
        assert_eq!(
            "0.00093 GiB/s",
            format!("{:.5}", t.display_as(ByteUnit::Gibibyte))
        );
    }

    #[test]
    fn test_from_str() {
        let units = &[
            ByteUnit::Byte,
            ByteUnit::Kilobit,
            ByteUnit::Kibibyte,
            ByteUnit::Megabit,
            ByteUnit::Mebibyte,
            ByteUnit::Gigabit,
            ByteUnit::Gibibyte,
        ];

        for u in units {
            let u2 = ByteUnit::from_str(u.as_str()).unwrap();
            assert_eq!(*u, u2);
        }

        assert!(ByteUnit::from_str("kb").is_err());
    }

    #[test]
    fn test_ops() {
        let t = Throughput::new_bytes_per_sec(10 * ByteUnit::Mebibyte.as_bytes_u64());
        let t2 = Throughput::new_bytes_per_sec(5 * ByteUnit::Mebibyte.as_bytes_u64());

        assert_eq!(
            t + t2,
            Throughput::new_bytes_per_sec(t.bytes_transferred() + t2.bytes_transferred())
        );
        assert_eq!(t - t2, t2);
        assert_eq!(
            t * 2,
            Throughput::new_bytes_per_sec(t.bytes_transferred() * 2)
        );
        assert_eq!(t / 2, t2);
    }

    #[test]
    fn test_byte_display_context() {
        assert_eq!("1 KiB", format!("{}", ByteUnit::display(1024)));
        assert_eq!("1 MiB", format!("{}", ByteUnit::display(1024 * 1024)));
        assert_eq!(
            "1 GiB",
            format!("{}", ByteUnit::display(1024 * 1024 * 1024))
        );

        assert_eq!("727 B", format!("{}", ByteUnit::display(727)));
        assert_eq!(
            "0.710 KiB",
            format!("{}", ByteCountDisplayContext::new(727, ByteUnit::Kibibyte))
        );
        assert_eq!("3.420 KiB", format!("{}", ByteUnit::display(3502)));
        assert_eq!("3.41992 KiB", format!("{:.5}", ByteUnit::display(3502)));

        assert_eq!("7.201 MiB", format!("{}", ByteUnit::display(7550498)));
        assert_eq!(
            "0.007 GiB",
            format!(
                "{}",
                ByteCountDisplayContext::new(7550498, ByteUnit::Gibibyte)
            )
        );

        assert_eq!("1.016 GiB", format!("{}", ByteUnit::display(1091242563)));
        assert_eq!(
            "1040.690 MiB",
            format!(
                "{}",
                ByteCountDisplayContext::new(1091242563, ByteUnit::Mebibyte)
            )
        );
    }
}
