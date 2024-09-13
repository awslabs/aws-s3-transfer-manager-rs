/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt::{self, Display};
use std::time::Duration;

/// Units of measurement
pub mod unit {
    /// SI byte units
    #[derive(Debug, Clone, Copy)]
    pub enum Bytes {
        /// 1 byte
        Byte,
        /// 1000 bits (125 bytes)
        Kilobit,
        /// 1024 bits (128 bytes)
        Kibibit,
        /// 10<sup>3</sup> bytes.
        Kilobyte,
        /// 2<sup>10</sup> bytes.
        Kibibyte,
        /// 125 * 10<sup>3</sup> bytes.
        Megabit,
        /// 2<sup>17</sup> bytes.
        Mebibit,
        /// 10<sup>6</sup> bytes.
        Megabyte,
        /// 2<sup>20</sup> bytes.
        Mebibyte,
        /// 125 * 10<sup>6</sup> bytes.
        Gigabit,
        /// 2<sup>27</sup> bytes.
        Gibibit,
        /// 10<sup>9</sup> bytes.
        Gigabyte,
        /// 2<sup>30</sup> bytes.
        Gibibyte,
    }

    impl Bytes {
        /// The number of bits represented by this unit
        pub const fn as_bits_u64(&self) -> u64 {
            match self {
                Bytes::Byte => 8,
                Bytes::Kilobit => 1_000,
                Bytes::Kibibit => 1 << 10,
                Bytes::Kilobyte => Bytes::Kilobit.as_bits_u64() << 3,
                Bytes::Kibibyte => Bytes::Kibibit.as_bits_u64() << 3,
                Bytes::Megabit => 1_000_000,
                Bytes::Mebibit => 1 << 20,
                Bytes::Megabyte => Bytes::Megabit.as_bits_u64() << 3,
                Bytes::Mebibyte => Bytes::Mebibit.as_bits_u64() << 3,
                Bytes::Gigabit => 1_000_000_000,
                Bytes::Gibibit => 1 << 30,
                Bytes::Gigabyte => Bytes::Gigabit.as_bits_u64() << 3,
                Bytes::Gibibyte => Bytes::Gibibit.as_bits_u64() << 3,
            }
        }

        /// The number of bytes represented by this unit
        pub const fn as_bytes_u64(&self) -> u64 {
            self.as_bits_u64() >> 3
        }

        pub(crate) const fn as_str(&self) -> &'static str {
            match self {
                Bytes::Byte => "B",
                Bytes::Kilobit => "Kb",
                Bytes::Kibibit => "Kib",
                Bytes::Kilobyte => "KB",
                Bytes::Kibibyte => "KiB",
                Bytes::Megabit => "Mb",
                Bytes::Mebibit => "Mib",
                Bytes::Megabyte => "MB",
                Bytes::Mebibyte => "MiB",
                Bytes::Gigabit => "Gb",
                Bytes::Gibibit => "Gib",
                Bytes::Gigabyte => "GB",
                Bytes::Gibibyte => "GiB",
            }
        }
    }

    impl AsRef<str> for Bytes {
        fn as_ref(&self) -> &str {
            self.as_str()
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
    pub fn new(bytes_transferred: u64, elapsed: Duration) -> Throughput {
        Throughput {
            bytes_transferred,
            elapsed,
        }
    }

    pub(crate) fn new_bytes_per_sec(bytes_transferred: u64) -> Throughput {
        Self::new(bytes_transferred, Duration::from_secs(1))
    }

    /// Convert this throughput into a specific unit per second
    pub fn as_unit_per_sec(&self, unit: unit::Bytes) -> f64 {
        (self.bytes_transferred as f64 / unit.as_bytes_u64() as f64) / self.elapsed.as_secs_f64()
    }

    /// Convert this throughput into bytes / sec
    pub fn as_bytes_per_sec(&self) -> f64 {
        self.as_unit_per_sec(unit::Bytes::Byte)
    }

    /// Returns a type that can be used to format/display this throughput in a particular unit
    pub fn display_as(&self, unit: unit::Bytes) -> ThroughputDisplayContext<'_> {
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

impl fmt::Display for Throughput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let context = ThroughputDisplayContext {
            throughput: self,
            unit: unit::Bytes::Megabit,
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
    pub unit: unit::Bytes,
}

impl<'a> fmt::Display for ThroughputDisplayContext<'a> {
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
    use std::time::Duration;

    use super::{unit::Bytes, Throughput};

    #[test]
    fn test_throughput_display() {
        // default
        assert_eq!(
            "1 Mb/s",
            format!("{}", Throughput::new(125_000, Duration::from_secs(1)))
        );

        let t = Throughput::new(1_000_000, Duration::from_secs(1));

        // explicit
        assert_eq!("1000000 B/s", format!("{}", t.display_as(Bytes::Byte)));
        assert_eq!("8000 Kb/s", format!("{}", t.display_as(Bytes::Kilobit)));
        assert_eq!("1000 KB/s", format!("{}", t.display_as(Bytes::Kilobyte)));
        assert_eq!("7812.5 Kib/s", format!("{}", t.display_as(Bytes::Kibibit)));
        assert_eq!(
            "976.5625 KiB/s",
            format!("{}", t.display_as(Bytes::Kibibyte))
        );
        assert_eq!("8 Mb/s", format!("{}", t.display_as(Bytes::Megabit)));
        assert_eq!("1 MB/s", format!("{}", t.display_as(Bytes::Megabyte)));
        assert_eq!(
            "7.629 Mib/s",
            format!("{:.3}", t.display_as(Bytes::Mebibit))
        );
        assert_eq!(
            "0.954 MiB/s",
            format!("{:.3}", t.display_as(Bytes::Mebibyte))
        );
        assert_eq!("0.008 Gb/s", format!("{}", t.display_as(Bytes::Gigabit)));
        assert_eq!("0.001 GB/s", format!("{}", t.display_as(Bytes::Gigabyte)));
        assert_eq!(
            "0.00745 Gib/s",
            format!("{:.5}", t.display_as(Bytes::Gibibit))
        );
        assert_eq!(
            "0.008 GiB/s",
            format!("{:.5}", t.display_as(Bytes::Gibibyte))
        );
    }
}
