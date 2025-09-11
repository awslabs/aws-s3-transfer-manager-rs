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

#[cfg(test)]
mod tests {
    use super::ByteUnit;
    use crate::metrics::unit::ByteCountDisplayContext;
    use std::str::FromStr;

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
