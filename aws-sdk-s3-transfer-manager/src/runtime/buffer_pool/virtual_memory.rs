/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Stable virtual-address reservations with whole-range access transitions.

use std::fmt;
use std::io;
use std::mem::MaybeUninit;
use std::num::NonZeroUsize;
use std::ptr::NonNull;

#[cfg(test)]
use test_support::InjectedFailures;

/// Operation reported by a virtual-memory failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum VirtualMemoryOperation {
    /// Query the runtime page size.
    QueryPageSize,
    /// Reserve an inaccessible address range.
    Reserve,
    /// Make a reserved range readable and writable.
    Prepare,
    /// Make a prepared range inaccessible.
    Deactivate,
    /// Release backing from an inaccessible range.
    Discard,
    /// Release the address reservation.
    Release,
}

impl fmt::Display for VirtualMemoryOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::QueryPageSize => "page-size query",
            Self::Reserve => "reservation",
            Self::Prepare => "preparation",
            Self::Deactivate => "deactivation",
            Self::Discard => "discard",
            Self::Release => "release",
        })
    }
}

/// Failure to establish or transition a stable virtual range.
#[derive(Debug)]
pub(super) struct VirtualMemoryError {
    /// Operation that failed.
    operation: VirtualMemoryOperation,
    /// Platform error.
    source: io::Error,
}

impl VirtualMemoryError {
    /// Attaches operation context to a platform error.
    fn new(operation: VirtualMemoryOperation, source: io::Error) -> Self {
        Self { operation, source }
    }

    /// Reports an invalid reservation length.
    fn invalid_length(len: usize, page_size: usize) -> Self {
        Self::new(
            VirtualMemoryOperation::Reserve,
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("range length {len} is not a nonzero multiple of page size {page_size}"),
            ),
        )
    }

    /// Returns the operation that failed.
    pub(super) fn operation(&self) -> VirtualMemoryOperation {
        self.operation
    }
}

impl fmt::Display for VirtualMemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "virtual-memory {} failed: {}",
            self.operation, self.source
        )
    }
}

impl std::error::Error for VirtualMemoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(test)]
mod test_support {
    use std::io;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::{VirtualMemoryError, VirtualMemoryOperation};

    /// One-shot transition failures installed by a test.
    #[derive(Debug, Default)]
    pub(super) struct InjectedFailures {
        /// Fails the next prepare transition when set.
        prepare: AtomicBool,
        /// Fails the next deactivate transition when set.
        deactivate: AtomicBool,
        /// Fails the next discard transition when set.
        discard: AtomicBool,
    }

    impl InjectedFailures {
        /// Installs one failure for `operation`.
        ///
        /// Panics when the operation does not support injection or already has
        /// a pending failure.
        pub(super) fn inject_once(&self, operation: VirtualMemoryOperation) {
            let pending = self.pending(operation);
            assert!(
                !pending.swap(true, Ordering::AcqRel),
                "{operation} already has a pending failure"
            );
        }

        /// Consumes and reports a pending failure for `operation`.
        pub(super) fn check(
            &self,
            operation: VirtualMemoryOperation,
        ) -> Result<(), VirtualMemoryError> {
            if self.pending(operation).swap(false, Ordering::AcqRel) {
                Err(VirtualMemoryError::new(
                    operation,
                    io::Error::other(format!("injected {operation} failure")),
                ))
            } else {
                Ok(())
            }
        }

        /// Returns the one-shot flag for an injectable transition.
        fn pending(&self, operation: VirtualMemoryOperation) -> &AtomicBool {
            match operation {
                VirtualMemoryOperation::Prepare => &self.prepare,
                VirtualMemoryOperation::Deactivate => &self.deactivate,
                VirtualMemoryOperation::Discard => &self.discard,
                VirtualMemoryOperation::QueryPageSize
                | VirtualMemoryOperation::Reserve
                | VirtualMemoryOperation::Release => {
                    panic!("{operation} does not support failure injection")
                }
            }
        }
    }
}

/// Returns the target's runtime page size.
///
/// Errors report [`VirtualMemoryOperation::QueryPageSize`] when the platform
/// query fails or returns a value that cannot be represented as nonzero
/// `usize`.
pub(super) fn page_size() -> Result<NonZeroUsize, VirtualMemoryError> {
    sys::page_size()
        .and_then(|size| {
            usize::try_from(size)
                .ok()
                .and_then(NonZeroUsize::new)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("platform returned invalid page size {size}"),
                    )
                })
        })
        .map_err(|source| VirtualMemoryError::new(VirtualMemoryOperation::QueryPageSize, source))
}

/// An exclusively owned, page-aligned virtual address range.
///
/// The address and length remain stable until drop. Protection and backing may
/// change without returning the address range to the operating system.
#[derive(Debug)]
pub(super) struct VirtualRange {
    /// Allocation base retained until drop.
    base: NonNull<MaybeUninit<u8>>,
    /// Reserved length in bytes.
    len: usize,
    /// One-shot transition failures installed by tests.
    #[cfg(test)]
    injected_failures: InjectedFailures,
}

// SAFETY: the range is exclusively reserved for this object, and sharing the
// wrapper exposes no reference or unsynchronized access to its storage.
unsafe impl Send for VirtualRange {}
unsafe impl Sync for VirtualRange {}

impl VirtualRange {
    /// Reserves an inaccessible page-aligned range.
    ///
    /// `len` must be a nonzero multiple of `page_size`. Success retains the
    /// complete range at one address until drop.
    pub(super) fn reserve(len: usize, page_size: NonZeroUsize) -> Result<Self, VirtualMemoryError> {
        if len == 0 || !len.is_multiple_of(page_size.get()) {
            return Err(VirtualMemoryError::invalid_length(len, page_size.get()));
        }

        let base = sys::reserve(len)
            .map_err(|source| VirtualMemoryError::new(VirtualMemoryOperation::Reserve, source))?;
        Ok(Self {
            base,
            len,
            #[cfg(test)]
            injected_failures: InjectedFailures::default(),
        })
    }

    /// Returns the reserved length in bytes.
    pub(super) fn len(&self) -> usize {
        self.len
    }

    /// Returns the allocation base for address comparison.
    ///
    /// The integer does not grant access and is never converted back to a
    /// pointer.
    pub(super) fn base_address(&self) -> usize {
        self.base.as_ptr().addr()
    }

    /// Makes the complete range readable and writable.
    ///
    /// Failure preserves exclusive address ownership but leaves protection
    /// unspecified. Bytes remain logically uninitialized after success.
    pub(super) fn prepare(&self) -> Result<(), VirtualMemoryError> {
        #[cfg(test)]
        self.injected_failures
            .check(VirtualMemoryOperation::Prepare)?;
        sys::prepare(self.base, self.len)
            .map_err(|source| VirtualMemoryError::new(VirtualMemoryOperation::Prepare, source))
    }

    /// Makes the complete range inaccessible while retaining its address.
    ///
    /// Failure preserves exclusive address ownership but leaves protection
    /// unspecified.
    pub(super) fn deactivate(&self) -> Result<(), VirtualMemoryError> {
        #[cfg(test)]
        self.injected_failures
            .check(VirtualMemoryOperation::Deactivate)?;
        sys::deactivate(self.base, self.len)
            .map_err(|source| VirtualMemoryError::new(VirtualMemoryOperation::Deactivate, source))
    }

    /// Makes backing from an inaccessible range reclaimable.
    ///
    /// Failure leaves the address reserved and may leave backing resident.
    pub(super) fn discard(&self) -> Result<(), VirtualMemoryError> {
        #[cfg(test)]
        self.injected_failures
            .check(VirtualMemoryOperation::Discard)?;
        sys::discard(self.base, self.len)
            .map_err(|source| VirtualMemoryError::new(VirtualMemoryOperation::Discard, source))
    }

    /// Injects one failure for a supported transition.
    ///
    /// Panics when `operation` has no injectable transition.
    #[cfg(test)]
    pub(super) fn inject_failure_once(&self, operation: VirtualMemoryOperation) {
        self.injected_failures.inject_once(operation);
    }

    /// Computes a pointer wholly contained in this range.
    ///
    /// # Safety
    ///
    /// The complete subrange must remain prepared until the pointer is no
    /// longer used. Mutable access requires exclusive claim ownership.
    /// Immutable access requires initialized owner coverage that prevents
    /// deactivation for the complete access.
    pub(super) unsafe fn ptr_for_range(
        &self,
        offset: usize,
        len: usize,
    ) -> Option<NonNull<MaybeUninit<u8>>> {
        let end = offset.checked_add(len)?;
        if len == 0 || end > self.len {
            return None;
        }

        // SAFETY: the checked nonempty subrange lies within this reservation.
        Some(unsafe { self.base.byte_add(offset) })
    }
}

impl Drop for VirtualRange {
    fn drop(&mut self) {
        // A valid reservation release cannot fail. Drop cannot return the
        // platform error or retry after relinquishing ownership of this value.
        if let Err(error) = sys::release(self.base, self.len) {
            tracing::error!(
                target: crate::telemetry::TARGET_MEMORY,
                operation = %VirtualMemoryOperation::Release,
                error = %error,
                base = self.base_address(),
                len = self.len,
                "buffer-pool virtual range release failed; aborting"
            );
            std::process::abort();
        }
    }
}

#[cfg(not(any(miri, all(test, s3_tm_loom))))]
mod sys {
    //! Native virtual-memory backends.

    #[cfg(any(
        target_os = "android",
        target_os = "freebsd",
        target_os = "linux",
        target_os = "macos"
    ))]
    mod platform {
        //! Unix virtual-memory backend.
        //!
        //! Linux and Android follow [`mmap(2)`], [`mprotect(2)`], and
        //! [`madvise(2)`]. FreeBSD follows its [`mmap(2)`][freebsd-mmap],
        //! [`mprotect(2)`][freebsd-mprotect], and [`madvise(2)`][freebsd-madvise]
        //! contracts. macOS follows the Darwin [`mmap(2)`][darwin-mmap],
        //! [`mprotect(2)`][darwin-mprotect], and [`madvise(2)`][darwin-madvise]
        //! manual pages.
        //!
        //! [`mmap(2)`]: https://man7.org/linux/man-pages/man2/mmap.2.html
        //! [`mprotect(2)`]: https://man7.org/linux/man-pages/man2/mprotect.2.html
        //! [`madvise(2)`]: https://man7.org/linux/man-pages/man2/madvise.2.html
        //! [freebsd-mmap]: https://man.freebsd.org/cgi/man.cgi?query=mmap&sektion=2
        //! [freebsd-mprotect]: https://man.freebsd.org/cgi/man.cgi?query=mprotect&sektion=2
        //! [freebsd-madvise]: https://man.freebsd.org/cgi/man.cgi?query=madvise&sektion=2
        //! [darwin-mmap]: https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/mmap.2.html
        //! [darwin-mprotect]: https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/mprotect.2.html
        //! [darwin-madvise]: https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/madvise.2.html

        use std::io;
        use std::mem::MaybeUninit;
        use std::ptr::{self, NonNull};

        /// Returns `_SC_PAGESIZE` from [`sysconf()`].
        ///
        /// [`sysconf()`]: https://pubs.opengroup.org/onlinepubs/9799919799/functions/sysconf.html
        pub(in super::super) fn page_size() -> io::Result<u64> {
            // SAFETY: sysconf has no pointer preconditions.
            let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
            if size <= 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(size as u64)
        }

        /// Creates a private anonymous `PROT_NONE` mapping.
        pub(in super::super) fn reserve(len: usize) -> io::Result<NonNull<MaybeUninit<u8>>> {
            // SAFETY: a null address asks the kernel to select a fresh range.
            // The returned mapping is owned by the caller until `release`.
            let base = unsafe {
                libc::mmap(
                    ptr::null_mut(),
                    len,
                    libc::PROT_NONE,
                    libc::MAP_PRIVATE | libc::MAP_ANON,
                    -1,
                    0,
                )
            };
            if base == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }
            if base.is_null() {
                // SAFETY: mmap reported success for this complete mapping.
                let released = unsafe { libc::munmap(base, len) };
                return Err(if released == 0 {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "mmap returned a null successful mapping",
                    )
                } else {
                    io::Error::last_os_error()
                });
            }
            // SAFETY: MAP_FAILED and null were rejected above.
            Ok(unsafe { NonNull::new_unchecked(base.cast()) })
        }

        /// Applies read-write protection to the complete mapping.
        pub(in super::super) fn prepare(
            base: NonNull<MaybeUninit<u8>>,
            len: usize,
        ) -> io::Result<()> {
            // SAFETY: `VirtualRange` retains this complete mapping.
            check_libc_result(unsafe {
                libc::mprotect(
                    base.as_ptr().cast(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                )
            })
        }

        /// Applies `PROT_NONE` to the complete mapping.
        pub(in super::super) fn deactivate(
            base: NonNull<MaybeUninit<u8>>,
            len: usize,
        ) -> io::Result<()> {
            // SAFETY: the block lifecycle excludes access to this complete
            // range.
            check_libc_result(unsafe { libc::mprotect(base.as_ptr().cast(), len, libc::PROT_NONE) })
        }

        /// Marks inactive backing reclaimable without releasing the mapping.
        ///
        /// Linux and Android use `MADV_DONTNEED`; FreeBSD and macOS use
        /// `MADV_FREE`.
        pub(in super::super) fn discard(
            base: NonNull<MaybeUninit<u8>>,
            len: usize,
        ) -> io::Result<()> {
            #[cfg(any(target_os = "android", target_os = "linux"))]
            let advice = libc::MADV_DONTNEED;
            #[cfg(any(target_os = "freebsd", target_os = "macos"))]
            let advice = libc::MADV_FREE;

            // SAFETY: lifecycle serialization keeps the complete range
            // inactive until discard and any later preparation finish.
            check_libc_result(unsafe { libc::madvise(base.as_ptr().cast(), len, advice) })
        }

        /// Releases the complete mapping.
        pub(in super::super) fn release(
            base: NonNull<MaybeUninit<u8>>,
            len: usize,
        ) -> io::Result<()> {
            // SAFETY: final `VirtualRange` destruction owns the complete
            // mapping.
            check_libc_result(unsafe { libc::munmap(base.as_ptr().cast(), len) })
        }

        /// Converts a libc result where zero indicates success.
        fn check_libc_result(result: libc::c_int) -> io::Result<()> {
            if result == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
    }

    #[cfg(target_os = "windows")]
    mod platform {
        //! Windows virtual-memory backend.
        //!
        //! Address reservation and commit follow [`VirtualAlloc`]. Decommit and
        //! final release follow [`VirtualFree`]. Runtime page geometry comes
        //! from [`GetSystemInfo`].
        //!
        //! [`VirtualAlloc`]: https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-virtualalloc
        //! [`VirtualFree`]: https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-virtualfree
        //! [`GetSystemInfo`]: https://learn.microsoft.com/en-us/windows/win32/api/sysinfoapi/nf-sysinfoapi-getsysteminfo

        use std::io;
        use std::mem::MaybeUninit;
        use std::ptr::{self, NonNull};

        use windows_sys::Win32::System::Memory::{
            VirtualAlloc, VirtualFree, MEM_COMMIT, MEM_DECOMMIT, MEM_RELEASE, MEM_RESERVE,
            PAGE_NOACCESS, PAGE_READWRITE,
        };
        use windows_sys::Win32::System::SystemInformation::{GetSystemInfo, SYSTEM_INFO};

        /// Returns the system page size.
        pub(in super::super) fn page_size() -> io::Result<u64> {
            let mut info = MaybeUninit::<SYSTEM_INFO>::uninit();
            // SAFETY: `info` is valid writable storage for one SYSTEM_INFO.
            unsafe { GetSystemInfo(info.as_mut_ptr()) };
            // SAFETY: GetSystemInfo initializes the complete output structure.
            let info = unsafe { info.assume_init() };
            Ok(info.dwPageSize as u64)
        }

        /// Reserves an inaccessible range without committing pages.
        pub(in super::super) fn reserve(len: usize) -> io::Result<NonNull<MaybeUninit<u8>>> {
            // SAFETY: a null address asks the kernel to select an exclusively
            // reserved range, which is released by `release`.
            let base = unsafe { VirtualAlloc(ptr::null(), len, MEM_RESERVE, PAGE_NOACCESS) };
            NonNull::new(base.cast()).ok_or_else(io::Error::last_os_error)
        }

        /// Commits the complete range with read-write protection.
        pub(in super::super) fn prepare(
            base: NonNull<MaybeUninit<u8>>,
            len: usize,
        ) -> io::Result<()> {
            // SAFETY: the requested range is wholly contained in one
            // reservation.
            let committed =
                unsafe { VirtualAlloc(base.as_ptr().cast(), len, MEM_COMMIT, PAGE_READWRITE) };
            let committed: NonNull<MaybeUninit<u8>> =
                NonNull::new(committed.cast()).ok_or_else(io::Error::last_os_error)?;
            if committed != base {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "VirtualAlloc committed a different address",
                ));
            }
            Ok(())
        }

        /// Decommits the complete range while retaining its reservation.
        pub(in super::super) fn deactivate(
            base: NonNull<MaybeUninit<u8>>,
            len: usize,
        ) -> io::Result<()> {
            // SAFETY: the block lifecycle excludes access to the committed
            // range.
            check_win32_result(unsafe { VirtualFree(base.as_ptr().cast(), len, MEM_DECOMMIT) })
        }

        /// Completes reclaim already performed by `MEM_DECOMMIT`.
        pub(in super::super) fn discard(
            _base: NonNull<MaybeUninit<u8>>,
            _len: usize,
        ) -> io::Result<()> {
            // MEM_DECOMMIT performs both deactivation and backing release.
            Ok(())
        }

        /// Releases the complete address reservation.
        pub(in super::super) fn release(
            base: NonNull<MaybeUninit<u8>>,
            _len: usize,
        ) -> io::Result<()> {
            // SAFETY: MEM_RELEASE requires the allocation base and a zero size.
            check_win32_result(unsafe { VirtualFree(base.as_ptr().cast(), 0, MEM_RELEASE) })
        }

        /// Converts a Win32 result where nonzero indicates success.
        fn check_win32_result(result: i32) -> io::Result<()> {
            if result != 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
    }

    #[cfg(not(any(
        target_os = "android",
        target_os = "freebsd",
        target_os = "linux",
        target_os = "macos",
        target_os = "windows"
    )))]
    mod platform {
        //! Unsupported-target backend.

        use std::io;
        use std::mem::MaybeUninit;
        use std::ptr::NonNull;

        /// Reports that page-size discovery is unsupported.
        pub(in super::super) fn page_size() -> io::Result<u64> {
            Err(unsupported())
        }

        /// Reports that virtual-range reservation is unsupported.
        pub(in super::super) fn reserve(_len: usize) -> io::Result<NonNull<MaybeUninit<u8>>> {
            Err(unsupported())
        }

        /// Reports that range preparation is unsupported.
        pub(in super::super) fn prepare(
            _base: NonNull<MaybeUninit<u8>>,
            _len: usize,
        ) -> io::Result<()> {
            Err(unsupported())
        }

        /// Reports that range deactivation is unsupported.
        pub(in super::super) fn deactivate(
            _base: NonNull<MaybeUninit<u8>>,
            _len: usize,
        ) -> io::Result<()> {
            Err(unsupported())
        }

        /// Reports that backing discard is unsupported.
        pub(in super::super) fn discard(
            _base: NonNull<MaybeUninit<u8>>,
            _len: usize,
        ) -> io::Result<()> {
            Err(unsupported())
        }

        /// Reports that final release is unsupported.
        pub(in super::super) fn release(
            _base: NonNull<MaybeUninit<u8>>,
            _len: usize,
        ) -> io::Result<()> {
            Err(unsupported())
        }

        /// Constructs an unsupported-target error.
        fn unsupported() -> io::Error {
            io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "no buffer-pool virtual-memory backend for {}",
                    std::env::consts::OS
                ),
            )
        }
    }

    pub(super) use platform::*;
}

#[cfg(any(miri, all(test, s3_tm_loom)))]
mod sys {
    //! Allocator-backed model for Miri and Loom.
    //!
    //! This backend preserves allocation ownership and pointer provenance. It
    //! does not model operating-system access protection or backing reclaim.

    use std::alloc::{alloc, dealloc, Layout};
    use std::io;
    use std::mem::MaybeUninit;
    use std::ptr::NonNull;

    /// Model page size and allocation alignment.
    const PAGE_SIZE: usize = 4096;

    /// Returns the model page size.
    pub(super) fn page_size() -> io::Result<u64> {
        Ok(PAGE_SIZE as u64)
    }

    /// Allocates a page-aligned model range.
    pub(super) fn reserve(len: usize) -> io::Result<NonNull<MaybeUninit<u8>>> {
        let layout = layout(len)?;
        // SAFETY: the layout is nonzero and valid.
        let base = unsafe { alloc(layout) };
        NonNull::new(base.cast()).ok_or_else(|| io::Error::from(io::ErrorKind::OutOfMemory))
    }

    /// Accepts a model prepare transition.
    pub(super) fn prepare(_base: NonNull<MaybeUninit<u8>>, _len: usize) -> io::Result<()> {
        Ok(())
    }

    /// Accepts a model deactivate transition.
    pub(super) fn deactivate(_base: NonNull<MaybeUninit<u8>>, _len: usize) -> io::Result<()> {
        Ok(())
    }

    /// Accepts a model discard transition.
    pub(super) fn discard(_base: NonNull<MaybeUninit<u8>>, _len: usize) -> io::Result<()> {
        Ok(())
    }

    /// Deallocates the model range with its original layout.
    pub(super) fn release(base: NonNull<MaybeUninit<u8>>, len: usize) -> io::Result<()> {
        let layout = layout(len)?;
        // SAFETY: `base` was allocated with this layout and remains owned.
        unsafe { dealloc(base.as_ptr().cast(), layout) };
        Ok(())
    }

    /// Returns the page-aligned allocation layout for `len`.
    fn layout(len: usize) -> io::Result<Layout> {
        Layout::from_size_align(len, PAGE_SIZE)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_and_partial_pages() {
        let page_size = NonZeroUsize::new(4096).unwrap();

        assert_eq!(
            VirtualRange::reserve(0, page_size).unwrap_err().operation(),
            VirtualMemoryOperation::Reserve
        );
        assert_eq!(
            VirtualRange::reserve(4097, page_size)
                .unwrap_err()
                .operation(),
            VirtualMemoryOperation::Reserve
        );
    }

    #[test]
    fn range_lifecycle_preserves_address() {
        let page_size = page_size().unwrap();
        let range = VirtualRange::reserve(page_size.get() * 2, page_size).unwrap();
        let base = range.base_address();

        assert_eq!(range.len(), page_size.get() * 2);
        assert_eq!(base % page_size.get(), 0);

        range.prepare().unwrap();
        // SAFETY: the prepared range is retained exclusively by this test.
        let first = unsafe { range.ptr_for_range(0, 1).unwrap() };
        // SAFETY: the first byte is writable and has no alias.
        unsafe { first.as_ptr().write(MaybeUninit::new(0x5a)) };
        // SAFETY: the preceding write initialized this byte.
        assert_eq!(unsafe { first.as_ptr().read().assume_init() }, 0x5a);

        range.deactivate().unwrap();
        range.discard().unwrap();
        range.prepare().unwrap();

        assert_eq!(range.base_address(), base);
        // SAFETY: revival made the retained range writable again.
        let last = unsafe {
            range
                .ptr_for_range(range.len() - 1, 1)
                .expect("last byte is within the range")
        };
        // SAFETY: the last byte is writable and has no alias.
        unsafe { last.as_ptr().write(MaybeUninit::new(0xa5)) };
        // SAFETY: the preceding write initialized this byte.
        assert_eq!(unsafe { last.as_ptr().read().assume_init() }, 0xa5);
    }

    #[test]
    fn injected_transition_failures_are_consumed_once() {
        let page_size = page_size().unwrap();
        let range = VirtualRange::reserve(page_size.get(), page_size).unwrap();
        range.inject_failure_once(VirtualMemoryOperation::Prepare);

        let error = range.prepare().unwrap_err();

        assert_eq!(error.operation(), VirtualMemoryOperation::Prepare);
        assert!(error
            .to_string()
            .contains("virtual-memory preparation failed"));
        assert!(std::error::Error::source(&error).is_some());
        range.prepare().unwrap();

        range.inject_failure_once(VirtualMemoryOperation::Deactivate);
        let error = range.deactivate().unwrap_err();
        assert_eq!(error.operation(), VirtualMemoryOperation::Deactivate);
        range.deactivate().unwrap();

        range.inject_failure_once(VirtualMemoryOperation::Discard);
        let error = range.discard().unwrap_err();
        assert_eq!(error.operation(), VirtualMemoryOperation::Discard);
        range.discard().unwrap();
        range.prepare().unwrap();
    }

    #[test]
    fn checked_pointer_rejects_invalid_subranges() {
        let page_size = page_size().unwrap();
        let range = VirtualRange::reserve(page_size.get(), page_size).unwrap();
        range.prepare().unwrap();

        // SAFETY: this test only inspects pointer construction while the
        // complete range is prepared and exclusively retained.
        unsafe {
            assert!(range.ptr_for_range(0, 0).is_none());
            assert!(range.ptr_for_range(range.len(), 1).is_none());
            assert!(range.ptr_for_range(range.len() - 1, 2).is_none());
            assert!(range.ptr_for_range(usize::MAX, 2).is_none());
            assert_eq!(
                range.ptr_for_range(0, range.len()).unwrap().addr().get(),
                range.base_address()
            );
            assert_eq!(
                range
                    .ptr_for_range(1, range.len() - 1)
                    .unwrap()
                    .addr()
                    .get(),
                range.base_address() + 1
            );
        }
    }

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn virtual_range_is_send_and_sync() {
        assert_send_sync::<VirtualRange>();
    }
}
