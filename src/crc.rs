const CRC32C: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

pub(crate) fn crc32(data: impl AsRef<[u8]>) -> u32 {
    CRC32C.checksum(data.as_ref())
}

pub(crate) struct Digest {
    inner: crc::Digest<'static, u32>,
}

impl Digest {
    pub fn update(&mut self, data: &[u8]) {
        self.inner.update(data);
    }

    pub fn finalize(self) -> u32 {
        self.inner.finalize()
    }
}

pub(crate) fn digest() -> Digest {
    Digest {
        inner: CRC32C.digest(),
    }
}
