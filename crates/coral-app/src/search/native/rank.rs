//! Provider-local rank input for native rows.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::search) struct NativeRankInput {
    provider_ordinal: u32,
}

impl NativeRankInput {
    pub(in crate::search) const fn from_provider_ordinal(provider_ordinal: u32) -> Self {
        Self { provider_ordinal }
    }

    pub(in crate::search) const fn provider_ordinal(self) -> u32 {
        self.provider_ordinal
    }
}
