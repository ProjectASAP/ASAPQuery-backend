/// Controls whether query-serving code may contact an external backend.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueryForwardingPolicy {
    #[default]
    Enabled,
    Disabled,
}

impl QueryForwardingPolicy {
    pub const fn allows_external_queries(self) -> bool {
        matches!(self, Self::Enabled)
    }
}
