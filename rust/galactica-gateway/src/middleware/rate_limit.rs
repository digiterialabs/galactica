/// Placeholder rate limiting middleware layer.
///
/// Will enforce per-tenant request limits.
#[derive(Clone)]
pub struct RateLimitLayer {
    pub max_requests_per_minute: u32,
}

impl RateLimitLayer {
    pub fn new(max_requests_per_minute: u32) -> Self {
        Self {
            max_requests_per_minute,
        }
    }
}
