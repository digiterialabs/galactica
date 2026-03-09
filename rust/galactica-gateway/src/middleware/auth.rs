/// Placeholder auth middleware layer.
///
/// Will validate API keys and tenant credentials from request headers.
#[derive(Clone)]
pub struct AuthLayer;

impl AuthLayer {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AuthLayer {
    fn default() -> Self {
        Self::new()
    }
}
