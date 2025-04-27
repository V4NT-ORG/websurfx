//! This module provides the functionality to generate random user agent string.

use fake_useragent::{Browsers, UserAgents, UserAgentsBuilder};
use tokio::sync::OnceCell;

/// A static variable which stores the initially build `UserAgents` struct. So as it can be resused
/// again and again without the need of reinitializing the `UserAgents` struct.
static USER_AGENTS: OnceCell<UserAgents> = OnceCell::const_new();

/// A function to generate random user agent to improve privacy of the user.
///
/// # Returns
///
/// A randomly generated user agent string.
pub async fn random_user_agent(threads: u8) -> Result<&'static str, Box<dyn std::error::Error>> {
    Ok(USER_AGENTS
        .get_or_try_init(|| async move {
            tokio::task::spawn_blocking(move || {
                UserAgentsBuilder::new()
                    .cache(false)
                    .dir("/tmp")
                    .thread(threads as u32)
                    .set_browsers(
                        Browsers::new()
                            .set_chrome()
                            .set_safari()
                            .set_edge()
                            .set_firefox()
                            .set_mozilla(),
                    )
                    .build()
            })
            .await
        })
        .await?
        .random())
}
