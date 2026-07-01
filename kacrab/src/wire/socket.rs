//! Tuned TCP socket construction for broker connections.

use std::{io, net::SocketAddr, time::Duration};

use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};
use tokio::net::TcpStream;

use super::config::SocketConfig;

/// A broker address to resolve and connect to.
pub(crate) struct ResolveTarget<'a> {
    /// Advertised or configured broker hostname (may be a literal IP).
    pub host: &'a str,
    /// Broker port.
    pub port: u16,
    /// Try every resolved IP (`client.dns.lookup=use_all_dns_ips`) rather than
    /// only the first.
    pub use_all_dns_ips: bool,
    /// Address used when the hostname does not resolve.
    pub fallback: SocketAddr,
}

/// Resolve `host:port` and connect, trying candidate addresses in IPv4-first
/// order so a dead IPv6 loopback (e.g. `localhost` → `[::1]` with a broker bound
/// only on IPv4) never stalls the connect. With `use_all_dns_ips` every resolved
/// address is tried until one succeeds (`client.dns.lookup=use_all_dns_ips`);
/// otherwise only the first (IPv4-preferred) address is used. `fallback` is used
/// when the hostname does not resolve (e.g. it is already a literal address).
pub(crate) async fn resolve_and_connect(
    config: &SocketConfig,
    connect_timeout: Duration,
    target: &ResolveTarget<'_>,
) -> io::Result<TcpStream> {
    let mut addresses: Vec<SocketAddr> = tokio::net::lookup_host((target.host, target.port))
        .await
        .map(Iterator::collect)
        .unwrap_or_default();
    order_ipv4_first(&mut addresses);
    if addresses.is_empty() {
        addresses.push(target.fallback);
    }
    let take = if target.use_all_dns_ips {
        addresses.len()
    } else {
        1
    };
    let mut last_error = None;
    for address in addresses.into_iter().take(take) {
        match connect(config, connect_timeout, address).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| io::Error::other("no broker address to connect")))
}

/// Order resolved addresses IPv4-first so a dead IPv6 loopback never stalls a
/// connect; ties keep their resolved order (stable sort).
fn order_ipv4_first(addresses: &mut [SocketAddr]) {
    addresses.sort_by_key(|address| !address.is_ipv4());
}

pub(crate) async fn connect(
    config: &SocketConfig,
    connect_timeout: Duration,
    addr: SocketAddr,
) -> io::Result<TcpStream> {
    let socket = new_socket(addr)?;
    apply_pre_connect_options(config, &socket)?;
    socket.set_nonblocking(true)?;

    match socket.connect(&addr.into()) {
        Ok(()) => {},
        Err(error) if is_connect_in_progress(&error) => {},
        Err(error) => return Err(error),
    }

    let std_stream: std::net::TcpStream = socket.into();
    std_stream.set_nonblocking(true)?;
    let stream = TcpStream::from_std(std_stream)?;
    tokio::time::timeout(connect_timeout, stream.writable())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "broker TCP connect timeout"))??;
    if let Some(error) = stream.take_error()? {
        return Err(error);
    }
    stream.set_nodelay(config.tcp_nodelay)?;

    #[cfg(any(
        target_os = "android",
        target_os = "cygwin",
        target_os = "fuchsia",
        target_os = "linux",
    ))]
    {
        apply_post_connect_options(&stream, config)?;
    }

    Ok(stream)
}

fn apply_pre_connect_options(config: &SocketConfig, socket: &Socket) -> io::Result<()> {
    if config.reuse_address {
        socket.set_reuse_address(true)?;
    }
    socket.set_tcp_nodelay(config.tcp_nodelay)?;
    if let Some(bytes) = config.send_buffer_bytes {
        socket.set_send_buffer_size(bytes)?;
    }
    if let Some(bytes) = config.receive_buffer_bytes {
        socket.set_recv_buffer_size(bytes)?;
    }
    if let Some(keepalive) = config.tcp_keepalive {
        let tcp_keepalive = TcpKeepalive::new()
            .with_time(keepalive.idle)
            .with_interval(keepalive.interval);
        socket.set_tcp_keepalive(&tcp_keepalive)?;
    }

    #[cfg(any(target_os = "android", target_os = "freebsd", target_os = "linux"))]
    {
        apply_pre_connect_platform_options(socket, config)?;
    }

    Ok(())
}

fn new_socket(addr: SocketAddr) -> io::Result<Socket> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
}

#[cfg(any(target_os = "android", target_os = "freebsd", target_os = "linux"))]
fn apply_pre_connect_platform_options(socket: &Socket, config: &SocketConfig) -> io::Result<()> {
    #[cfg(any(target_os = "android", target_os = "linux"))]
    if let Some(bytes) = config.tcp_notsent_lowat_bytes {
        socket.set_tcp_notsent_lowat(bytes)?;
    }

    #[cfg(any(target_os = "freebsd", target_os = "linux"))]
    if let Some(congestion) = config.tcp_congestion {
        socket.set_tcp_congestion(congestion.as_bytes())?;
    }
    Ok(())
}

#[cfg(any(
    target_os = "android",
    target_os = "cygwin",
    target_os = "fuchsia",
    target_os = "linux",
))]
fn apply_post_connect_options(stream: &TcpStream, config: &SocketConfig) -> io::Result<()> {
    let socket = socket2::SockRef::from(stream);

    if let Some(enabled) = config.tcp_quickack {
        socket.set_tcp_quickack(enabled)?;
    }

    socket.set_tcp_user_timeout(config.tcp_user_timeout_ms)?;
    Ok(())
}

fn is_connect_in_progress(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    ) || error.raw_os_error().is_some_and(is_in_progress_errno)
}

const fn is_in_progress_errno(errno: i32) -> bool {
    #[cfg(unix)]
    {
        errno == libc_errno::EINPROGRESS
    }
    #[cfg(not(unix))]
    {
        let _unused = errno;
        false
    }
}

#[cfg(unix)]
mod libc_errno {
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
    pub(super) const EINPROGRESS: i32 = 36;

    #[cfg(target_os = "linux")]
    pub(super) const EINPROGRESS: i32 = 115;
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use std::{
        io,
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        time::Duration,
    };

    use tokio::net::TcpListener;

    use super::{connect, is_connect_in_progress, new_socket, order_ipv4_first};
    use crate::wire::{SocketConfig, TcpKeepaliveConfig};

    #[test]
    fn order_ipv4_first_moves_ipv4_ahead_of_ipv6() {
        let ipv6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9092);
        let ipv4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092);
        let mut addresses = vec![ipv6, ipv4];
        order_ipv4_first(&mut addresses);
        assert_eq!(addresses, vec![ipv4, ipv6]);

        // Already IPv4-first / single-family lists are left in order.
        let mut only_ipv6 = vec![ipv6];
        order_ipv4_first(&mut only_ipv6);
        assert_eq!(only_ipv6, vec![ipv6]);
    }

    #[test]
    fn connect_in_progress_detection_accepts_async_connect_errors() {
        assert!(is_connect_in_progress(&io::Error::from(
            io::ErrorKind::WouldBlock
        )));
        assert!(is_connect_in_progress(&io::Error::from(
            io::ErrorKind::Interrupted
        )));
        assert!(!is_connect_in_progress(&io::Error::from(
            io::ErrorKind::ConnectionRefused
        )));
    }

    #[test]
    fn new_socket_supports_ipv4_and_ipv6_addresses() {
        let ipv4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let ipv6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0);

        assert!(new_socket(ipv4).is_ok());
        assert!(new_socket(ipv6).is_ok());
    }

    #[tokio::test]
    async fn connect_applies_pre_connect_options_and_succeeds_on_loopback() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let accept = tokio::spawn(async move {
            let (_stream, _peer) = listener.accept().await.expect("accept connection");
        });
        let config = SocketConfig::default()
            .send_buffer_bytes(16 * 1024)
            .receive_buffer_bytes(16 * 1024)
            .tcp_keepalive(Some(TcpKeepaliveConfig {
                idle: Duration::from_secs(30),
                interval: Duration::from_secs(5),
            }));

        let stream = connect(&config, Duration::from_secs(1), addr)
            .await
            .expect("connect");

        assert!(stream.peer_addr().is_ok());
        accept.await.expect("accept task");
    }

    #[tokio::test]
    async fn connect_returns_immediate_socket_error_for_refused_loopback() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        drop(listener);

        assert!(
            connect(&SocketConfig::default(), Duration::from_millis(50), addr)
                .await
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn unix_errno_detection_matches_platform_einprogress() {
        assert!(super::is_in_progress_errno(super::libc_errno::EINPROGRESS));
        assert!(!super::is_in_progress_errno(0));
    }
}
