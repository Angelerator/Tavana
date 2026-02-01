//! Kubernetes resource parsing utilities
//!
//! Provides functions to parse K8s resource quantities (memory, CPU) into
//! standardized units for internal use.

/// Parse K8s memory string to bytes.
///
/// Supports formats: "256Mi", "1Gi", "512M", "1G", "1234567890" (raw bytes)
///
/// # Examples
/// ```
/// use tavana_common::k8s::parse_k8s_memory_bytes;
/// assert_eq!(parse_k8s_memory_bytes("1Gi"), 1024 * 1024 * 1024);
/// assert_eq!(parse_k8s_memory_bytes("256Mi"), 256 * 1024 * 1024);
/// ```
pub fn parse_k8s_memory_bytes(value: &str) -> u64 {
    let value = value.trim();

    if value.ends_with("Gi") {
        value.trim_end_matches("Gi").parse::<u64>().unwrap_or(0) * 1024 * 1024 * 1024
    } else if value.ends_with("Mi") {
        value.trim_end_matches("Mi").parse::<u64>().unwrap_or(0) * 1024 * 1024
    } else if value.ends_with("Ki") {
        value.trim_end_matches("Ki").parse::<u64>().unwrap_or(0) * 1024
    } else if value.ends_with("G") {
        // Decimal units (SI)
        value.trim_end_matches("G").parse::<u64>().unwrap_or(0) * 1_000_000_000
    } else if value.ends_with("M") {
        value.trim_end_matches("M").parse::<u64>().unwrap_or(0) * 1_000_000
    } else if value.ends_with("K") {
        value.trim_end_matches("K").parse::<u64>().unwrap_or(0) * 1_000
    } else {
        // Assume raw bytes
        value.parse::<u64>().unwrap_or(0)
    }
}

/// Parse K8s memory string to megabytes (MB).
///
/// Supports formats: "256Mi", "1Gi", "512M", "1G", "1234567890" (raw bytes)
///
/// # Examples
/// ```
/// use tavana_common::k8s::parse_k8s_memory_mb;
/// assert_eq!(parse_k8s_memory_mb("1Gi"), 1024);
/// assert_eq!(parse_k8s_memory_mb("512Mi"), 512);
/// ```
pub fn parse_k8s_memory_mb(value: &str) -> u64 {
    let value = value.trim();

    if value.ends_with("Gi") {
        value.trim_end_matches("Gi").parse::<u64>().unwrap_or(0) * 1024
    } else if value.ends_with("Mi") {
        value.trim_end_matches("Mi").parse::<u64>().unwrap_or(0)
    } else if value.ends_with("Ki") {
        value.trim_end_matches("Ki").parse::<u64>().unwrap_or(0) / 1024
    } else if value.ends_with("G") {
        // Decimal units: 1G = 1000MB (SI)
        value.trim_end_matches("G").parse::<u64>().unwrap_or(0) * 1000
    } else if value.ends_with("M") {
        value.trim_end_matches("M").parse::<u64>().unwrap_or(0)
    } else if value.ends_with("K") {
        value.trim_end_matches("K").parse::<u64>().unwrap_or(0) / 1000
    } else {
        // Assume raw bytes, convert to MB
        value.parse::<u64>().unwrap_or(0) / (1024 * 1024)
    }
}

/// Parse K8s CPU string to millicores.
///
/// Supports formats: "100m", "1", "1500m", "2.5", "1000000000n"
///
/// # Examples
/// ```
/// use tavana_common::k8s::parse_k8s_cpu_millicores;
/// assert_eq!(parse_k8s_cpu_millicores("1"), 1000);
/// assert_eq!(parse_k8s_cpu_millicores("500m"), 500);
/// assert_eq!(parse_k8s_cpu_millicores("2.5"), 2500);
/// ```
pub fn parse_k8s_cpu_millicores(value: &str) -> u64 {
    let value = value.trim();

    if value.ends_with("m") {
        value.trim_end_matches("m").parse::<u64>().unwrap_or(0)
    } else if value.ends_with("n") {
        // Nanocores to millicores
        value.trim_end_matches("n").parse::<u64>().unwrap_or(0) / 1_000_000
    } else if value.contains('.') {
        // Fractional cores (e.g., "2.5" = 2500m)
        (value.parse::<f64>().unwrap_or(0.0) * 1000.0) as u64
    } else {
        // Whole cores (e.g., "2" = 2000m)
        value.parse::<u64>().unwrap_or(0) * 1000
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_k8s_memory_bytes() {
        assert_eq!(parse_k8s_memory_bytes("1Gi"), 1024 * 1024 * 1024);
        assert_eq!(parse_k8s_memory_bytes("256Mi"), 256 * 1024 * 1024);
        assert_eq!(parse_k8s_memory_bytes("1024Ki"), 1024 * 1024);
        assert_eq!(parse_k8s_memory_bytes("1G"), 1_000_000_000);
        assert_eq!(parse_k8s_memory_bytes("100M"), 100_000_000);
        assert_eq!(parse_k8s_memory_bytes("1000K"), 1_000_000);
        assert_eq!(parse_k8s_memory_bytes("1048576"), 1048576);
    }

    #[test]
    fn test_parse_k8s_memory_mb() {
        assert_eq!(parse_k8s_memory_mb("1Gi"), 1024);
        assert_eq!(parse_k8s_memory_mb("512Mi"), 512);
        assert_eq!(parse_k8s_memory_mb("256Mi"), 256);
        assert_eq!(parse_k8s_memory_mb("2Gi"), 2048);
        assert_eq!(parse_k8s_memory_mb("100G"), 100_000); // SI: 100 * 1000
        assert_eq!(parse_k8s_memory_mb("2048Ki"), 2);
    }

    #[test]
    fn test_parse_k8s_cpu_millicores() {
        assert_eq!(parse_k8s_cpu_millicores("1"), 1000);
        assert_eq!(parse_k8s_cpu_millicores("500m"), 500);
        assert_eq!(parse_k8s_cpu_millicores("2"), 2000);
        assert_eq!(parse_k8s_cpu_millicores("100m"), 100);
        assert_eq!(parse_k8s_cpu_millicores("2.5"), 2500);
        assert_eq!(parse_k8s_cpu_millicores("1000000000n"), 1000);
    }

    #[test]
    fn test_parse_k8s_memory_with_whitespace() {
        assert_eq!(parse_k8s_memory_mb("  512Mi  "), 512);
        assert_eq!(parse_k8s_cpu_millicores("  500m  "), 500);
    }

    #[test]
    fn test_parse_k8s_invalid_values() {
        assert_eq!(parse_k8s_memory_bytes("invalid"), 0);
        assert_eq!(parse_k8s_memory_mb("invalid"), 0);
        assert_eq!(parse_k8s_cpu_millicores("invalid"), 0);
    }
}
