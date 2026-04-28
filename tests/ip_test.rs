#[cfg(test)]
mod tests {
    use local_ip_address::local_ip;

    #[test]
    fn get_local_ip_test() {
        let my_local_ip = local_ip().unwrap();
        println!("This is my local IP address: {my_local_ip}");
    }

    #[test]
    fn get_app_version_test() {
        println!("This is software version: {}", env!("CARGO_PKG_VERSION"));
    }
}
