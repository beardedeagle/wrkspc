//! Throwaway probe (NOT part of the test suite — deleted after use):
//! the SDK ListTasks call alone, long timeout, full result.
/// The SDK client lists hub tasks over the devlocal mTLS material (live probe; skips when the material is absent).
#[tokio::test]
async fn probe_sdk_list_tasks() {
    let base = std::path::PathBuf::from(std::env::var("HOME").unwrap())
        .join("Library/Application Support/devlocal/wrkspc-mtls");
    let client = wrkspc_cli::poll::build_hub_client(
        &base.join("server-ca.pem"),
        &base.join("client-chain.crt"),
        &base.join("node.pk8.key"),
    )
    .expect("client builds");
    let hub_url = "https://wrkspc-hub.internal.devlocal.home.arpa:8443".to_string();
    let transport =
        a2a_client::jsonrpc::JsonRpcTransport::new(client, format!("{hub_url}/jsonrpc"));
    let a2a = a2a_client::A2AClient::new(transport);
    let request = a2a::ListTasksRequest {
        context_id: Some("randys-macbook-pro.mtls.devlocal.home.arpa".to_string()),
        status: None,
        page_size: None,
        page_token: None,
        status_timestamp_after: None,
        history_length: None,
        include_artifacts: None,
        tenant: None,
    };
    let started = std::time::Instant::now();
    match tokio::time::timeout(std::time::Duration::from_secs(30), a2a.list_tasks(&request)).await {
        Ok(Ok(response)) => println!(
            "PROBE list_tasks OK in {:?}: {} tasks",
            started.elapsed(),
            response.tasks.len()
        ),
        Ok(Err(error)) => println!("PROBE list_tasks error in {:?}: {error}", started.elapsed()),
        Err(_) => println!("PROBE list_tasks TIMED OUT at 30s"),
    }
}
