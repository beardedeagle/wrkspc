#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
// Test fixture: a fake ACP agent for the integration suites; the
// test-idiom lints do not apply to a tool that only ever runs
// under test harnesses.
//! Fake ACP agent used by wrkspc-acp integration tests.
//!
//! Speaks ACP over stdio (the phase 0 spike's agent mode, extended):
//! - `session/new` → fresh id; with `--record FILE` it writes the
//!   request's `mcpServers` as JSON (the injection runtime proof)
//! - `session/set_config_option` → acknowledged; with
//!   `--record-config FILE` the request is written as JSON (the typed
//!   payload round-trip proof)
//! - `session/prompt` → echo chunk + `EndTurn`; trailing content
//!   `"slow"` delays 1.5 s; `"never"` holds the turn open until
//!   `session/cancel`; a prompt containing the `--die-on` substring
//!   kills the process without responding (a harness crash before any
//!   ACP write); a prompt for an unknown session id is refused with
//!   the SDK's session-not-found JSON-RPC error. Dispatcher prompts
//!   carry a wrkspc addressing prefix, so the control triggers match
//!   the TRAILING content.
//! - with `--request-permission FILE`, every prompt also sends a
//!   `session/request_permission` request to the client (from a
//!   subtask, never awaited inside the dispatch loop) and records the
//!   client's outcome as JSON in FILE
//! - with `--log-turn FILE`, each turn appends `start\t<text>\t<millis>` on
//!   receipt and `end\t<text>` on completion (serialization evidence:
//!   a serialized loop never interleaves turns)
//! - with `--exit-after-ms MS`, the process exits after MS idle
//!   milliseconds (a real ACP adapter dying idle)
//! - `session/cancel` → responds to the held prompt with `Cancelled`
//!   (the HLD's confirmed cancellation carrier)

use agent_client_protocol::v1::schema::{
    AgentCapabilities, ClientNotification, ContentBlock, InitializeRequest, InitializeResponse,
    LoadSessionRequest, LoadSessionResponse, NewSessionRequest, NewSessionResponse,
    PermissionOption, PermissionOptionKind, PromptRequest, PromptResponse,
    RequestPermissionRequest, SessionNotification, SessionUpdate, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse, StopReason, TextContent, ToolCallUpdate, ToolCallUpdateFields,
};
use agent_client_protocol::v1::{ConnectionTo, Responder};
use agent_client_protocol::{Agent, Client, Stdio};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct FakeState {
    sessions: HashMap<String, ()>,
    next_id: u64,
    held: HashMap<String, Responder<PromptResponse>>,
}

/// Appends one `event\t<text>\t<millis>` line to the turn log
/// (newlines in the prompt text are flattened so each turn is exactly
/// one line). The trailing millisecond timestamp lets serialization
/// tests prove turns did not overlap without a second clock source.
fn log_turn_event(path: &str, event: &str, text: &str) {
    use std::io::Write as _;
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or(0);
    let line = format!("{event}\t{}\t{millis}\n", text.replace('\n', " "));
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    let _ = file.write_all(line.as_bytes());
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    // --record FILE: write the first session/new request's mcpServers here.
    let record = args
        .windows(2)
        .find(|pair| pair[0] == "--record")
        .map(|pair| pair[1].clone());
    // --die-on SUBSTRING: exit(1) without responding when a prompt
    // contains SUBSTRING — a harness crash mid-turn, before any ACP
    // write (Appendix D check 4's pre-write point).
    let die_on = args
        .windows(2)
        .find(|pair| pair[0] == "--die-on")
        .map(|pair| pair[1].clone());
    // --touch FILE: create FILE when a prompt arrives (a deterministic
    // side channel for tests proving the harness received the prompt).
    let touch = args
        .windows(2)
        .find(|pair| pair[0] == "--touch")
        .map(|pair| pair[1].clone());
    // --record-config FILE: write every session/set_config_option
    // request here as JSON (the typed payload round-trip proof).
    let record_config = args
        .windows(2)
        .find(|pair| pair[0] == "--record-config")
        .map(|pair| pair[1].clone());
    // --log-turn FILE: append `start\t<text>` on prompt receipt and
    // `end\t<text>` on turn completion (serialization evidence: the
    // order of the lines proves turns never overlap).
    let log_turn = args
        .windows(2)
        .find(|pair| pair[0] == "--log-turn")
        .map(|pair| pair[1].clone());
    // --request-permission FILE: during every prompt, send a
    // `session/request_permission` request to the client and record the
    // client's outcome (its JSON form) in FILE — the integration test's
    // proof that the client's permission policy ran.
    let permission_probe = args
        .windows(2)
        .find(|pair| pair[0] == "--request-permission")
        .map(|pair| pair[1].clone());
    // --exit-after-ms MS: exit the process after MS idle milliseconds —
    // a real ACP adapter dying while its session is cached and idle.
    let exit_after_ms = args
        .windows(2)
        .find(|pair| pair[0] == "--exit-after-ms")
        .and_then(|pair| pair[1].parse::<u64>().ok());
    let state = Arc::new(Mutex::new(FakeState::default()));

    if let Some(ms) = exit_after_ms {
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            std::process::exit(0);
        });
    }

    Agent
        .v1()
        .name("wrkspc-fake-acp-agent")
        .on_receive_request(
            async move |init: InitializeRequest, responder: Responder<InitializeResponse>, _cx| {
                let mut caps = AgentCapabilities::new();
                caps.load_session = true;
                responder.respond(
                    InitializeResponse::new(init.protocol_version).agent_capabilities(caps),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = state.clone();
                let record = record.clone();
                async move |req: NewSessionRequest,
                            responder: Responder<NewSessionResponse>,
                            _cx: ConnectionTo<Client>| {
                    if let Some(path) = record.clone() {
                        let servers = req.mcp_servers;
                        if let Ok(json) = serde_json::to_string(&servers) {
                            let _ = std::fs::write(&path, json);
                        }
                    }
                    let mut state = state.lock().unwrap();
                    state.next_id += 1;
                    let id = format!("fake-{}", state.next_id);
                    state.sessions.insert(id.clone(), ());
                    responder.respond(NewSessionResponse::new(id))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = state.clone();
                async move |req: LoadSessionRequest,
                            responder: Responder<LoadSessionResponse>,
                            _cx: ConnectionTo<Client>| {
                    // The advertised load_session capability is REAL:
                    // a loaded session re-enters the known set (the
                    // fake holds no per-session process state, so the
                    // id is the whole state).
                    state
                        .lock()
                        .unwrap()
                        .sessions
                        .insert(req.session_id.to_string(), ());
                    responder.respond(LoadSessionResponse::new())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let record_config = record_config.clone();
                async move |req: SetSessionConfigOptionRequest,
                            responder: Responder<SetSessionConfigOptionResponse>,
                            _cx: ConnectionTo<Client>| {
                    // The ack is the whole contract: the fake holds no
                    // per-session config state. With --record-config the
                    // received request is written as JSON so the test can
                    // assert the exact payload the wire carried.
                    if let Some(path) = record_config.clone()
                        && let Ok(json) = serde_json::to_string(&req)
                    {
                        let _ = std::fs::write(path, json);
                    }
                    responder.respond(SetSessionConfigOptionResponse::new(Vec::new()))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = state.clone();
                let die_on = die_on.clone();
                let touch = touch.clone();
                let log_turn = log_turn.clone();
                let permission_probe = permission_probe.clone();
                async move |req: PromptRequest,
                            responder: Responder<PromptResponse>,
                            connection: ConnectionTo<Client>| {
                    let sid = req.session_id.to_string();
                    if !state.lock().unwrap().sessions.contains_key(&sid) {
                        // An unknown session is a protocol refusal, not
                        // silence: dropping the responder would leave the
                        // client awaiting a response until the connection
                        // closes. Answer with the SDK's session-not-found
                        // JSON-RPC error.
                        return responder.respond_with_error(
                            agent_client_protocol::v1::schema::Error::session_not_found(sid).into(),
                        );
                    }
                    if let Some(path) = &touch {
                        let _ = std::fs::write(path, b"prompted");
                    }
                    let text: String = req
                        .prompt
                        .iter()
                        .filter_map(|b| match b {
                            ContentBlock::Text(t) => Some(t.text.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    if let Some(path) = &log_turn {
                        log_turn_event(path, "start", &text);
                    }

                    // Crash-before-write: die without responding so the
                    // client observes a lost turn, not a refusal.
                    if let Some(substring) = &die_on
                        && text.contains(substring)
                    {
                        std::process::exit(1);
                    }

                    // Permission-probe mode: ask the client for permission
                    // on every prompt and record whatever the client
                    // answers. The response is awaited in a SUBTASK — the
                    // SDK's dispatch loop runs this handler, and awaiting a
                    // response here would deadlock the connection.
                    if let Some(path) = &permission_probe {
                        let connection = connection.clone();
                        let permission_sid = sid.clone();
                        let path = path.clone();
                        tokio::spawn(async move {
                            let request = RequestPermissionRequest::new(
                                permission_sid,
                                ToolCallUpdate::new("perm-1", ToolCallUpdateFields::new()),
                                vec![PermissionOption::new(
                                    "allow-once",
                                    "Allow once",
                                    PermissionOptionKind::AllowOnce,
                                )],
                            );
                            let outcome = connection
                                .send_request(request)
                                .block_task()
                                .await
                                .map(|response| response.outcome);
                            // JSON, not Debug text: the test parses the
                            // outcome back into a typed value.
                            let recorded = match outcome {
                                Ok(outcome) => serde_json::to_string(&outcome)
                                    .unwrap_or_else(|error| format!("serialize failed: {error}")),
                                Err(error) => format!("request failed: {error}"),
                            };
                            let _ = std::fs::write(path, recorded);
                        });
                    }

                    match text.trim_end() {
                        // Dispatcher prompts carry the wrkspc addressing
                        // prefix before the content, so the control
                        // triggers match the trailing content only.
                        content if content.ends_with("never") => {
                            // Emit the receipt chunk BEFORE holding, so the
                            // test side can deterministically observe that
                            // the prompt reached the agent (the cancel test
                            // waits on this event instead of sleeping).
                            connection.send_notification(SessionNotification::new(
                                sid.clone(),
                                SessionUpdate::AgentMessageChunk(
                                    agent_client_protocol::v1::schema::ContentChunk::new(
                                        ContentBlock::Text(TextContent::new(
                                            "echo: never".to_string(),
                                        )),
                                    ),
                                ),
                            ))?;
                            state.lock().unwrap().held.insert(sid.clone(), responder);
                        }
                        content if content.ends_with("slow") => {
                            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
                            if let Some(path) = &log_turn {
                                log_turn_event(path, "end", &text);
                            }
                            responder.respond(PromptResponse::new(StopReason::EndTurn))?;
                            connection.send_notification(SessionNotification::new(
                                sid,
                                SessionUpdate::AgentMessageChunk(
                                    agent_client_protocol::v1::schema::ContentChunk::new(
                                        ContentBlock::Text(TextContent::new(
                                            "echo: slow".to_string(),
                                        )),
                                    ),
                                ),
                            ))?;
                        }
                        _ => {
                            if let Some(path) = &log_turn {
                                log_turn_event(path, "end", &text);
                            }
                            responder.respond(PromptResponse::new(StopReason::EndTurn))?;
                            connection.send_notification(SessionNotification::new(
                                sid,
                                SessionUpdate::AgentMessageChunk(
                                    agent_client_protocol::v1::schema::ContentChunk::new(
                                        ContentBlock::Text(TextContent::new(format!(
                                            "echo: {text}"
                                        ))),
                                    ),
                                ),
                            ))?;
                        }
                    }
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_notification(
            {
                let state = state.clone();
                async move |notification: ClientNotification, _cx| {
                    // Agents receive the ClientNotification enum; only the
                    // cancel variant carries our held-turn response.
                    let ClientNotification::CancelNotification(cancel) = notification else {
                        return Ok(());
                    };
                    let held = state
                        .lock()
                        .unwrap()
                        .held
                        .remove(&cancel.session_id.to_string());
                    if let Some(responder) = held {
                        responder.respond(PromptResponse::new(StopReason::Cancelled))?;
                    }
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .connect_to(Stdio::new())
        .await?;

    Ok(())
}
