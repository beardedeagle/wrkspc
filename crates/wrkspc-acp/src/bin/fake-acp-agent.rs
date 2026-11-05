//! Fake ACP agent used by wrkspc-acp integration tests.
//!
//! Speaks ACP over stdio (the phase 0 spike's agent mode, extended):
//! - `session/new` → fresh id; with `--record FILE` it writes the
//!   request's `mcpServers` as JSON (the injection runtime proof)
//! - `session/prompt` → echo chunk + `EndTurn`; trailing content
//!   `"slow"` delays 1.5 s; `"never"` holds the turn open until
//!   `session/cancel`; a prompt containing the `--die-on` substring
//!   kills the process without responding (a harness crash before any
//!   ACP write). Dispatcher prompts carry a wrkspc addressing prefix,
//!   so the control triggers match the TRAILING content.
//! - `session/cancel` → responds to the held prompt with `Cancelled`
//!   (the HLD's confirmed cancellation carrier)

use agent_client_protocol::schema::v1::{
    AgentCapabilities, ClientNotification, ContentBlock, InitializeRequest, InitializeResponse,
    NewSessionRequest, NewSessionResponse, PromptRequest, PromptResponse, SessionNotification,
    SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::{Agent, Client, ConnectionTo, Responder, Stdio};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct FakeState {
    sessions: HashMap<String, ()>,
    next_id: u64,
    held: HashMap<String, Responder<PromptResponse>>,
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
    // --log-prompts FILE: append `<millis>\t<text>` per prompt receipt
    // (serialization evidence: the gaps prove turns never overlap).
    let log_prompts = args
        .windows(2)
        .find(|pair| pair[0] == "--log-prompts")
        .map(|pair| pair[1].clone());
    let state = Arc::new(Mutex::new(FakeState::default()));

    Agent
        .builder()
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
                let die_on = die_on.clone();
                let touch = touch.clone();
                let log_prompts = log_prompts.clone();
                async move |req: PromptRequest,
                            responder: Responder<PromptResponse>,
                            connection: ConnectionTo<Client>| {
                    let sid = req.session_id.to_string();
                    if !state.lock().unwrap().sessions.contains_key(&sid) {
                        return Ok(());
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
                    if let Some(path) = &log_prompts {
                        // One line per prompt; the dispatcher's addressing
                        // prefix embeds newlines, so flatten them.
                        let line = format!(
                            "{}\t{}\n",
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis(),
                            text.replace('\n', " ")
                        );
                        use std::io::Write as _;
                        let mut file = std::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(path)
                            .unwrap();
                        let _ = file.write_all(line.as_bytes());
                    }

                    // Crash-before-write: die without responding so the
                    // client observes a lost turn, not a refusal.
                    if let Some(substring) = &die_on
                        && text.contains(substring)
                    {
                        std::process::exit(1);
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
                                    agent_client_protocol::schema::v1::ContentChunk::new(
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
                            responder.respond(PromptResponse::new(StopReason::EndTurn))?;
                            connection.send_notification(SessionNotification::new(
                                sid,
                                SessionUpdate::AgentMessageChunk(
                                    agent_client_protocol::schema::v1::ContentChunk::new(
                                        ContentBlock::Text(TextContent::new(
                                            "echo: slow".to_string(),
                                        )),
                                    ),
                                ),
                            ))?;
                        }
                        _ => {
                            responder.respond(PromptResponse::new(StopReason::EndTurn))?;
                            connection.send_notification(SessionNotification::new(
                                sid,
                                SessionUpdate::AgentMessageChunk(
                                    agent_client_protocol::schema::v1::ContentChunk::new(
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
