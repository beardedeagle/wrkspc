//! wrkspc-core — the transactional store, fence flows, moderation state,
//! and the committed event feed for the wrkspc pilot.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod error;
pub mod model;
pub mod moderation;
pub mod roster;
pub mod store;
pub mod tasks;
pub mod team;
pub mod views;

pub use error::{Result, StoreError};
pub use model::{
    AttemptState, DeliveryState, FenceKind, IntentState, PILOT_HOST_ID, PRESENCE_STATES, PeerClass,
    Reconciliation, is_presence_state,
};
pub use moderation::{CancelOutcome, DueIntent, Guidance, OpenAttempt};
pub use roster::{ModerationEvent, PeerStatus, PresenceOutcome, RosterEntry, status_is_ejected};
pub use store::{AcceptOutcome, Event, Fence, RecipientRequest, Store};
pub use tasks::{DeliveryClaimOutcome, DeliveryTaskRequest, TaskRow};
pub use team::{HubUnknownView, OutboundMessage, RemoteFanoutRequest};
pub use views::{ConversationView, DeliveryView, MessageView, UnknownAttemptView};
