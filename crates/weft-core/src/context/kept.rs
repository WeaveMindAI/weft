//! The state a node keeps across a pause ([`Kept`], from `ctx.keep`).
//!
//! A node body resumes by running again from its top. It gets back, at
//! once, the last state it saved, and that state alone says where it
//! was: every `once` whose field is filled hands its value back without
//! running, every `wait` whose field is filled hands its answer back
//! without waiting, and the first empty one is where the body carries
//! on. Every call where a run can pause takes `&mut impl PausePoint` (a
//! bus read, a stream pull, a yield) or `&mut Kept<S>` (a wait, a program
//! call that waits in a loop). The pause points a node can hand over are
//! its `Kept` and a [`Holding`] (`ctx.hold()`), whose calls never pause;
//! [`HarnessRead`] is for test code reading from outside a node, and is
//! refused, when the call runs, for a read that belongs to a firing. A
//! node with nothing worth keeping keeps `()`.
//!
//! The state is written on [`Kept::save`], at the end of each
//! [`Kept::once`] and [`Kept::wait`], and when the run pauses while the
//! body sits in one of those calls. Never per bus message, stream item or
//! read: the runtime saves how far those went beside the state
//! (`crate::run::saved::Positions`), and [`Kept::read`] writes what it read
//! into the state, saved with the rest at the next of those.
//!
//! A resumed body runs again from its top, so it comes back to the call
//! it paused in only if nothing it reads on the way would read again: the
//! firing's [`Places`] hold every run of the body to that rule, live.

use std::panic::Location;
use std::sync::{Arc, Mutex};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::error::{WeftError, WeftResult};
use crate::primitive::SignalSpec;
use crate::run::saved::PausedHelper;

use super::ContextHandle;

/// Where a save stands relative to the call the body is in.
#[derive(Debug, Clone, PartialEq)]
pub enum SaveAt {
    /// Where the body stands now ([`Kept::save`], the end of a `once` or
    /// a `wait`).
    Here,
    /// The body is pausing inside a call that holds the state: the
    /// positions are the ones from before that call, which the resumed
    /// body makes again, and the firing is marked paused. `helper` is the
    /// waiting helper the pause is inside, and where it stood
    /// ([`Kept::follow`]): the resumed body comes back into its wait from
    /// there.
    Pausing { helper: Option<PausedHelper> },
}

/// A node's saved state, from `ctx.keep(initial)`. Reads and writes as
/// `S` (it derefs to it); `save`, `once` and `wait` write it to the run's
/// record. One per firing: a second `ctx.keep` fails the node.
pub struct Kept<S> {
    value: S,
    handle: Arc<dyn ContextHandle>,
}

impl<S> std::ops::Deref for Kept<S> {
    type Target = S;
    fn deref(&self) -> &S {
        &self.value
    }
}

impl<S> std::ops::DerefMut for Kept<S> {
    fn deref_mut(&mut self) -> &mut S {
        &mut self.value
    }
}

impl<S: Serialize + DeserializeOwned + Send> Kept<S> {
    /// The state the firing saved before, or `initial` on its first run.
    pub(super) async fn load(handle: Arc<dyn ContextHandle>, initial: S) -> WeftResult<Self> {
        let initial = to_json(&initial)?;
        let saved = handle.keep_state(initial).await?;
        let value = serde_json::from_value(saved).map_err(|e| {
            WeftError::NodeFailed(format!(
                "the state this node saved before the run paused no longer reads as its state ({e}): its code \
                 changed the state's shape since; start the run again"
            ))
        })?;
        Ok(Self { value, handle })
    }

    /// Write the state now. For right after something the node must not
    /// do twice, when it is not in a `once`: a crash or a pause after
    /// this save resumes the body with this state.
    pub async fn save(&mut self) -> WeftResult<()> {
        let state = to_json(&self.value)?;
        self.handle.save_state(state, SaveAt::Here).await
    }

    /// The value in `field` when it is filled, running nothing. When it
    /// is empty: run `work`, put its result in `field`, save, and hand it
    /// back. The one gap is a crash between `work` returning and the save
    /// landing, which runs `work` again; a service that must not see it
    /// twice has to refuse the duplicate itself. Hand it
    /// `ctx.firing_key()` ([`crate::context::RunContext::firing_key`]):
    /// the body run again sends the same key, so the service can answer
    /// the second call with what it answered the first.
    ///
    /// The result has to save as something (not JSON `null`: a `()`, a
    /// `None`, a unit struct), or a body run again could not tell the step
    /// done from a step not done yet: such a result fails the node. Return
    /// what says it is done instead, such as `true`.
    ///
    /// `work` gets the state, so it can read with it
    /// (`state.once(|s| &mut s.f, async |st| { reader.next(st).await ... })`).
    /// The direct reads inside a `once` that finished do not count for the
    /// rule of [`Places`]: a finished phase of reads goes in a `once`.
    /// Paused inside, the resumed body runs `work` again from its
    /// top, its reader at its saved position: what a loop in it adds up
    /// across turns has to live in the state.
    pub async fn once<T, F>(&mut self, field: impl Fn(&mut S) -> &mut Option<T>, work: F) -> WeftResult<T>
    where
        T: Serialize + Clone,
        F: AsyncFnOnce(&mut Self) -> WeftResult<T>,
    {
        if let Some(value) = field(&mut self.value) {
            return Ok(value.clone());
        }
        let started = self.handle.places().once_started();
        let value = work(self).await?;
        self.handle.places().once_finished(started);
        if to_json(&value)?.is_null() {
            return Err(WeftError::NodeFailed(
                "the result of a `once` saves as nothing (JSON null: a `()`, a `None`, a unit struct), so a body run \
                 again could not tell the step done from a step not done yet: return something that says it is done, \
                 such as `true`"
                    .into(),
            ));
        }
        *field(&mut self.value) = Some(value.clone());
        self.save().await?;
        Ok(value)
    }

    /// The answer in `field` when it is filled, waiting for nothing. When
    /// it is empty: wait on `on` (a signal kind, or one that cancels its
    /// job if the run is cancelled, `crate::signal::CancelWith`), put the
    /// answer in `field`, save, and hand it back.
    ///
    /// The wait stays open in this call. An answer that comes while the
    /// run is on its worker goes straight into it. Once every node the
    /// run is running waits and the run's quiet window (`pauseAfterSecs`)
    /// runs out, every waiting call pauses together: the worker lets go,
    /// and the answer has a worker pick the run up again, where the body
    /// runs from its top and comes back to this call with `field` still
    /// empty (the pause saved the state as it stood), which then hands the
    /// answer over at once. While the run cannot pause (a caller on the
    /// line, `recorded: false`, a node test) the wait holds in this call
    /// until it ends. A wait somebody skipped ends the firing skipped; one the
    /// run gave up on fails it, which `_error` may catch.
    ///
    /// An answer that is JSON `null` cannot be kept in an `Option` field
    /// (it would read back as no answer yet): it fails the node, naming why.
    #[track_caller]
    pub fn wait<'a, T, W>(
        &'a mut self,
        field: impl Fn(&mut S) -> &mut Option<T> + 'a,
        on: W,
    ) -> impl std::future::Future<Output = WeftResult<T>> + 'a
    where
        T: DeserializeOwned + Clone + 'a,
        W: crate::signal::WaitOn + 'a,
    {
        self.wait_at(field, on, Point::place("waited", Location::caller()))
    }

    async fn wait_at<T, W>(&mut self, field: impl Fn(&mut S) -> &mut Option<T>, on: W, point: Point) -> WeftResult<T>
    where
        T: DeserializeOwned + Clone,
        W: crate::signal::WaitOn,
    {
        // A place even when its answer is in: the rule counts the places a
        // run reaches, whatever they hand back.
        let point = point.on(field_key(&mut self.value, &field));
        self.handle.places().before(&point, self.pausable())?;
        if let Some(value) = field(&mut self.value) {
            return Ok(value.clone());
        }
        // A wait of the body's own, reached while it paused inside a
        // helper's wait, would take that wait's number and its answer.
        if let Some(paused) = self.handle.resumed_helper() {
            return Err(WeftError::NodeFailed(format!(
                "this node paused inside '{}', and when it ran again it came to a wait of its own first: its saved \
                 state must say where it was, so that its body comes back to the same call",
                paused.helper
            )));
        }
        let answer = match self.handle.wait(on.into_spec()).await {
            Ok(answer) => answer,
            Err(error) => return Err(self.paused_by(error, None).await),
        };
        if answer.is_null() {
            return Err(WeftError::NodeFailed(
                "the answer to this node's wait is JSON null, which its field cannot keep (it would read back as no \
                 answer yet when the body runs again)"
                    .into(),
            ));
        }
        let value: T = serde_json::from_value(answer).map_err(|e| {
            WeftError::NodeFailed(format!(
                "the answer to this node's wait does not read as the field it goes in ({e}): the field's type must \
                 match what the signal answers with"
            ))
        })?;
        *field(&mut self.value) = Some(value.clone());
        self.save().await?;
        Ok(value)
    }

    /// The item in `field` when it is filled, reading nothing. When it is
    /// empty: read the next item `from` a bus reader or a stream, put it in
    /// `field`, and hand it back (`None` at the end, leaving `field`
    /// empty). Nothing is saved here: the field goes on record with the
    /// rest of the state at the next pause, `save`, or end of a `once` or a
    /// `wait`. So a body that reads a message and then waits keeps the
    /// message over the pause, and comes back past this read.
    ///
    /// Not a direct read: a body may read through its state at as many
    /// lines as it likes ([`Places`]).
    #[track_caller]
    pub fn read<'a, R: StateRead + 'a>(
        &'a mut self,
        field: impl Fn(&mut S) -> &mut Option<R::Item> + 'a,
        from: &'a mut R,
    ) -> impl std::future::Future<Output = WeftResult<Option<R::Item>>> + 'a {
        let point = Point::place("read through its state", Location::caller());
        async move {
            // A place even when its item is in: the rule counts the places a
            // run reaches, whatever they hand back.
            let point = point.on(field_key(&mut self.value, &field));
            self.handle.places().before(&point, self.pausable())?;
            if let Some(item) = field(&mut self.value) {
                return Ok(Some(item.clone()));
            }
            let item = from.read_through(self, point).await?;
            if let Some(item) = &item {
                *field(&mut self.value) = Some(item.clone());
            }
            Ok(item)
        }
    }

    /// The loop of a helper that follows a program call in looks (a copy
    /// coming up, a build, a task): `step` makes the calls one look takes
    /// from `place` and says whether the helper is done, or where it
    /// stands for its next look; between looks the run pauses on
    /// `look_again`. `helper` names the helper and `call` is what it asks
    /// for (its target and arguments). Pausing in that wait saves the
    /// place with the state, and the resumed body, coming back into the
    /// same helper asking for the same thing, goes straight back to that
    /// wait from there: none of the calls the helper made before is made
    /// again (a read made again would answer what stands now, not what led
    /// to the pause, and a call that starts something would start it
    /// twice). Coming back to another helper, or to the same one asking
    /// for something else (a loop that did not keep where it was), is
    /// refused.
    pub(crate) async fn follow<P, T, F, Fut>(
        &mut self,
        helper: &'static str,
        call: &impl Serialize,
        look_again: SignalSpec,
        start: P,
        mut step: F,
    ) -> WeftResult<T>
    where
        P: Serialize + DeserializeOwned,
        F: FnMut(P) -> Fut,
        Fut: std::future::Future<Output = WeftResult<Step<P, T>>>,
    {
        let call = call_digest(helper, call)?;
        let mut place = match self.handle.resumed_helper() {
            Some(paused) if paused.helper == helper && paused.call == call => {
                let place: P = serde_json::from_value(paused.place).map_err(|e| {
                    WeftError::NodeFailed(format!(
                        "where '{helper}' stood when the run paused no longer reads as its place ({e}): start the run again"
                    ))
                })?;
                self.look_again(look_again.clone(), helper, &call, &place).await?;
                place
            }
            Some(paused) => {
                return Err(WeftError::NodeFailed(format!(
                    "this node paused inside '{}', and when it ran again it came to '{helper}' asking for something \
                     else first: its saved state must say where it was, so that its body comes back to the same call",
                    paused.helper
                )))
            }
            None => start,
        };
        loop {
            match step(place).await? {
                Step::Done(value) => return Ok(value),
                Step::LookAgain(next) => {
                    self.look_again(look_again.clone(), helper, &call, &next).await?;
                    place = next;
                }
            }
        }
    }

    /// One wait between a helper's looks ([`Self::follow`]): its answer is
    /// not kept; pausing in it saves where the helper stands.
    async fn look_again<P: Serialize>(&mut self, spec: SignalSpec, helper: &'static str, call: &str, place: &P) -> WeftResult<()> {
        // One place per helper and what it asks for: this line is the same
        // for every helper.
        let point = Point::place("waited between a helper's looks", Location::caller()).on(helper_key(helper, call));
        self.handle.places().before(&point, self.pausable())?;
        match self.handle.wait(spec).await {
            Ok(_) => Ok(()),
            Err(error @ WeftError::Suspended { .. }) => {
                let place = match serde_json::to_value(place) {
                    Ok(place) => place,
                    Err(e) => return Err(WeftError::NodeFailed(format!("where '{helper}' stands does not save: {e}"))),
                };
                Err(self.paused_by(error, Some(PausedHelper { helper: helper.to_string(), call: call.to_string(), place })).await)
            }
            Err(error) => Err(error),
        }
    }

    /// What a call holding the state returns for `error`: unchanged,
    /// except a suspension, which first saves the state as the pause
    /// leaves it (the firing then carries on from it). A save that fails
    /// is the error instead: the firing could not pause, and it fails.
    pub(crate) async fn paused_by(&mut self, error: WeftError, helper: Option<PausedHelper>) -> WeftError {
        if !matches!(error, WeftError::Suspended { .. }) {
            return error;
        }
        let state = match to_json(&self.value) {
            Ok(state) => state,
            Err(e) => return e,
        };
        match self.handle.save_state(state, SaveAt::Pausing { helper }).await {
            Ok(()) => error,
            Err(e) => e,
        }
    }
}

/// What one look of a helper found ([`Kept::follow`]): it is done with
/// its answer, or it looks again from where it now stands.
pub(crate) enum Step<P, T> {
    Done(T),
    LookAgain(P),
}

/// The refusal of a second `ctx.keep` in one firing: one state per
/// firing, so two would each say where the body is, differently.
pub fn kept_twice() -> WeftError {
    WeftError::NodeFailed(
        "this node called ctx.keep twice in one firing; a firing keeps one state, which says where its body is: \
         keep everything in one struct"
            .into(),
    )
}

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// One call where the run can pause, as a firing's [`Places`] checks it:
/// what it does (for the refusal), the line of the node's code that
/// called it, what it reads ([`Self::on`]), and what it is to the rule.
#[derive(Debug, Clone, Copy)]
pub struct Point {
    what: &'static str,
    site: &'static Location<'static>,
    /// What it reads: a bus, a stream, or the state's field, as a key
    /// unique among the firing's (0 until set).
    source: u128,
    kind: PointKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PointKind {
    /// A read that does not go through the state (a cursor read, a stream
    /// pull): a place the run can pause, and a direct read.
    Direct,
    /// A direct read that cannot pause (`Generator::try_next`).
    DirectNeverWaiting,
    /// A place the run can pause that reads nothing directly
    /// (`state.read`, `state.wait`, a yield).
    Place,
    /// Held to nothing: a membership wait, which asks whether a member ever
    /// joined, against the list the bus keeps over a pause.
    Unchecked,
}

impl Point {
    pub(crate) fn direct(what: &'static str, site: &'static Location<'static>) -> Self {
        Self { what, site, source: 0, kind: PointKind::Direct }
    }

    /// A place the run can pause that is not a direct read: a read made
    /// for [`Kept::read`], a wait, a yield.
    pub(crate) fn place(what: &'static str, site: &'static Location<'static>) -> Self {
        Self { what, site, source: 0, kind: PointKind::Place }
    }

    /// A direct read that never waits (`Generator::try_next`): it counts as
    /// a direct read for the places after it, but is no place itself, since
    /// the run cannot pause in it.
    pub(crate) fn direct_never_waiting(what: &'static str, site: &'static Location<'static>) -> Self {
        Self { what, site, source: 0, kind: PointKind::DirectNeverWaiting }
    }

    /// A membership wait (`BusHandle::wait_for`).
    pub(crate) fn unchecked(what: &'static str, site: &'static Location<'static>) -> Self {
        Self { what, site, source: 0, kind: PointKind::Unchecked }
    }

    /// The call reads `source` (a bus's id, a stream's port, a field of
    /// the state), unless what it reads was already set: a read made for
    /// [`Kept::read`] stays keyed by the state's field.
    pub(crate) fn on(self, source: u128) -> Self {
        if self.source != 0 {
            return self;
        }
        Self { source, ..self }
    }

    /// A stream's key, from its port.
    pub(crate) fn stream_key(port: &str) -> u128 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(&("stream", port), &mut hasher);
        u128::from(std::hash::Hasher::finish(&hasher)) | 1
    }
}

/// The one rule a run of a node's body is held to, checked live, on every
/// run (first or resumed), with nothing saved for it: the first time the
/// run reaches a place where it can pause (a bus read, a stream pull,
/// `state.read`, `state.wait`, a yield; a place is its line and what it
/// reads: a bus, a stream, a field of the state), no direct read may have happened
/// before it in this run, except inside a `state.once` that finished. A
/// resumed body runs again from its top, so a direct read before a new
/// place would read again, from where its reader stopped, what it read
/// before the pause, and hand the wrong message on: refused, naming both
/// lines. A body that takes another path on its resume (it branched on
/// the time, a random value, an outside call) fails the same way where the
/// paths split. Reads made by a firing holding its run (`ctx.hold()`)
/// never pause, and are held to nothing.
pub struct Places {
    node: String,
    inner: Mutex<PlacesNow>,
}

#[derive(Default)]
struct PlacesNow {
    /// The line of the last direct read this run made, since the last
    /// `once` that finished.
    direct: Option<&'static Location<'static>>,
    /// The places this run has reached, by line and what each reads: a
    /// later turn of a loop reaches the same ones again, and a loop reading
    /// several buses at one line reaches one place per bus.
    reached: std::collections::HashSet<(&'static str, u32, u32, u128)>,
    /// How many `ctx.hold()` values of the firing exist: while any does,
    /// no wait of the firing pauses ([`Places::holding`]).
    holds: usize,
    /// The line of the last direct read made while the firing held its
    /// run: once the last hold is dropped, it counts as a direct read for
    /// every place after ([`Places::hold_ended`]).
    held_read: Option<&'static Location<'static>>,
}

fn line(at: &Location<'_>) -> String {
    format!("{}:{}:{}", at.file(), at.line(), at.column())
}

impl Places {
    /// The places of node `node`'s firing, for one run of its body.
    pub fn new(node: impl Into<String>) -> Self {
        Self { node: node.into(), inner: Mutex::new(PlacesNow::default()) }
    }

    fn now(&self) -> std::sync::MutexGuard<'_, PlacesNow> {
        self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Before the call `point`: refused when it breaks the rule.
    /// `pausable`: the call may pause the run (made with a `Kept`); one
    /// that cannot is held to nothing.
    pub fn before(&self, point: &Point, pausable: bool) -> WeftResult<()> {
        if point.kind == PointKind::Unchecked {
            return Ok(());
        }
        let mut now = self.now();
        // A call that cannot pause (the firing holds its run) is no place,
        // and a direct read made so counts once the hold is dropped.
        if !pausable {
            if matches!(point.kind, PointKind::Direct | PointKind::DirectNeverWaiting) {
                now.held_read = Some(point.site);
            }
            return Ok(());
        }
        if point.kind == PointKind::DirectNeverWaiting {
            now.direct = Some(point.site);
            return Ok(());
        }
        let place = (point.site.file(), point.site.line(), point.site.column(), point.source);
        if now.reached.insert(place) {
            if let Some(earlier) = now.direct {
                return Err(WeftError::NodeFailed(format!(
                    "node '{}' read directly (not through its state) at {} before it {} at {}, a place its run can \
                     pause: run again after a pause, it would read at {} again from where its reader stopped and \
                     lose what it read there. Read that earlier one through the state (`state.read`), or put that \
                     earlier phase inside a `state.once`",
                    self.node,
                    line(earlier),
                    point.what,
                    line(point.site),
                    line(earlier),
                )));
            }
        }
        if point.kind == PointKind::Direct {
            now.direct = Some(point.site);
        }
        Ok(())
    }

    /// A `ctx.hold()` value of the firing was made: no wait of the firing
    /// pauses until every one is dropped.
    pub fn hold_started(&self) {
        self.now().holds += 1;
    }

    /// A `ctx.hold()` value of the firing was dropped: once none is left,
    /// the firing can pause again, and the last direct read it made while
    /// holding counts as a direct read for every place after.
    pub fn hold_ended(&self) {
        let mut now = self.now();
        now.holds = now.holds.saturating_sub(1);
        if now.holds == 0 {
            if let Some(read) = now.held_read.take() {
                now.direct = Some(read);
            }
        }
    }

    /// Whether the firing holds its run: a `ctx.hold()` value of it exists.
    pub fn holding(&self) -> bool {
        self.now().holds > 0
    }

    /// A `once`'s work starts: what it answers is handed back to
    /// [`Self::once_finished`].
    pub fn once_started(&self) -> OnceStarted {
        OnceStarted(self.now().direct)
    }

    /// A `once` that `started` finished: the direct reads inside it are
    /// done with, and one made before it counts again, as it did before it.
    pub fn once_finished(&self, started: OnceStarted) {
        self.now().direct = started.0;
    }
}

/// The direct read a `once`'s work started after, if any
/// ([`Places::once_started`]).
pub struct OnceStarted(Option<&'static Location<'static>>);

/// A source [`Kept::read`] reads through the state: a bus reader or a
/// stream.
pub trait StateRead: sealed::Sealed + Send {
    /// What one read hands back, kept in the state's field.
    type Item: Serialize + DeserializeOwned + Clone + Send;
    /// The next item, read as the call `point`, with `at`.
    fn read_through<'a, P: PausePoint>(
        &'a mut self,
        at: &'a mut P,
        point: Point,
    ) -> impl std::future::Future<Output = WeftResult<Option<Self::Item>>> + Send + 'a;
}

/// What a call where the run can pause takes: the firing's state
/// ([`Kept`]). A bus read, a membership wait and a stream pull take
/// `&mut impl PausePoint`; sealed, so the only one a node can hand over
/// is its `Kept`, and a node that could pause without saying where it is
/// does not compile.
pub trait PausePoint: sealed::Sealed + Send {
    /// Before the call `point`: the firing's [`Places`] check it.
    /// `in_firing`: the call reads for a firing (a cursor, a feed or a
    /// yield of a node's body), never true for a read a test makes from
    /// outside one.
    fn before(&self, point: &Point, in_firing: bool) -> WeftResult<()>;
    /// Whether the run may pause while the call waits: a firing that
    /// holds the run ([`Holding`]) keeps it from pausing instead.
    fn pausable(&self) -> bool {
        true
    }
    /// The run paused while the call waited: save the state as it
    /// stands, mark the firing paused, and answer the suspension the body
    /// hands back with `?` (or the error a failed save is).
    fn paused(&mut self) -> impl std::future::Future<Output = WeftError> + Send;
}

impl<S> sealed::Sealed for Kept<S> {}

impl<S: Serialize + DeserializeOwned + Send> PausePoint for Kept<S> {
    fn before(&self, point: &Point, _in_firing: bool) -> WeftResult<()> {
        self.handle.places().before(point, self.pausable())
    }

    /// Not while the firing holds its run (`ctx.hold()`): its reads,
    /// pulls and yields then hold too.
    fn pausable(&self) -> bool {
        !self.handle.places().holding()
    }

    fn paused(&mut self) -> impl std::future::Future<Output = WeftError> + Send {
        self.paused_by(WeftError::Suspended { token: None }, None)
    }
}

/// What a firing that must not be paused in the middle of its work reads
/// and yields with (`ctx.hold()`): a session with a provider that cannot
/// be picked up again, audio being gathered into one file. While it
/// exists the firing holds its run: no wait of the firing pauses (a call
/// made with it, and a `state.wait`, a helper's wait or a read with the
/// state too), and the run holds its worker as a run that cannot pause
/// holds a wait (open while a caller is on the line, otherwise up to its
/// `holdSecs` with nothing moving, then the wait gives up loudly). Once it
/// is dropped the firing can pause again, and the last direct read made
/// while holding counts as a direct read for every place after
/// ([`Places::hold_ended`]). A firing that holds saves nothing for it: it
/// is never picked up from the middle.
pub struct Holding {
    handle: Arc<dyn ContextHandle>,
}

impl Holding {
    pub(crate) fn new(handle: Arc<dyn ContextHandle>) -> Self {
        handle.places().hold_started();
        Self { handle }
    }
}

impl Drop for Holding {
    fn drop(&mut self) {
        self.handle.places().hold_ended();
    }
}

impl sealed::Sealed for Holding {}

impl PausePoint for Holding {
    fn before(&self, point: &Point, _in_firing: bool) -> WeftResult<()> {
        self.handle.places().before(point, false)
    }

    fn pausable(&self) -> bool {
        false
    }

    async fn paused(&mut self) -> WeftError {
        // A holding wait is never told the run pauses.
        WeftError::NodeFailed("a call of a firing that holds the run was asked to pause".into())
    }
}

/// What test code (never a node) reads a bus or a stream with: a read
/// from outside every firing, which has no state to keep and nothing to
/// resume. Handed to a read that belongs to a firing (a node's body
/// reading with it), it is refused before the read starts: the firing
/// would read without saying where it is.
pub struct HarnessRead;

impl sealed::Sealed for HarnessRead {}

impl PausePoint for HarnessRead {
    fn before(&self, point: &Point, in_firing: bool) -> WeftResult<()> {
        if in_firing {
            return Err(WeftError::NodeFailed(format!(
                "this node {} with `HarnessRead`, which is for reads from outside a node; a node reads with its own state (`ctx.keep`)",
                point.what
            )));
        }
        Ok(())
    }

    async fn paused(&mut self) -> WeftError {
        // Only a read for a firing can be paused, and `before` refused it.
        WeftError::NodeFailed("a read from outside every firing was asked to pause".into())
    }
}

/// What a unit test of this crate reads for a firing with (a cursor of a
/// firing's handle): a read with no state behind it, which nothing
/// pauses.
#[cfg(test)]
pub(crate) struct FiringRead;

#[cfg(test)]
impl sealed::Sealed for FiringRead {}

#[cfg(test)]
impl PausePoint for FiringRead {
    fn before(&self, _point: &Point, _in_firing: bool) -> WeftResult<()> {
        Ok(())
    }

    async fn paused(&mut self) -> WeftError {
        WeftError::NodeFailed("a unit test's read was asked to pause".into())
    }
}

/// What `helper`'s `call` asks for ([`PausedHelper::digest`]).
fn call_digest(helper: &str, call: &impl Serialize) -> WeftResult<String> {
    PausedHelper::digest(call).map_err(|e| WeftError::NodeFailed(format!("what '{helper}' asks for does not save: {e}")))
}

/// The key of a helper's wait between looks for [`Point::on`]: the helper
/// and what it asks for.
fn helper_key(helper: &str, call: &str) -> u128 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&("helper", helper, call), &mut hasher);
    u128::from(std::hash::Hasher::finish(&hasher)) | 1
}

/// The key of the state's `field` for [`Point::on`]: where it sits in the
/// state, which is the same on every run of the body as long as the field
/// sits in the state itself (not behind a `Box` or in a `Vec`'s items,
/// whose address moves: a field there can read as a new place).
fn field_key<S, T>(state: &mut S, field: &impl Fn(&mut S) -> &mut Option<T>) -> u128 {
    let base = state as *mut S as usize;
    let at = field(state) as *mut Option<T> as usize;
    // Never 0 (no source yet), and apart from a bus id or a stream key.
    (u128::from(at.wrapping_sub(base) as u64) << 1) | (1 << 127)
}

fn to_json<S: Serialize>(state: &S) -> WeftResult<Value> {
    serde_json::to_value(state).map_err(|e| WeftError::NodeFailed(format!("this node's state does not serialize: {e}")))
}

// Layer 1: the rule a firing's places hold its pause points to.
#[cfg(test)]
mod tests {
    use super::*;

    /// A line of this test's code, as a pause point would name it.
    #[track_caller]
    fn line_here() -> &'static Location<'static> {
        Location::caller()
    }

    /// A direct read before a place the run can pause first reaches is
    /// refused, naming both lines; the same places again (a later turn of
    /// a loop) are not; a finished `once` leaves no direct read behind; a
    /// place reached only through the state, a membership wait and a read
    /// that cannot pause are held to nothing.
    #[test]
    fn a_direct_read_before_a_new_place_is_refused() {
        let (greeting, turn, wait, joined) = (line_here(), line_here(), line_here(), line_here());

        let looping = Places::new("n");
        for _ in 0..3 {
            looping.before(&Point::direct("read a bus", turn), true).expect("a loop reading directly");
        }

        let greeted = Places::new("n");
        greeted.before(&Point::place("read through its state", greeting), true).unwrap();
        greeted.before(&Point::unchecked("waited for a member of a bus", joined), true).unwrap();
        greeted.before(&Point::direct("read a bus", turn), true).expect("a greeting through the state, then a loop");

        let phased = Places::new("n");
        let started = phased.once_started();
        phased.before(&Point::direct("read a bus", greeting), true).unwrap();
        phased.once_finished(started);
        phased.before(&Point::place("waited", wait), true).expect("reads inside a finished once do not count");

        let read_first = Places::new("n");
        read_first.before(&Point::direct("read a bus", greeting), true).unwrap();
        let started = read_first.once_started();
        read_first.once_finished(started);
        read_first.before(&Point::place("waited", wait), true).expect_err("a read before a once still counts once it finished");

        let pulled = Places::new("n");
        pulled.before(&Point::direct_never_waiting("pulled a stream", joined), true).unwrap();
        pulled.before(&Point::direct_never_waiting("pulled a stream", joined), true).expect("a pull that never waits is no place");
        pulled.before(&Point::place("waited", wait), true).expect_err("but a direct read for the places after it");

        let held = Places::new("n");
        held.hold_started();
        assert!(held.holding());
        held.before(&Point::direct("pulled a stream", greeting), false).unwrap();
        held.before(&Point::place("waited", wait), false).expect("inside the hold, a read that cannot pause is held to nothing");
        held.hold_ended();
        assert!(!held.holding());
        held.before(&Point::place("waited", turn), true).expect_err("once the hold is dropped, its last read counts as a direct read");

        let one_bus = Places::new("n");
        for _ in 0..3 {
            one_bus.before(&Point::direct("read a bus", turn).on(7), true).expect("one bus read at one line, every turn");
        }
        let two_buses = Places::new("n");
        two_buses.before(&Point::direct("read a bus", turn).on(7), true).unwrap();
        let error = two_buses.before(&Point::direct("read a bus", turn).on(8), true).unwrap_err().to_string();
        assert!(error.contains("before it read a bus at"), "a second bus at the same line is a new place: {error}");

        let refused = Places::new("n");
        refused.before(&Point::direct("read a bus", greeting), true).unwrap();
        let error = refused.before(&Point::place("waited", wait), true).unwrap_err().to_string();
        assert!(error.contains(&format!("read directly (not through its state) at {} before it waited at {}", line(greeting), line(wait))), "{error}");
    }
}
