//! Layer-3 tests for the install's news going out through the outbox,
//! against a REAL Postgres: news a write left is told in one call per
//! batch, taken out only once told, told again from memory a few times
//! while it cannot be, then marked refused and written down, never
//! travelling with or holding up the news after it, and never holds up
//! the database's own announcements.

use crate::support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use weft_task_store::db::{Client as _, Pool};
use weft_platform_traits::{Listening, News, NewsHeard, NewsKind, RunNews};

/// The crate's schema, and where a refusal of the news is written down
/// (with the run columns every write of a failure reads).
async fn setup(pool: &Pool) {
    support::setup(pool).await;
    weft_task_store::apply_groups(pool, &[&weft_task_store::unanswered::GROUP]).await.expect("unanswered schema");
    weft_task_store::unanswered::judged_run_columns(pool).await.expect("the run columns");
}

/// Refuses the first `fails` calls, and every batch carrying the run
/// `refuses` while it is set; records each batch announced.
struct Flaky {
    fails: AtomicUsize,
    refuses: Mutex<Option<uuid::Uuid>>,
    told: Mutex<Vec<Vec<RunNews>>>,
    heard: tokio::sync::broadcast::Sender<NewsHeard>,
}

impl Flaky {
    fn failing(fails: usize) -> Arc<Self> {
        Arc::new(Self {
            fails: AtomicUsize::new(fails),
            refuses: Mutex::default(),
            told: Mutex::default(),
            heard: tokio::sync::broadcast::channel(1).0,
        })
    }

    fn told(&self) -> Vec<Vec<RunNews>> {
        self.told.lock().unwrap().clone()
    }
}

#[async_trait]
impl News for Flaky {
    async fn announce(&self, news: &[RunNews]) -> anyhow::Result<()> {
        if self.fails.load(Ordering::SeqCst) > 0 {
            self.fails.fetch_sub(1, Ordering::SeqCst);
            anyhow::bail!("the bus is away");
        }
        let refused = *self.refuses.lock().unwrap();
        if news.iter().any(|piece| Some(piece.run) == refused) {
            anyhow::bail!("the bus will not take this");
        }
        self.told.lock().unwrap().push(news.to_vec());
        Ok(())
    }

    fn listen(&self) -> Listening {
        self.heard.subscribe().into()
    }

    async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn tell(pool: &Pool, run: u128) -> RunNews {
    let news = RunNews { project: uuid::Uuid::from_u128(1), run: uuid::Uuid::from_u128(run), kind: NewsKind::Cancel };
    pool.execute("SELECT weft_tell($1, $2, 'cancel')", &[&news.project, &news.run]).await.unwrap();
    news
}

async fn waiting(pool: &Pool) -> i64 {
    pool.scalar("SELECT COUNT(*) FROM weft_news", &[]).await.unwrap()
}

async fn until(what: &str, holds: impl AsyncFn() -> bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !holds().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

/// News waiting goes out in one call per batch, once, and is then taken
/// out of the outbox.
#[weft_db_macros::test]
async fn waiting_news_goes_out_in_one_call_and_once(pool: Pool) {
    setup(&pool).await;
    let batch = vec![tell(&pool, 2).await, tell(&pool, 3).await, tell(&pool, 4).await];
    let bus = Flaky::failing(0);
    weft_task_store::announce::tell_news_through(&pool, bus.clone());
    until("taken out once told", async || waiting(&pool).await == 0).await;
    weft_task_store::announce::committed(&pool);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(bus.told(), vec![batch], "one call, once");
}

/// A batch refused is told again as it was, from memory: news written
/// meanwhile is not read into it, and goes out after it.
#[weft_db_macros::test]
async fn a_refused_batch_is_told_again_without_reading_the_database(pool: Pool) {
    setup(&pool).await;
    let first = tell(&pool, 2).await;
    let bus = Flaky::failing(2);
    weft_task_store::announce::tell_news_through(&pool, bus.clone());
    until("the first batch was tried", async || bus.fails.load(Ordering::SeqCst) < 2).await;
    let second = tell(&pool, 3).await;
    weft_task_store::announce::news_written(&pool);
    until("both told", async || bus.told().len() == 2).await;
    assert_eq!(bus.told(), vec![vec![first], vec![second]], "the refused batch went out as it was read");
    until("taken out once told", async || waiting(&pool).await == 0).await;
}

/// A commit that left no news costs no look at the news: only a write that
/// left some says so, and that wakes the teller.
#[weft_db_macros::test]
async fn only_a_write_that_left_news_wakes_the_teller(pool: Pool) {
    setup(&pool).await;
    let bus = Flaky::failing(0);
    weft_task_store::announce::tell_news_through(&pool, bus.clone());
    // The teller's first look, for news a stopped process left, finds none.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let news = tell(&pool, 2).await;
    weft_task_store::announce::committed(&pool);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(bus.told().is_empty() && waiting(&pool).await == 1, "a commit alone looked at no news");
    weft_task_store::announce::news_written(&pool);
    until("told once its write said so", async || bus.told() == vec![vec![news]]).await;
}

/// A bus that refuses everything holds up none of the database's own
/// announcements: they go out at once, while the news waits.
#[weft_db_macros::test]
async fn a_bus_that_refuses_holds_up_no_announcement(pool: Pool) {
    setup(&pool).await;
    tell(&pool, 2).await;
    let bus = Flaky::failing(usize::MAX);
    weft_task_store::announce::tell_news_through(&pool, bus.clone());
    until("the bus refused", async || bus.fails.load(Ordering::SeqCst) < usize::MAX).await;
    let watch = support::signals(&pool).await;
    let mut heard = watch.subscribe();
    pool.execute("SELECT weft_announce($1, 'x')", &[&weft_task_store::tasks::TASK_READY_CHANNEL]).await.unwrap();
    weft_task_store::announce::committed(&pool);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    assert!(
        heard.woken_before(deadline, |channel, payload| channel == weft_task_store::tasks::TASK_READY_CHANNEL && payload == "x").await.unwrap(),
        "the announcement went out while the news waits"
    );
    assert_eq!(waiting(&pool).await, 1, "the news still waits");
}

async fn refusal_written(pool: &Pool) -> bool {
    pool.scalar("SELECT EXISTS (SELECT 1 FROM unanswered_call WHERE callee = 'news:')", &[]).await.unwrap()
}

async fn refused(pool: &Pool) -> i64 {
    pool.scalar("SELECT COUNT(*) FROM weft_news WHERE refused_at IS NOT NULL", &[]).await.unwrap()
}

/// A batch the bus keeps refusing is tried a few times, then marked
/// refused with the refusal written down, and the news after it goes out.
/// News written later goes out alone and before any refused news is tried
/// again; the refused news is then tried one piece per call, so the one
/// piece the bus will never take refuses only itself, and once it goes
/// out the refusal written down is cleared.
#[weft_db_macros::test]
async fn refused_news_never_travels_with_new_news_nor_holds_it_up(pool: Pool) {
    setup(&pool).await;
    let mut first = Vec::new();
    for run in 2..=101 {
        first.push(tell(&pool, run).await);
    }
    let after = tell(&pool, 102).await;
    let bus = Flaky::failing(0);
    let poison = first[0];
    *bus.refuses.lock().unwrap() = Some(poison.run);
    weft_task_store::announce::tell_news_through(&pool, bus.clone());
    until("the news after the refused batch told", async || bus.told() == vec![vec![after]]).await;
    until("the refusal written down", async || refusal_written(&pool).await).await;
    // The bus hears a batch before the teller takes it out, so the news
    // after the refused batch can still be in the table here.
    until("the news after the refused batch taken out", async || waiting(&pool).await == 100).await;
    assert_eq!(refused(&pool).await, 100, "the refused batch waits, marked refused");

    let later = tell(&pool, 103).await;
    weft_task_store::announce::news_written(&pool);
    until("everything but the piece the bus will never take told", async || {
        weft_task_store::announce::news_written(&pool);
        waiting(&pool).await == 1
    })
    .await;
    let told = bus.told();
    assert_eq!(told[..2], [vec![after], vec![later]], "new news went out alone, before the refused news was tried");
    assert!(told[2..].iter().all(|call| call.len() == 1), "refused news is tried one piece per call");
    let mut retold: Vec<RunNews> = told[2..].iter().map(|call| call[0]).collect();
    retold.sort_by_key(|piece| piece.run);
    assert_eq!(retold, first[1..], "every refused piece but the one the bus will never take went out");
    assert!(refusal_written(&pool).await, "a refused piece is still waiting");

    *bus.refuses.lock().unwrap() = None;
    weft_task_store::announce::news_written(&pool);
    until("the last refused piece told once news was next written", async || waiting(&pool).await == 0).await;
    assert_eq!(bus.told().last(), Some(&vec![poison]));
    until("the refusal cleared once no refused news is left", async || !refusal_written(&pool).await).await;
}

/// A look on an install whose bus never refused anything reads the new
/// news and nothing more: the refused path, and its install-wide lock, is
/// taken only once a refusal exists.
#[weft_db_macros::test]
async fn a_look_with_nothing_refused_never_takes_the_refused_path(pool: Pool) {
    setup(&pool).await;
    // Count the calls to the refused path instead of making them.
    pool.batch_execute(
        "CREATE TABLE refused_looks (n INT); \
         CREATE OR REPLACE FUNCTION weft_news_refused(p_limit BIGINT, p_skip BIGINT[], p_callee TEXT) \
             RETURNS TABLE (seq BIGINT, project_id UUID, run_id UUID, kind TEXT) AS $$ \
             BEGIN INSERT INTO refused_looks VALUES (1); RETURN; END; $$ LANGUAGE plpgsql",
    )
    .await
    .unwrap();
    let refused_looks = async || -> i64 { pool.scalar("SELECT COUNT(*) FROM refused_looks", &[]).await.unwrap() };
    let first = tell(&pool, 2).await;
    let bus = Flaky::failing(0);
    weft_task_store::announce::tell_news_through(&pool, bus.clone());
    until("the first news told", async || waiting(&pool).await == 0).await;
    // Looks run one after another, so once the second is told the first
    // look has ended.
    let second = tell(&pool, 3).await;
    weft_task_store::announce::news_written(&pool);
    until("the second news told", async || waiting(&pool).await == 0).await;
    assert_eq!(bus.told(), vec![vec![first], vec![second]]);
    assert_eq!(refused_looks().await, 0, "nothing refused: no look took the refused path");

    weft_task_store::unanswered::failed(&pool, weft_task_store::unanswered::Callee::News, None, "the bus is away").await.unwrap().log();
    weft_task_store::announce::news_written(&pool);
    until("a refusal written down sends the next look down the refused path", async || refused_looks().await > 0).await;
}

/// A refusal written down while no refused news is left (another process
/// told it meanwhile) is cleared by the next look, even one that finds no
/// news at all.
#[weft_db_macros::test]
async fn an_empty_look_clears_a_refusal_no_news_is_left_for(pool: Pool) {
    setup(&pool).await;
    weft_task_store::unanswered::failed(&pool, weft_task_store::unanswered::Callee::News, None, "the bus is away").await.unwrap().log();
    assert!(refusal_written(&pool).await);
    let bus = Flaky::failing(0);
    weft_task_store::announce::tell_news_through(&pool, bus.clone());
    until("the refusal cleared", async || !refusal_written(&pool).await).await;
    assert!(bus.told().is_empty());
}
