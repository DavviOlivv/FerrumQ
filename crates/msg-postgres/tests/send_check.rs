use msg_postgres::migrations::run_migrations;
use sqlx::postgres::PgPoolOptions;

fn assert_send<T: Send>(_: T) {}

#[tokio::test(flavor = "current_thread")]
async fn migrations_future_is_send() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://localhost/ferrumq")
        .unwrap();
    assert_send(run_migrations(&pool));
}
