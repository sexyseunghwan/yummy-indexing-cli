use crate::common::*;

static SQL_DB_POOL: OnceCell<DatabaseConnection> = OnceCell::const_new();

#[doc = "SQL 커넥션 POOL을 초기화 해주는 함수"]
pub async fn establish_connection() -> &'static DatabaseConnection {
    SQL_DB_POOL
        .get_or_init(|| async {
            dotenv().ok();
            let db_url: String =
                env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env");

            Database::connect(db_url)
                .await
                .expect("Database connection failed")
        })
        .await
}

#[doc = "SQL 커넥션 POOL을 가져와주는 함수"]
pub fn get_db_pool() -> &'static DatabaseConnection {
    SQL_DB_POOL
        .get()
        .expect("Database connection has not been initialized. Call establish_connection() first.")
}
