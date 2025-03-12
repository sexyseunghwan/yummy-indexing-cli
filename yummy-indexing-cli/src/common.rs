pub use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    fmt::Debug,
    fs::File,
    future::Future,
    io::{self, BufReader, Write},
    ops::Deref, //time::Duration,
    str::FromStr,
    sync::Arc,
};

pub use rand::{prelude::SliceRandom, rngs::StdRng, SeedableRng};

pub use tokio::{
    io::AsyncReadExt,
    signal,
    sync::{Mutex, MutexGuard, OnceCell},
    time::{Duration, Interval},
};

pub use log::{error, info, warn};

pub use flexi_logger::{Age, Cleanup, Criterion, FileSpec, Logger, Naming, Record};

pub use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone, Utc};

pub use serde::{Deserialize, Serialize};

pub use serde::de::DeserializeOwned;

pub use serde_json::{json, Value};

pub use http::header::{HeaderMap, HeaderValue, CONTENT_TYPE};

pub use dotenv::dotenv;

pub use cron::Schedule;

pub use elasticsearch::{
    http::response::Response,
    http::transport::{ConnectionPool, Transport},
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    http::Url,
    indices::{
        IndicesCreateParts, IndicesDeleteParts, IndicesGetAliasParts, IndicesGetParts,
        IndicesRefreshParts,
    },
    BulkOperation, BulkParts, DeleteByQueryParts, DeleteParts, Elasticsearch, IndexParts,
    SearchParts,
};

pub use anyhow::{anyhow, Result};

pub use derive_new::new;
pub use getset::{Getters, Setters};

// pub use rdkafka:: {
//     config::ClientConfig,
//     consumer::Consumer,
//     producer::{FutureProducer, FutureRecord},
//     message::Message as KafkaMessage
// };

// pub use kafka::{
//     producer::{Producer, Record as KafkaRecord, RequiredAcks}
// };

pub use sea_orm::{
    prelude::{Decimal, Expr},
    ActiveModelBehavior, ColumnTrait, Condition, Database, DatabaseConnection, EntityTrait,
    FromQueryResult, JoinType, QueryFilter, QueryOrder, QuerySelect, RelationTrait, Select,
};

// pub use diesel::{
//     dsl::count_star,
//     mysql::MysqlConnection,
//     r2d2::{ConnectionManager, Pool, PooledConnection},
//     AsChangeset, ExpressionMethods, Insertable, NullableExpressionMethods, QueryDsl, Queryable,
//     QueryableByName, RunQueryDsl, SelectableHelper
// };

pub use async_trait::async_trait;

//use crate::repository::es_repository::*;
//use crate::repository::kafka_repository::*;
//pub static ELASTICSEARCH_CLIENT: OnceCell<Arc<EsRepositoryPub>> = OnceCell::new();
//pub static KAFKA_PRODUCER: OnceCell<Arc<KafkaRepositoryPub>> = OnceCell::const_new();

pub use once_cell::sync::Lazy as once_lazy;
