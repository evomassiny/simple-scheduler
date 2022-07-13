use crate::models::{UserId, User};
use chrono::{DateTime, Duration, Utc};
use rocket::http::Cookie;
use rocket::http::Status;
use rocket::request::{self, FromRequest, Outcome, Request};
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

pub const AUTH_COOKIE_NAME: &'static str = "Authorization";
pub const AUTH_COOKIE_VALID_PERIOD_IN_DAYS: i64 = 1;

#[derive(Debug, Deserialize, Serialize)]
pub struct AuthToken {
    /// expiration date
    pub creation: DateTime<Utc>,
    pub user_id: i64,
}

impl AuthToken {
    pub async fn fetch_user(&self, conn: &mut SqliteConnection) -> Option<User<UserId>> {
        User::get_from_id(self.user_id, conn).await
    }

    fn is_still_valid(&self) -> bool {
        let now = Utc::now();
        (self.creation + Duration::days(AUTH_COOKIE_VALID_PERIOD_IN_DAYS)) >= now
    }

    pub fn new(user: User<UserId>) -> Option<Self> {
        Some(Self {
            user_id: user.id,
            creation: Utc::now(),
        })
    }

    pub fn as_cookie(&self) -> Cookie {
        Cookie::new(
            AUTH_COOKIE_NAME,
            serde_json::to_string(self).unwrap_or("error".to_owned()),
        )
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for AuthToken {
    type Error = ();

    /// Extract Auth token from the "AUTH_COOKIE_NAME" cookie.
    async fn from_request(req: &'r Request<'_>) -> request::Outcome<AuthToken, Self::Error> {
        if let Some(cookie) = req.cookies().get_private(AUTH_COOKIE_NAME) {
            let auth = serde_json::from_str::<Self>(cookie.value());
            if let Ok(auth_token) = auth {
                if auth_token.is_still_valid() {
                    return Outcome::Success(auth_token);
                }
            }
        }
        Outcome::Failure((Status::Forbidden, ()))
    }
}
