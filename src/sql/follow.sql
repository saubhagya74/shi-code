create table followed_following(
    follower_id bigint not null, --user id; followed by
    following_id bigint not null, --followed to?
    followed_at timestamptz default CURRENT_TIMESTAMP,
    is_active    boolean DEFAULT true,
    primary key (follower_id, following_id)
)
create index idx_get_followers on followed_following (following_id);
